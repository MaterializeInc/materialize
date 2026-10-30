---
title: "S3-compatible storage and Iceberg REST catalogs"
description: "How to export results from Materialize to Apache Iceberg tables in an Iceberg REST catalog, such as Apache Polaris, backed by S3-compatible object storage, such as MinIO."
menu:
  main:
    parent: sink-iceberg
    name: "S3-compatible storage and REST catalogs"
    weight: 40
aliases:
  - /serve-results/sink/iceberg-rest/
---

{{< public-preview />}}

This guide walks you through setting up an Iceberg sink to a catalog that
implements the [Iceberg REST catalog
specification](https://iceberg.apache.org/rest-catalog-spec/), with tables
stored in Amazon S3 or S3-compatible object storage. The examples use [Apache
Polaris](https://polaris.apache.org/) as the catalog and MinIO as the object
store.

Materialize does not take a storage endpoint or storage credentials of its own.
The catalog supplies both: with [credential
vending](/sql/create-connection/#iceberg-catalog-access-delegation), it hands
Materialize temporary credentials scoped to the table it writes.

{{< tip >}}
Some catalogs have their own guide: [AWS S3 Tables](/export-data/iceberg-aws/),
[GCP BigLake](/export-data/iceberg-gcp/), and [Databricks Unity
Catalog](/export-data/iceberg-databricks/).
{{< /tip >}}

## Prerequisites

- A Polaris deployment, and an S3-compatible bucket for its warehouse. The
  examples use a bucket named `warehouse`.

- Polaris configured with storage credentials that can call the object store's
  STS `AssumeRole` API, for example through the `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` environment variables.

- Polaris's API and the object store reachable from Materialize. Iceberg
  catalog connections cannot tunnel through [AWS
  PrivateLink](/sql/create-connection/#aws-privatelink) or an [SSH
  bastion host](/sql/create-connection/#ssh-tunnel). If either endpoint sits
  behind a firewall, allow traffic from the [static egress IP
  addresses](/ingest-data/network-security/static-ips/) of your Materialize
  region.

- An access token for a Polaris principal that can manage catalogs and
  principals, such as the `root` principal created at bootstrap:

  ```bash
  POLARIS=https://<polaris_host>
  read -rs ROOT_CLIENT_SECRET

  TOKEN=$(curl -sS "$POLARIS/api/catalog/v1/oauth/tokens" \
    -d grant_type=client_credentials \
    -d client_id=<root_client_id> \
    --data-urlencode "client_secret=$ROOT_CLIENT_SECRET" \
    -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
  ```

## Set up Polaris

### Step 1. Create a catalog backed by your bucket

Create a catalog whose `storageConfigInfo` points at your object store. Polaris
vends `endpoint` to Materialize, so it must be reachable from Materialize. If
Polaris reaches the object store at a different address, add that address as
`endpointInternal`. S3-compatible stores such as MinIO generally need
`pathStyleAccess`.

```bash
curl -sS --fail-with-body "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "materialize_catalog",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://warehouse/materialize_catalog/"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://warehouse/materialize_catalog/"],
        "endpoint": "https://<minio_host>:9000",
        "pathStyleAccess": true,
        "region": "us-east-1"
      }
    }
  }'
```

Leave `s3.access-key-id` and `s3.secret-access-key` out of the catalog's
`properties`. Polaris returns catalog properties to every client, so long-lived
credentials placed there are shared with anyone who can load a table.

### Step 2. Create a principal for Materialize

Create a principal, and save the `clientId` and `clientSecret` that Polaris
returns. Polaris does not show the secret again.

```bash
curl -sS --fail-with-body "$POLARIS/api/management/v1/principals" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"principal": {"name": "materialize"}}' | jq .credentials
```

### Step 3. Grant the principal access to the catalog

Polaris grants privileges to catalog roles, which are assigned to principal
roles, which are assigned to principals. `CATALOG_MANAGE_CONTENT` lets the
principal create and commit to tables, and includes the `TABLE_READ_DATA` and
`TABLE_WRITE_DATA` privileges that authorize credential vending.

```bash
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
API="$POLARIS/api/management/v1"

# Create a principal role and assign it to the principal.
curl -sS --fail-with-body "${H[@]}" "$API/principal-roles" \
  -d '{"principalRole": {"name": "materialize_role"}}'
curl -sS --fail-with-body "${H[@]}" -X PUT "$API/principals/materialize/principal-roles" \
  -d '{"principalRole": {"name": "materialize_role"}}'

# Create a catalog role, grant it privileges, and assign it to the principal role.
curl -sS --fail-with-body "${H[@]}" "$API/catalogs/materialize_catalog/catalog-roles" \
  -d '{"catalogRole": {"name": "materialize_writer"}}'
curl -sS --fail-with-body "${H[@]}" -X PUT \
  "$API/catalogs/materialize_catalog/catalog-roles/materialize_writer/grants" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
curl -sS --fail-with-body "${H[@]}" -X PUT \
  "$API/principal-roles/materialize_role/catalog-roles/materialize_catalog" \
  -d '{"catalogRole": {"name": "materialize_writer"}}'
```

### Step 4. Create a namespace

Materialize creates the table a sink writes to, but not its namespace.

```bash
curl -sS --fail-with-body "$POLARIS/api/catalog/v1/materialize_catalog/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"namespace": ["materialize"]}'
```

## Create the Iceberg catalog connection in Materialize

### Step 1. Store the principal's credentials

Store the `clientId` and `clientSecret` from [Step 2](#step-2-create-a-principal-for-materialize)
as a single value, separated by a colon:

```mzsql
CREATE SECRET polaris_credential AS '<client_id>:<client_secret>';
```

### Step 2. Create the connection

```mzsql
CREATE CONNECTION iceberg_catalog_connection TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://<polaris_host>/api/catalog',
    WAREHOUSE = 'materialize_catalog',
    CREDENTIAL = SECRET polaris_credential,
    SCOPE = 'PRINCIPAL_ROLE:ALL',
    ACCESS DELEGATION = 'vended-credentials'
);
```

| Option | Value for Polaris |
| --- | --- |
| `URL` | Polaris's API base URL followed by `/api/catalog`. |
| `WAREHOUSE` | The catalog name. |
| `SCOPE` | `PRINCIPAL_ROLE:ALL`, or `PRINCIPAL_ROLE:<name>` to activate a single principal role. Required: Polaris rejects the token request without it. |
| `ACCESS DELEGATION` | `'vended-credentials'`, so that Materialize writes with the credentials Polaris vends. |
| `STORAGE PROVIDER` | Omit it for S3 and S3-compatible storage such as MinIO, the default. Set it to `'gcs'` or `'adls'` if the catalog stores its tables in Google Cloud Storage or Azure Data Lake Storage Gen2. |

`CREATE CONNECTION` validates the connection by listing the catalog's
namespaces, so a wrong URL, scope, or credential fails here. For all options,
see [`CREATE CONNECTION`](/sql/create-connection/#iceberg-catalog).

## Other REST catalogs

The same connection works with other Iceberg REST catalogs, with these
differences:

- Without `ACCESS DELEGATION`, Materialize uses the storage properties the
  catalog returns: `s3.endpoint`, `s3.region`, `s3.path-style-access`,
  `s3.access-key-id`, and `s3.secret-access-key`. Materialize adds nothing to
  them, so they must describe the store completely.
- Materialize exchanges `CREDENTIAL` at `<URL>/v1/oauth/tokens` and ignores the
  token endpoint the catalog advertises. Set `OAUTH2 SERVER URL` if the catalog
  serves it elsewhere.
- Materialize refreshes vended credentials through the catalog's
  `loadCredentials` endpoint. Sinks on a catalog that vends credentials but does
  not implement `loadCredentials` fail once the first credential expires.
- For storage other than S3, set `STORAGE PROVIDER`.

## Create the Iceberg sink in Materialize

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-intro" %}}

### Upsert mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-upsert-mode" %}}

### Append mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-append-mode" %}}

### Verify the sink

Once the first commit interval elapses, check that the sink is running:

```mzsql
SELECT name, status, error
FROM mz_internal.mz_sink_statuses
WHERE name = '<sink_name>';
```

```nofmt
    name     | status  | error
-------------+---------+-------
 <sink_name> | running |
```

The table's data and metadata files appear in your bucket under
`s3://warehouse/materialize_catalog/<namespace>/<table>/`.

## Considerations

### Commit interval tradeoffs {#commit-interval-tradeoffs}

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Credential refresh

Materialize refreshes both its OAuth2 token and the vended storage credentials
while the sink runs. Rotating the principal's secret in Polaris requires
updating the Materialize secret:

```mzsql
ALTER SECRET polaris_credential AS '<client_id>:<new_client_secret>';
```

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

- Iceberg catalog connections cannot be modified with [`ALTER
  CONNECTION`](/sql/alter-connection). To change any option, drop the connection
  and the sinks that depend on it, then recreate them.

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

| Error | Cause |
| --- | --- |
| `CREATE CONNECTION` fails with `invalid_scope` | `SCOPE` is missing. |
| `CREATE CONNECTION` fails with `401 Unauthorized` | The credential is wrong, or `SCOPE` names a principal role that is not assigned to the principal. |
| The sink stalls with `403 Forbidden` and `is not authorized for op CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION` | The principal's catalog role lacks either `CATALOG_MANAGE_CONTENT` or both `TABLE_READ_DATA` and `TABLE_WRITE_DATA`, so Polaris refuses to vend credentials. |
| The sink stalls with `500 Internal Server Error` from Polaris | Polaris cannot reach the object store, or its storage credentials cannot call `AssumeRole`. Check the Polaris logs. |
| The sink stalls with `Failed to close batch writer` and `signing http request` | Materialize has no storage credentials. Set `ACCESS DELEGATION = 'vended-credentials'` on the connection. |
| The sink stalls with `Failed to close batch writer` for another reason | The `endpoint` in the catalog's `storageConfigInfo` is not reachable from Materialize. |
| A namespace-not-found error when the sink starts | The namespace named by `NAMESPACE` does not exist, or the principal cannot see it. |

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection/#iceberg-catalog)
- [Storage access delegation](/sql/create-connection/#iceberg-catalog-access-delegation)
- [Apache Polaris documentation](https://polaris.apache.org/)
