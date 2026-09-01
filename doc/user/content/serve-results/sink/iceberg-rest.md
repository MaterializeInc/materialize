---
title: "Iceberg REST catalog with S3-compatible storage"
description: "How to export results from Materialize to Apache Iceberg tables in a catalog implementing the Iceberg REST specification, backed by S3-compatible object storage."
menu:
  main:
    parent: sink-iceberg
    name: "REST catalog"
    weight: 40
---

{{< public-preview />}}

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud against a catalog that implements the [Iceberg REST catalog
specification](https://iceberg.apache.org/spec/) with OAuth2 client credentials,
and whose tables are stored in Amazon S3 or an S3-compatible object store such as
MinIO. [Apache Polaris](https://polaris.apache.org/) is the catalog Materialize
tests this path against.

Some catalogs have their own guide, because they authenticate differently or need
specific options:

- [AWS S3 Tables](/serve-results/sink/iceberg-aws/), which authenticates through
  an AWS connection.
- [GCP BigLake](/serve-results/sink/iceberg-gcp/), which authenticates through a
  GCP connection.
- [Databricks Unity Catalog](/serve-results/sink/iceberg-databricks/), which
  authenticates with OAuth2 but manages its own storage.

## How Materialize reaches your storage

An Iceberg catalog connection to a REST catalog carries **no** storage
configuration of its own: no endpoint, no region, and no S3 access keys. Every
option in the connection is about reaching the *catalog*. Materialize learns how
to reach the *storage* from the catalog itself, one of two ways:

| Path | How it works | When to use it |
| --- | --- | --- |
| Catalog-supplied properties | The catalog returns storage properties (`s3.endpoint`, `s3.region`, `s3.access-key-id`, ...) when Materialize loads its configuration and tables. Materialize passes them to its S3 client unchanged. | The catalog is configured with credentials it is willing to hand to clients. |
| Credential vending | With `ACCESS DELEGATION = 'vended-credentials'`, Materialize asks the catalog for temporary, table-scoped credentials and uses only those. | The catalog withholds long-lived credentials, or you would rather not have them leave the catalog. |

The practical consequence: **the S3-compatible endpoint is configured on the
catalog, not in Materialize.** A catalog that returns no usable storage
configuration leaves the sink unable to write, even though the connection itself
validates.

An `AWS CONNECTION` cannot be used to supply the endpoint and keys instead:
`CATALOG TYPE = 'rest'` rejects it, since that option exists for [AWS S3
Tables](/serve-results/sink/iceberg-aws/), whose catalog and storage are both
authenticated with SigV4.

{{< note >}}
Materialize's Iceberg sinks are tested against Apache Polaris with MinIO storage.
Other catalog and storage combinations use the same code path, but are not
covered by Materialize's tests.
{{< /note >}}

## Prerequisites

### A REST catalog reachable from Materialize

The catalog's REST endpoint, its OAuth2 token endpoint, and the object storage
behind it must all be reachable from Materialize over the public internet.
Iceberg catalog connections do not support tunneling through [AWS
PrivateLink](/sql/create-connection/#aws-privatelink) or [SSH
bastion hosts](/sql/create-connection/#ssh-tunnel), so a catalog that is only
reachable inside a private network cannot be used.

If any of those endpoints sits behind a firewall, allow traffic from the [static
egress IP addresses](/ingest-data/network-security/static-ips/) associated with
your Materialize region.

### OAuth2 client credentials

Materialize authenticates to the catalog with the OAuth2 client credentials
grant: it exchanges a client ID and client secret for a bearer token, and
refreshes the token as it expires. Create a catalog principal for Materialize and
obtain its client ID and client secret.

For [Apache Polaris](https://polaris.apache.org/), this is a service principal,
whose `clientId` and `clientSecret` are returned when the principal is created.

### A warehouse configured for your S3-compatible storage

Materialize calls the catalog's `/v1/config` endpoint with the connection's
`WAREHOUSE` as a query parameter, and the catalog answers with the storage
properties for that warehouse. Those properties have to describe your object
store completely, because Materialize adds nothing to them:

| Property | Purpose |
| --- | --- |
| `s3.endpoint` | The object store's endpoint. Required for anything other than Amazon S3. |
| `s3.region` | The region to sign requests for. S3-compatible stores that have no regions still need a value here, since SigV4 signing requires one. |
| `s3.path-style-access` | Set to `true` for stores that address buckets by path rather than by virtual host, which most S3-compatible stores do. |
| `s3.access-key-id`, `s3.secret-access-key` | The credentials clients should use. Omit these only if you are using credential vending. |

The warehouse's base location must be an `s3://` location in that store.

In Polaris, these are the catalog's `properties`, alongside a
`storageConfigInfo` of `storageType: S3` naming the same endpoint and allowed
locations.

### A namespace in the catalog

Materialize creates the Iceberg *table* a sink writes to, but not the namespace
containing it, so the namespace must already exist.

### Privileges for the Materialize principal

The principal needs whatever privileges the catalog requires to list namespaces,
create a table in your namespace, read its metadata, and commit to it.

If you plan to use credential vending, the principal also needs whatever
privilege authorizes vending. In Polaris, these are the `TABLE_READ_DATA` and
`TABLE_WRITE_DATA` privileges. A catalog that gates delegation behind a privilege
the principal does not hold rejects the request outright rather than falling back,
so an unprivileged principal cannot use `ACCESS DELEGATION` at all.

## Create the Iceberg catalog connection in Materialize

### Step 1. Store the catalog credentials

Store the client ID and client secret as a single value, separated by a colon:

```mzsql
CREATE SECRET rest_catalog_oauth AS '<client_id>:<client_secret>';
```

A value with no colon is sent as the client secret alone, for catalogs that
authenticate with a secret only.

### Step 2. Create the Iceberg catalog connection

{{% include-syntax file="examples/create_connection" example="syntax-iceberg-catalog-rest" %}}

The values to use are catalog-specific:

| Option | What to set it to |
| --- | --- |
| `URL` | The path the catalog serves `/v1/` under. For Polaris, `https://<host>/api/catalog`. |
| `WAREHOUSE` | The warehouse to operate in. What this names is catalog-specific: for Polaris it is the catalog name, for other catalogs it may be a storage location. |
| `CREDENTIAL` | The secret from [Step 1](#step-1-store-the-catalog-credentials). |
| `OAUTH2 SERVER URL` | Only needed if the catalog does not serve its token endpoint at `<url>/v1/oauth/tokens`. Polaris does, so omit it there. |
| `SCOPE` | Only needed if the catalog does not accept the specification's `catalog` scope. Polaris expects `PRINCIPAL_ROLE:ALL`. |
| `ACCESS DELEGATION` | Set to `'vended-credentials'` to use catalog-vended storage credentials. Omit it to use the credentials the catalog returns in its properties. |

{{< note >}}
Materialize builds its own OAuth2 client rather than taking the endpoint the
catalog advertises in its `/v1/config` response. If your catalog serves its token
endpoint somewhere other than `<url>/v1/oauth/tokens`, set `OAUTH2 SERVER URL`
explicitly; Materialize will not discover it.

`OAUTH2 SERVER URL` must resolve to a public address. Materialize POSTs the
catalog credential to it, so a URL aimed at a private address is rejected when
the connection is created.
{{< /note >}}

`CREATE CONNECTION` validates the connection by listing the catalog's
namespaces, so a wrong URL, scope, or credential fails immediately rather than at
sink creation.

{{% include-example file="examples/create_connection"
example="example-iceberg-catalog-rest-connection" %}}

## Create the Iceberg sink in Materialize

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-intro" %}}

### Upsert mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-upsert-mode" %}}

### Append mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-append-mode" %}}

## Considerations

### Commit interval tradeoffs {#commit-interval-tradeoffs}

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Credential refresh

The OAuth2 token Materialize exchanges its credentials for is short-lived, and
Materialize refreshes it while the sink runs.

Vended storage credentials are refreshed from the catalog's `loadCredentials`
endpoint. Materialize re-fetches 15 minutes before the expiry the catalog
reports in the `s3.session-token-expires-at-ms` property, and every 5 minutes if
the catalog reports no expiry at all. A catalog that vends credentials on
`loadTable` but does not implement `loadCredentials` cannot refresh them, and its
sinks fail once the first credential expires.

Rotating the client secret at the catalog requires updating the Materialize
secret:

```mzsql
ALTER SECRET rest_catalog_oauth AS '<client_id>:<new_client_secret>';
```

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

- Materialize does not create namespaces. The namespace named by the sink's
  `NAMESPACE` option must already exist in the catalog.

- Iceberg catalog connections cannot be modified with [`ALTER
  CONNECTION`](/sql/alter-connection). To change any option, drop the connection
  and the sinks that depend on it, then recreate them.

- Iceberg catalog connections cannot tunnel through AWS PrivateLink or an SSH
  bastion host.

- Only S3 and S3-compatible storage is supported on this path. A REST catalog
  backed by Azure Blob Storage cannot be used.

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

If the sink reports an error, start with the sink's own status:

```mzsql
SELECT name, error FROM mz_internal.mz_sink_statuses WHERE name = '<sink_name>';
```

| Error | Cause |
| --- | --- |
| Token exchange failures | `OAUTH2 SERVER URL` or `SCOPE` does not match what the catalog expects, or the credentials have been rotated or revoked. |
| Authentication failures on every catalog request | The principal lacks the privileges the catalog requires. |
| A namespace-not-found error when the sink starts | The namespace named by `NAMESPACE` does not exist, or the credentials cannot see it. |
| Storage errors once the sink is running, or requests going to Amazon S3 rather than your endpoint | The catalog is not returning the storage properties Materialize needs. Check that its warehouse configuration carries `s3.endpoint`, `s3.region`, and `s3.path-style-access`, and either client-visible credentials or `ACCESS DELEGATION = 'vended-credentials'`. |
| The catalog rejects `loadTable` outright | `ACCESS DELEGATION` is set but the principal is not authorized to receive vended credentials. |

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Storage access delegation](/sql/create-connection/#iceberg-catalog-access-delegation)
- [Apache Iceberg REST catalog specification](https://iceberg.apache.org/spec/)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
