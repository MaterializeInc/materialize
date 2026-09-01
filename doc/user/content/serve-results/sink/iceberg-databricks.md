---
title: "Databricks Unity Catalog on AWS"
description: "How to export results from Materialize to Apache Iceberg tables registered in Databricks Unity Catalog on AWS."
menu:
  main:
    parent: sink-iceberg
    name: "Databricks Unity Catalog"
    weight: 30
---

{{< public-preview />}}

{{< warning >}}
Sinks into Unity Catalog must use **`MODE APPEND`**. Upsert mode is not
supported, because it writes Iceberg equality delete files that Unity Catalog
managed tables do not accept. See [Append-only
sinks](#append-only-sinks) below.
{{< /warning >}}

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud against [Databricks Unity
Catalog](https://docs.databricks.com/aws/en/external-access/iceberg) on AWS.
Materialize reaches Unity Catalog through its [Iceberg REST catalog
endpoint](https://docs.databricks.com/aws/en/external-access/iceberg),
authenticating with the OAuth2 credentials of a Databricks service principal.

This guide covers Databricks workspaces on AWS, whose URLs have the form
`https://<workspace>.cloud.databricks.com`. Databricks on Azure and Google Cloud
are not yet covered.

## Prerequisites

### A Unity Catalog metastore with external data access enabled

Your Databricks workspace must be attached to a Unity Catalog metastore, and
that metastore must have **External data access** enabled. Unity Catalog rejects
every Iceberg REST request until it is, including the ones Materialize makes to
discover the table.

The toggle is in **Catalog Explorer** on the metastore's **Details** tab, listed
as **External data access**. Editing metastore details requires an account
administrator, who can also set it from the account console. See [Databricks:
Enable external data access to Unity
Catalog](https://docs.databricks.com/aws/en/external-access/admin).

### A catalog and a schema to write into

Materialize creates the Iceberg *table* a sink writes to, but it does not create
the catalog or the schema containing it. Both must exist first.

In a Databricks SQL editor or notebook:

```sql
CREATE SCHEMA <catalog_name>.<schema_name>;
```

The two names map onto the Materialize objects you create below:

- The catalog name becomes the connection's `WAREHOUSE`. For Unity Catalog this
  is the name of a catalog, not a storage location.
- The schema name becomes the sink's `NAMESPACE`.

Materialize can only sink into Unity Catalog **managed** tables, so use a
standard catalog with managed storage. Foreign catalogs and Delta tables are
read-only through the Iceberg REST catalog.

### A service principal with credentials and privileges

Materialize authenticates as a [Databricks service
principal](https://docs.databricks.com/aws/en/admin/users-groups/service-principals).

1. Create the service principal at the account level, and assign it to the
   workspace whose URL you use for the connection. Materialize exchanges its
   credentials at that workspace's token endpoint, so a service principal
   without access to the workspace cannot authenticate.

2. Generate an **OAuth secret** for the service principal. This gives you a
   client ID (its application ID) and a client secret. Record the secret when it
   is shown; Databricks does not display it again.

3. Grant the service principal these privileges **on the metastore**. In
   **Catalog Explorer**, open the metastore, go to its **Permissions** tab, and
   grant, under **Data Administration**:

    | Privilege | Why Materialize needs it |
    | --- | --- |
    | `USE METASTORE REMOTELY` | Reach the metastore from outside a Databricks compute resource, which is what an Iceberg REST client is. |
    | `READ METADATA` | Read the table metadata the sink commits against. |
    | `CREATE STORAGE CREDENTIAL` | Receive the temporary storage credentials Unity Catalog vends for the table's storage. |

    These are metastore-level grants, separate from the catalog and schema grants
    below. Granting only the catalog and schema privileges leaves the connection
    failing on every request.

4. Grant the service principal the privileges Materialize needs **on the catalog
   and schema**. In a Databricks SQL editor, using the service principal's
   application ID as the grantee:

    ```sql
    GRANT USE CATALOG ON CATALOG <catalog_name> TO `<application_id>`;

    GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT, EXTERNAL USE SCHEMA
      ON SCHEMA <catalog_name>.<schema_name> TO `<application_id>`;
    ```

    | Privilege | Why Materialize needs it |
    | --- | --- |
    | `USE CATALOG`, `USE SCHEMA` | Reach the catalog and schema at all. |
    | `EXTERNAL USE SCHEMA` | Read and write the schema's tables from an Iceberg REST client. Without it, every catalog request is rejected. |
    | `CREATE TABLE` | Create the Iceberg table the first time the sink runs. |
    | `MODIFY` | Commit new snapshots as data changes. |
    | `SELECT` | Read the table's current metadata before each commit. |

    Databricks restricts who may grant `EXTERNAL USE SCHEMA`. If the grant is
    rejected, ask the catalog owner or a metastore admin to run it. See
    [Databricks: Access Databricks tables from Apache Iceberg
    clients](https://docs.databricks.com/aws/en/external-access/iceberg).

## Create the Iceberg catalog connection in Materialize

### Step 1. Store the service principal credentials

Store the client ID and client secret as a single value, separated by a colon:

```mzsql
CREATE SECRET databricks_oauth AS '<client_id>:<client_secret>';
```

### Step 2. Create the Iceberg catalog connection

{{% include-example file="examples/create_connection"
example="example-iceberg-catalog-databricks-connection" %}}

Fill in the options as follows:

| Option | Value for Unity Catalog |
| --- | --- |
| `URL` | `https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest` |
| `WAREHOUSE` | The name of the Unity Catalog catalog holding your tables. Unlike other catalogs, this is not a storage location. |
| `CREDENTIAL` | The secret created in [Step 1](#step-1-store-the-service-principal-credentials). |
| `OAUTH2 SERVER URL` | `https://<workspace>.cloud.databricks.com/oidc/v1/token` |
| `SCOPE` | `all-apis` |
| `ACCESS DELEGATION` | `'vended-credentials'` |

The last three options are optional in the [Iceberg REST catalog
specification](https://iceberg.apache.org/spec/), but Unity Catalog requires all
three. It serves its token endpoint on a path unrelated to the catalog URL, does
not grant the specification's `catalog` scope, and manages the storage behind its
tables without handing out long-lived credentials for it. See [Storage access
delegation](/sql/create-connection/#iceberg-catalog-access-delegation) for what
`ACCESS DELEGATION` changes.

For the full option reference, see [`CREATE CONNECTION`: Iceberg
catalog](/sql/create-connection/#iceberg-catalog).

## Create the Iceberg sink in Materialize

Set the sink's `NAMESPACE` option to the Unity Catalog schema you granted
privileges on.

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-intro" %}}

### Append-only sinks {#append-only-sinks}

Unity Catalog tables accept only `MODE APPEND`, so the sink appends a row per
change rather than updating rows in place: `_mz_diff` is `+1` for an insertion
and `-1` for a deletion, and an update arrives as both. To recover the current
contents of the relation, group by the data columns and keep the groups whose
`_mz_diff` sums above zero. See [Append
mode](/sql/create-sink/iceberg/#append-mode) for the full change encoding.

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-append-mode" %}}

## Considerations

### Commit interval tradeoffs {#commit-interval-tradeoffs}

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Credential refresh

The OAuth2 token Materialize exchanges its credentials for is short-lived, as
are the storage credentials Unity Catalog vends. Materialize refreshes both while
the sink runs, so a long-running sink needs no intervention. Rotating the service
principal's OAuth secret in Databricks does require updating the Materialize
secret:

```mzsql
ALTER SECRET databricks_oauth AS '<client_id>:<new_client_secret>';
```

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

- Materialize does not create schemas. The Unity Catalog schema named by the
  sink's `NAMESPACE` option must already exist.

- Materialize can only sink into *managed* Iceberg tables. Foreign Iceberg
  tables and Delta tables are read-only through the Iceberg REST catalog.

- Only `MODE APPEND` sinks are supported. `MODE UPSERT` expresses retractions as
  Iceberg equality delete files, which Unity Catalog managed tables do not
  accept.

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

If the sink reports an error, start with the sink's own status:

```mzsql
SELECT name, error FROM mz_internal.mz_sink_statuses WHERE name = '<sink_name>';
```

| Error | Cause |
| --- | --- |
| Token exchange failures | `OAUTH2 SERVER URL` or `SCOPE` does not match what the workspace expects, the service principal is not assigned to the workspace, or its OAuth secret has been rotated or revoked. |
| Authentication failures on every catalog request | External data access is not enabled on the metastore, or the service principal is missing a metastore grant (`USE METASTORE REMOTELY`, `READ METADATA`) or `EXTERNAL USE SCHEMA` on the schema. |
| A namespace-not-found error when the sink starts | The schema named by `NAMESPACE` does not exist, or the service principal cannot see it. |
| Storage errors once the sink is running | `ACCESS DELEGATION = 'vended-credentials'` is not set on the connection, or the service principal lacks `CREATE STORAGE CREDENTIAL` on the metastore. Unity Catalog vends credentials as the only way to reach its storage. |

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Storage access delegation](/sql/create-connection/#iceberg-catalog-access-delegation)
- [Databricks: Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
