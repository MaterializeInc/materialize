---
title: "Iceberg REST catalog"
description: "How to export results from Materialize to Apache Iceberg tables through a catalog implementing the Iceberg REST specification."
menu:
  main:
    parent: sink-iceberg
    name: "Iceberg REST catalog"
    weight: 30
---

{{< private-preview />}}

This guide walks you through the steps required to set up Iceberg sinks in
Materialize Cloud against a catalog implementing the [Iceberg REST catalog
specification](https://iceberg.apache.org/spec/) with OAuth2 authentication.
Databricks Unity Catalog and Apache Polaris are both such catalogs.

AWS S3 Tables and Google Cloud BigLake also speak Iceberg REST, but they
authenticate through an AWS or GCP connection rather than OAuth2 credentials.
Follow [AWS S3 Tables](/serve-results/sink/iceberg-aws/) or [GCP
BigLake](/serve-results/sink/iceberg-gcp/) instead for those.

## Prerequisites

- Catalog implementing the Iceberg REST specification, reachable from
  Materialize over the public internet.
- OAuth2 client credentials for the catalog, as a client ID and client secret.
- Namespace in the catalog. Materialize creates the *table* a sink writes to,
  but not the namespace containing it, so the namespace must exist first.
- Whatever privileges the catalog requires for creating a table in that
  namespace and committing to it. See [Catalog-specific
  settings](#catalog-specific-settings) below.

## Create the Iceberg catalog connection in Materialize

### Step 1. Store the catalog credentials

Store the client ID and client secret as a single value, separated by a colon:

```mzsql
CREATE SECRET iceberg_catalog_credential AS '<client_id>:<client_secret>';
```

### Step 2. Create the Iceberg catalog connection

{{% include-syntax file="examples/create_connection" example="syntax-iceberg-catalog-rest" %}}

Three of these options exist for catalogs that deviate from the defaults the
Iceberg REST specification defines, and can be omitted otherwise:

- `OAUTH2 SERVER URL`, when the catalog does not serve its token endpoint at
  `<url>/v1/oauth/tokens`.
- `SCOPE`, when the catalog does not accept the specification's `catalog` scope.
- `ACCESS DELEGATION`, when the catalog manages its own storage and expects
  clients to use credentials it vends. See [Storage access
  delegation](/sql/create-connection/#iceberg-catalog-access-delegation).

## Catalog-specific settings

### Databricks Unity Catalog

Unity Catalog requires all three of the options above. It serves its token
endpoint on a path unrelated to the catalog URL, does not grant the `catalog`
scope, and manages the storage behind its tables without handing out long-lived
credentials for it.

{{% include-example file="examples/create_connection"
example="example-iceberg-catalog-databricks-connection" %}}

| Option | Value for Unity Catalog |
| --- | --- |
| `URL` | `https://<workspace>.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest` |
| `WAREHOUSE` | The name of the Unity Catalog catalog holding your tables. Unlike other catalogs, this is not a storage location. |
| `OAUTH2 SERVER URL` | `https://<workspace>.cloud.databricks.com/oidc/v1/token` |
| `SCOPE` | `all-apis` |
| `ACCESS DELEGATION` | `'vended-credentials'` |

Before creating the connection, in Databricks:

1. [Enable external data access on the
   metastore](https://docs.databricks.com/aws/en/external-access/admin). Unity
   Catalog rejects Iceberg REST requests until this is enabled.
2. Create a [service
   principal](https://docs.databricks.com/aws/en/admin/users-groups/service-principals)
   with an OAuth secret, and use its client ID and secret in [Step
   1](#step-1-store-the-catalog-credentials).
3. Grant the service principal these privileges on the schema that will hold
   your Iceberg tables:

    | Privilege | Why it is needed |
    | --- | --- |
    | `EXTERNAL USE SCHEMA` | Allows Iceberg REST clients to reach the schema's tables. Without it, every catalog request is rejected. |
    | `CREATE TABLE` | Materialize creates the Iceberg table the first time the sink runs. |
    | `MODIFY` | Materialize commits new snapshots as data changes. |
    | `SELECT` | Materialize reads the table's current metadata before each commit. |

    The service principal also needs `USE CATALOG` on the catalog and
    `USE SCHEMA` on the schema.

Set the sink's `NAMESPACE` option to the Unity Catalog schema you granted these
privileges on.

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

The OAuth2 token Materialize exchanges its credentials for is short-lived, as
are any storage credentials the catalog vends. Materialize refreshes both while
the sink runs, so a long-running sink needs no intervention. Rotating the
credentials at the catalog does require updating the Materialize secret:

```mzsql
ALTER SECRET iceberg_catalog_credential AS '<client_id>:<new_client_secret>';
```

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

- Materialize does not create namespaces. The namespace named by the sink's
  `NAMESPACE` option must already exist in the catalog.

- Against Databricks Unity Catalog, Materialize can only sink into *managed*
  Iceberg tables. Foreign Iceberg tables and Delta tables are read-only through
  the Iceberg REST catalog.

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

If the sink reports an error, start with the sink's own status:

```mzsql
SELECT name, error FROM mz_internal.mz_sink_statuses WHERE name = '<sink_name>';
```

| Error | Cause |
| --- | --- |
| Token exchange failures | `OAUTH2 SERVER URL` or `SCOPE` does not match what the catalog expects, or the credentials have been rotated or revoked. |
| Authentication failures on every catalog request | The credentials lack the privileges the catalog requires. For Unity Catalog, check that external data access is enabled on the metastore and that the service principal holds `EXTERNAL USE SCHEMA`. |
| A namespace-not-found error when the sink starts | The namespace named by `NAMESPACE` does not exist, or the credentials cannot see it. |
| Storage errors once the sink is running | The catalog manages its own storage and expects vended credentials. Set `ACCESS DELEGATION = 'vended-credentials'` on the connection. |

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Storage access delegation](/sql/create-connection/#iceberg-catalog-access-delegation)
- [Databricks: Access Databricks tables from Apache Iceberg clients](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
