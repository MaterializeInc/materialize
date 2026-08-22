---
title: "Self-managed (REST catalog)"
description: "How to export results from Materialize to Apache Iceberg tables in a self-managed Iceberg REST catalog and S3-compatible object storage."
menu:
  main:
    parent: sink-iceberg
    name: "Self-managed (REST catalog)"
    weight: 30
---

{{< private-preview />}}

{{< warning >}}
Materialize does not perform Iceberg table maintenance on this path. You are
responsible for running compaction, snapshot expiration, and orphan file
cleanup yourself. Without maintenance, query performance on the sinked tables
degrades over time and storage usage grows unboundedly. See
[Table maintenance](#table-maintenance).
{{< /warning >}}

This guide walks you through the steps required to set up Iceberg sinks
against a self-managed [Iceberg REST
catalog](https://iceberg.apache.org/rest-catalog-spec/) backed by
S3-compatible object storage, rather than a managed offering like AWS S3
Tables or GCP BigLake. Materialize continuously tests this configuration
against [Apache Polaris](https://polaris.apache.org/) and
[MinIO](https://min.io/). Other spec-compliant catalog and storage
combinations are expected to work but are not individually validated.

## Prerequisites

- An Iceberg REST catalog that implements the [Iceberg REST catalog
  specification](https://iceberg.apache.org/rest-catalog-spec/), reachable
  from your Materialize deployment.
- The catalog must support **OAuth2 client credentials** authentication.
- The catalog must support **storage credential vending** (also called access
  delegation). When Materialize loads a table, the catalog must return the
  storage configuration and credentials (for example `s3.access-key-id`,
  `s3.secret-access-key`, and `s3.endpoint`) that Materialize uses to write
  data files. There is no way to configure object storage credentials on the
  sink directly.
- A namespace in the catalog to hold the sinked tables.

## Create the Iceberg catalog connection in Materialize

In Materialize, create an **Iceberg catalog connection** for the Iceberg sink
to use. To create, use [`CREATE CONNECTION ... TO ICEBERG
CATALOG`](/sql/create-connection/#iceberg-catalog), replacing:

- `<catalog_url>` with the base URL of your REST catalog, e.g.
  `https://polaris.example.com/api/catalog`,
- `<client_id>:<client_secret>` with the OAuth2 client credentials for your
  catalog,
- `<warehouse>` with the warehouse name as your catalog expects it, and
- `<scope>` with the OAuth2 scope, if your catalog requires one (e.g.
  `PRINCIPAL_ROLE:ALL` for Polaris).

```mzsql
CREATE SECRET iceberg_credential AS '<client_id>:<client_secret>';

CREATE CONNECTION iceberg_catalog_connection TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = '<catalog_url>',
    CREDENTIAL = SECRET iceberg_credential,
    WAREHOUSE = '<warehouse>',
    SCOPE = '<scope>'
);
```

## Create the Iceberg sink in Materialize

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-intro" %}}

### Upsert mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-upsert-mode" %}}

### Append mode

{{% include-example file="examples/create_sink_iceberg" example="tutorial-create-sink-append-mode" %}}

## Considerations

### Table maintenance

Managed Iceberg offerings like AWS S3 Tables run table maintenance for you.
On this path there is no maintenance service, so you must run it yourself.
Regularly perform, at minimum:

- **Compaction**: Materialize commits new data files on every commit
  interval, so tables accumulate many small files. Rewrite them into larger
  files to keep scans efficient.
- **Snapshot expiration**: every commit creates a new table snapshot. Expire
  old snapshots to allow underlying data files to be removed.
- **Orphan file cleanup**: remove files no longer referenced by any snapshot.

Tools that can run these operations include [Spark stored
procedures](https://iceberg.apache.org/docs/latest/spark-procedures/) and
your catalog's own maintenance features, if it has any.

### Commit interval tradeoffs {#commit-interval-tradeoffs}

{{% include-headless "/headless/iceberg-sinks/commit-interval-tradeoffs" %}}

### Exactly-once delivery

{{< include-from-yaml data="examples/create_sink_iceberg"
name="exactly-once-delivery" >}}

### Type mapping

{{% include-headless
  "/headless/iceberg-sinks/type-mapping" %}}

### Limitations

- Materialize authenticates to object storage exclusively with the
  credentials vended by the catalog. Catalogs that do not support credential
  vending are not supported.

{{% include-headless "/headless/iceberg-sinks/limitations-list" %}}

## Troubleshooting

{{% include-headless "/headless/iceberg-sinks/troubleshooting" %}}

## Related pages

- [`CREATE SINK`](/sql/create-sink/iceberg)
- [`CREATE CONNECTION`](/sql/create-connection)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
