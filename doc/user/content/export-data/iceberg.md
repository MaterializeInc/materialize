---
title: "Apache Iceberg"
description: "How to export results from Materialize to Apache Iceberg tables."
menu:
    main:
        parent: sink
        name: "Apache Iceberg"
        identifier: sink-iceberg
        weight: 20
aliases:
  - /serve-results/sink/iceberg/
---

{{< public-preview />}}

Iceberg sinks provide exactly once delivery of updates from Materialize into
[Apache Iceberg](https://iceberg.apache.org/)[^1] tables. As data changes in
Materialize, the corresponding Iceberg tables are automatically kept up to date.
You can sink data from a materialized view, a source, or a table.

## Create an Iceberg sink

Materialize reaches your tables through an Iceberg catalog. Follow the guide for
the catalog hosting them:

- [AWS S3
  Tables](/export-data/iceberg-aws/)[^2], which authenticates through an
  AWS connection.
- [GCP BigLake](/export-data/iceberg-gcp/)[^3] {{< private-preview-inline />}},
  which authenticates through a GCP connection.
- [Databricks Unity Catalog](/export-data/iceberg-databricks/)[^4] on
  AWS, which authenticates with the OAuth2 credentials of a Databricks service
  principal.

## Consume an Iceberg sink

A sink created with `MODE APPEND` writes a changelog rather than current state,
so consuming it means reconstructing current state from the `_mz_diff` column:

{{% include-headless "/headless/iceberg-sinks/append-mode-current-state" %}}

For the query to do this in a specific engine's dialect, along with the setup
that engine requires, see:

- [Snowflake on AWS S3 Tables](/export-data/iceberg-aws-snowflake/)

[^1]:
    [Apache Iceberg](https://iceberg.apache.org/) is an open table format for
    large-scale analytics datasets.

[^2]:
    [Amazon S3
    Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) is
    an AWS feature that provides fully managed Apache Iceberg tables as a native
    S3 storage type.

[^3]:
    [Google Cloud
    BigLake](https://cloud.google.com/biglake) provides a managed Apache Iceberg
    REST catalog over Google Cloud Storage.

[^4]:
    [Databricks Unity
    Catalog](https://docs.databricks.com/aws/en/external-access/iceberg) exposes
    its tables to Apache Iceberg clients through an Iceberg REST catalog
    endpoint.
