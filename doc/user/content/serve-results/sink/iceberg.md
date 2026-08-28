---
title: "Apache Iceberg"
description: "How to export results from Materialize to Apache Iceberg tables."
menu:
  main:
    parent: sink
    name: "Apache Iceberg"
    identifier: sink-iceberg
    weight: 15
---

{{< public-preview />}}

Iceberg sinks provide exactly once delivery of updates from Materialize into
[Apache Iceberg](https://iceberg.apache.org/)[^1] tables. As data changes in
Materialize, the corresponding Iceberg tables are automatically kept up to date.
You can sink data from a materialized view, a source, or a table.

Materialize reaches your tables through an Iceberg catalog. Follow the guide for
the catalog hosting them:

- [AWS S3
  Tables](/serve-results/sink/iceberg-aws/)[^2], which authenticates through an
  AWS connection.
- [GCP BigLake](/serve-results/sink/iceberg-gcp/)[^3] {{< private-preview-inline />}},
  which authenticates through a GCP connection.
- [Iceberg REST catalog](/serve-results/sink/iceberg-rest/) {{< private-preview-inline />}},
  for any catalog implementing the [Iceberg REST catalog
  specification](https://iceberg.apache.org/spec/) with OAuth2 credentials.
  [Databricks Unity
  Catalog](https://docs.databricks.com/aws/en/external-access/iceberg) is such a
  catalog: follow this guide to sink into it.

[^1]: [Apache Iceberg](https://iceberg.apache.org/) is an open table format for
large-scale analytics datasets.

[^2]: [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) is
    an AWS feature that provides fully managed Apache Iceberg tables as a native
    S3 storage type.

[^3]: [Google Cloud
BigLake](https://cloud.google.com/biglake) provides a managed Apache Iceberg
    REST catalog over Google Cloud Storage.
