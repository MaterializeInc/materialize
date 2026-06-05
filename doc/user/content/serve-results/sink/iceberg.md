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
[Apache Iceberg](https://iceberg.apache.org/)[^1] tables hosted on either
[Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html)[^2]
or [Google Cloud BigLake](https://cloud.google.com/biglake)[^3]. As data
changes in Materialize, the corresponding Iceberg tables are automatically
kept up to date. You can sink data from a materialized view, a source, or a
table.

Follow the guide for the platform hosting your Iceberg tables:

- [AWS S3 Tables](/serve-results/sink/iceberg-aws/)
- [GCP BigLake](/serve-results/sink/iceberg-gcp/)

[^1]: [Apache Iceberg](https://iceberg.apache.org/) is an open table format for
large-scale analytics datasets.

[^2]: [Amazon S3
Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html) is
    an AWS feature that provides fully managed Apache Iceberg tables as a native
    S3 storage type.

[^3]: [Google Cloud
BigLake](https://cloud.google.com/biglake) provides a managed Apache Iceberg
    REST catalog over Google Cloud Storage.
