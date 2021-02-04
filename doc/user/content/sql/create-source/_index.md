---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
menu:
  main:
    identifier: create-source
    parent: sql
disable_list: true
disable_toc: true
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../overview/api-components#sources).

## Types of sources

Materialize can connect to many different external sources of data, each with
their own requirements. For details about creating sources, view the
documentation for the type of data you are trying to load into Materialize:

| Data format | Kafka | Kinesis (Alpha) | S3 (Experimental) | Local files |
|-------------|-------|-----------------|-------------------|-------------|
| Avro | [Avro + Kafka](./avro-kafka) | - | - | [Avro + file](./avro-file) |
| Text/bytes | [Text/bytes + Kafka](./text-kafka) | [Text/bytes + Kinesis](./text-kinesis) | [Text/Bytes + S3](./text-s3) | [Text/bytes + file](./text-file) |
| Protobuf | [Protobuf + Kafka](./protobuf-kafka) | [Protobuf + Kinesis](./protobuf-kinesis) | - | - |
| CSV | [CSV + Kafka](./csv-kafka) | [CSV + Kinesis](./csv-kinesis) | [CSV + S3](./csv-s3) | [CSV + files](./csv-file) |
| JSON | [JSON + Kafka](./json-kafka) | [JSON + Kinesis](./json-kinesis) | [JSON + S3](./json-s3) | [JSON + file](./text-file) |

Don't see what you're looking for? [Let us know on GitHub](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md).

{{< kinesis-alpha >}}

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
