---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
menu:
  # This should also have a "non-content entry" under Connect, which is
  # configured in doc/user/config.toml
  main:
    parent: reference
    name: Sources
    identifier: 'create-source'
    weight: 20
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [Key Concepts:
Sources](../../overview/key-concepts/#sources).

## Types of sources

Materialize can connect to many different external sources of data, each with
their own requirements. For details about creating sources, view the
documentation for the type of data you are trying to load into Materialize:

Source type     | Avro                         | Text/bytes                             | Protobuf                                 | CSV                            | JSON
----------------|------------------------------|----------------------------------------|------------------------------------------|--------------------------------|---------------------------------
Kafka           | [Avro + Kafka](./avro-kafka) | [Text/bytes + Kafka](./text-kafka)     | [Protobuf + Kafka](./protobuf-kafka)     | [CSV + Kafka](./csv-kafka)     | [JSON + Kafka](./json-kafka)
Kinesis         | -                            | [Text/bytes + Kinesis](./text-kinesis) | [Protobuf + Kinesis](./protobuf-kinesis) | [CSV + Kinesis](./csv-kinesis) | [JSON + Kinesis](./json-kinesis)
S3              | -                            | [Text/bytes + S3](./text-s3)           | -                                        | [CSV + S3](./csv-s3)           | [JSON + S3](./json-s3)
PubNub          | -                            | [Text + PubNub](./text-pubnub)         | -                                        | -                              | [JSON + PubNub](./json-pubnub)
[Postgres](./postgres)  | -  | - | -  | -  | -


Don't see what you're looking for? [Let us know on GitHub](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md).

## Related pages

- [Key Concepts](../../overview/key-concepts/)
- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
