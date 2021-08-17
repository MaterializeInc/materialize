---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
menu:
  # This should also have a "non-content entry" under Connect, which is
  # configured in doc/user/config.toml
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

## Materialized source details

Materializing a source keeps data it receives in an in-memory
[index](/overview/api-components/#indexes), the presence of which makes the
source directly queryable. In contrast, non-materialized sources cannot process
queries directly; to access the data the source receives, you need to create
[materialized views](/sql/create-materialized-view) that `SELECT` from the
source.

For a mental model, materializing the source is approximately equivalent to
creating a non-materialized source, and then creating a materialized view from
all of the source's columns:

```sql
CREATE SOURCE src ...;
CREATE MATERIALIZED VIEW src_view AS SELECT * FROM src;
```

The actual implementation of materialized sources differs, though, by letting
you refer to the source's name directly in queries.

For more details about the impact of materializing sources (and implicitly
creating an index), see [`CREATE INDEX`: Details &mdash; Memory
footprint](/sql/create-index/#memory-footprint).

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
Local files     | [Avro + file](./avro-file)   | [Text/bytes + file](./text-file)       | -                                        | [CSV + files](./csv-file)      | [JSON + file](./json-file)
[Postgres](./postgres)  | -  | - | -  | -  | -


Don't see what you're looking for? [Let us know on GitHub](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md).

## Related pages

- [API Components](../../overview/api-components)
- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
