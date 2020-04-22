---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
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

- [Avro, raw bytes, or raw text over Kafka](./avro-source)
- [Protobuf over Kafka](./protobuf-source)
- [JSON over Kinesis](./kinesis-source)
- [Local CSV files](./csv-source)
- [Other local files (e.g. text, JSON)](./text-source)

Don't see what you're looking for? [Let us know on GitHub](https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md).

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
