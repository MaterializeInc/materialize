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

- [Avro over Kafka](./avro-source)
- [Protobuf over Kafka](./protobuf-source)
- [Local CSV files](./csv-source)
- [Bytes or unstructured text from local files](./text-source)

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SELECT`](../select)
