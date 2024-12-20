---
title: "CockroachDB"
description: "Connecting Materialize to a CockroachDB source."
disable_list: true
menu:
  main:
    parent: "ingest-data"
    name: "CockroachDB"
    identifier: "crdb"
    weight: 16
---

## Change Data Capture (CDC)

Materialize supports CockroachDB as a real-time data source. Using Kafka with
CockroachDB
[Changefeeds](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview),
Materialize can consume changefeedsto create and efficiently maintain real-time
views on top of CDC data.

{{< tip >}}
{{< guided-tour-blurb-for-ingest-data >}}
{{< /tip >}}

## Integration guides

| Integration guides                          |
| ------------------------------------------- |
| <ul><li>[Kafka + Changefeeds](/ingest-data/cockroachdb/kafka-changefeeds)</li></ul>    |
