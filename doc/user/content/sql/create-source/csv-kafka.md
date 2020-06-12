---
title: "CREATE SOURCE: CSV over Kafka"
description: "Learn how to connect Materialize to a CSV-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a CSV-formatted Kafka topic.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-csv-kafka.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="csv" envelopes="append-only" %}}

## Example

### Creating a source from a CSV-formatted Kafka topic

```sql
CREATE SOURCE csv_kafka (col_foo, col_bar, col_baz)
KAFKA BROKER 'localhost:9092' TOPIC 'csv'
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
