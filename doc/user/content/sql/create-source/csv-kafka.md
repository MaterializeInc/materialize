---
title: "CREATE SOURCE: CSV over Kafka"
description: "Learn how to connect Materialize to a CSV-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a CSV-formatted Kafka topic. You can also use these instructions to connect to Kafka-compatible [Redpanda](/third-party/redpanda/) topics.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-csv-kafka.svg" >}}

#### `key_constraint`

{{< diagram "key-constraint.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="csv" envelopes="append-only" keyConstraint=true %}}

## Example

### Creating a source from a CSV-formatted Kafka topic

```sql
CREATE SOURCE csv_kafka (col_foo, col_bar, col_baz)
FROM KAFKA BROKER 'localhost:9092' TOPIC 'csv'
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.

### Setting partition offsets

```sql
CREATE SOURCE csv_kafka (col_foo, col_bar, col_baz)
FROM KAFKA BROKER 'localhost:9092' TOPIC 'csv'
WITH (start_offset=[0,10,100])
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.
- Starts reading with no offset on the first partition, the second partition at 10, and the third partition at 100.

It is possible to set `start_offset` based on Kafka timestamps using the `kafka_time_offset` option.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
