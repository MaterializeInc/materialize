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
FROM KAFKA BROKER 'localhost:9092' TOPIC 'csv'
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.

### Caching records to local disk

```sql
CREATE SOURCE csv_kafka (col_foo, col_bar, col_baz)
FROM KAFKA BROKER 'localhost:9092' TOPIC 'csv'
WITH (cache = true)
FORMAT CSV WITH 3 COLUMNS;
```

This creates a source that...

- Is append-only.
- Has 3 columns (`col_foo`, `col_bar`, `col_baz`). Materialize will not ingest
  any row without 3 columns.
- Caches messages from the `csv` topic to local disk.

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

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
