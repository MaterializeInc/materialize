---
title: "CREATE SOURCE: JSON over Kafka"
description: "Learn how to connect Materialize to JSON-formatted Kafka topics"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to JSON-formatted Kafka
topics.

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-json-kafka.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="json-bytes" envelopes="append-only" %}}

## Examples

```sql
CREATE SOURCE json_kafka
FROM KAFKA BROKER 'localhost:9092' TOPIC 'json'
FORMAT BYTES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.

To use this data in views, you can decode its bytes into
[`jsonb`](/sql/types/jsonb). For example:

```sql
CREATE MATERIALIZED VIEW jsonified_kafka_source AS
  SELECT CAST(data AS jsonb) AS data
  FROM (
      SELECT convert_from(data, 'utf8') AS data
      FROM json_kafka
  )
```

### Persisting records to local disk

```sql
CREATE SOURCE json_kafka
FROM KAFKA BROKER 'localhost:9092' TOPIC 'json'
WITH (persistence = true)
FORMAT BYTES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.
- Persists messages from the `json` topic to local disk.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
