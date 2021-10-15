---
title: "CREATE SOURCE: JSON over Kafka"
description: "Learn how to connect Materialize to JSON-formatted Kafka topics"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to JSON-formatted Kafka
topics. You can also use these instructions to connect to [Redpanda](/third-party/redpanda/) as a Kafka broker.

{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-json-kafka.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="json-bytes" envelopes="append-only upsert" keyConstraint=false %}}

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
  );
```

### Setting partition offsets

```sql
CREATE MATERIALIZED SOURCE data_offset
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  WITH (start_offset=[0,10,100])
  FORMAT BYTES;
```

This creates a source that...

- Is append-only.
- Has one column, `data`, which represents the stream's incoming bytes.
- Starts reading with no offset on the first partition, the second partition at 10, and the third partition at 100.

It is possible to set `start_offset` based on Kafka timestamps using the `kafka_time_offset` option.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
