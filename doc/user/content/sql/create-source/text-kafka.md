---
title: "CREATE SOURCE: Text or bytes over Kafka"
description: "Learn how to connect Materialize to an text- or byte-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a text- or byte–formatted
Kafka topic. You can also use these instructions to connect to Kafka-compatible [Redpanda](/third-party/redpanda/) topics.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-text-kafka.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="text bytes" envelopes="upsert append-only" keyConstraint=false %}}

## Examples

### Upsert on a Kafka topic

```sql
CREATE SOURCE current_predictions
FROM KAFKA BROKER 'localhost:9092' TOPIC 'kv_feed'
FORMAT TEXT
USING SCHEMA FILE '/scratch/kv_feed.json'
ENVELOPE UPSERT;
```

This creates a source that...

- Has its schema in a file on disk, and decodes payload data using that schema.
- Decodes data received from the `kv_feed` topic published by Kafka running on
  `localhost:9092`.
- Uses message keys to determine what should be inserted, deleted, and updated.
- Treats both message keys and values as text.

### Setting partition offsets

```sql
CREATE MATERIALIZED SOURCE data_offset
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'data'
  WITH (start_offset=[0,10,100])
  FORMAT TEXT;
```

This creates a source that...

- Is append-only.
- Decodes data received from the `data` topic published by Kafka running on
  `localhost:9092` as text.
- Starts reading with no offset on the first partition, the second partition at 10, and the third partition at 100.

It is also possible to set `start_offset` based on Kafka timestamps using the `kafka_time_offset` option.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
