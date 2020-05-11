---
title: "CREATE SOURCE: Text or bytes over Kafka"
description: "Learn how to connect Materialize to an Avro-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a text- or byte–formatted
Kafka topic.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-text-kafka.html" >}}

{{% create-source/syntax-details connector="kafka" formats="text bytes" envelopes="upsert append-only" %}}

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

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
