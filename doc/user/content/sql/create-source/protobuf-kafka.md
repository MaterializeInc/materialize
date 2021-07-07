---
title: "CREATE SOURCE: Protobuf over Kafka"
description: "Learn how to connect Materialize to a Protobuf-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/proto
    - /sql/create-source/protobuf
    - /sql/create-source/protobuf-source
---

{{% create-source/intro %}}
This document details how to connect Materialize to Protobuf-formatted Kafka
topics.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-protobuf-kafka.svg" >}}

### key_constraint

{{< diagram "key-constraint.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="protobuf" envelopes="append-only" keyConstraint=true %}}

## Examples

### Receiving Protobuf messages

Assuming you've already generated a [`FileDescriptorSet`](#filedescriptorset)
named `SCHEMA`:

```sql
CREATE SOURCE batches
KAFKA BROKER 'localhost:9092' TOPIC 'billing'
FORMAT PROTOBUF MESSAGE '.billing.Batch' USING '[path to schema]';
```

This creates a source that...

- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#filedescriptorset).

### Connecting to a Kafka broker using SSL authentication

```sql
CREATE SOURCE batches
KAFKA BROKER 'localhost:9092' TOPIC 'billing' WITH (
    security_protocol = 'SSL',
    ssl_key_location = '/secrets/materialized.key',
    ssl_certificate_location = '/secrets/materialized.crt',
    ssl_ca_location = '/secrets/ca.crt',
    ssl_key_password = 'mzmzmz'
)
FORMAT PROTOBUF MESSAGE '.billing.Batch' USING '[path to schema]';
```

This creates a source that...
- Connects to a Kafka broker that requires SSL authentication.
- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#filedescriptorset).

### Setting partition offsets

```sql
CREATE SOURCE batches
  KAFKA BROKER 'localhost:9092' TOPIC 'billing'
  WITH (start_offset=[0,10,100])
  FORMAT PROTOBUF MESSAGE '.billing.Batch' USING '[path to schema]';
```

This creates a source that...

- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#filedescriptorset).
- Starts reading with no offset on the first partition, the second partition at 10, and the third partition at 100.

It is possible to set `start_offset` based on Kafka timestamps using the `kafka_time_offset` option.

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
