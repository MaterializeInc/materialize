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

#### `key_constraint`

{{< diagram "key-constraint.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="kafka" formats="protobuf" envelopes="append-only upsert" keyConstraint=true %}}

### Confluent Schema Registry details

When using Confluent Schema Registry with Protobuf sources, the registered
Protobuf schemas must contain exactly one `Message` definition. We expect to
lift this restriction in the future {{% gh 9598 %}}.

## Examples

### Receiving Protobuf messages using Confluent Schema Registry

```sql
CREATE SOURCE batches
FROM KAFKA BROKER 'localhost:9092' TOPIC 'billing'
FORMAT PROTOBUF USING SCHEMA REGISTRY 'http://localhost:8081';
```

This creates a source that...

- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data from the Protobuf message published to the `billing-value`
  subject in the Confluent Schema Registry running on `localhost:8081`.

### Receiving Protobuf messages using a schema file

Assuming you've already generated a [`FileDescriptorSet`](#filedescriptorset)
named `billing.pb` that contains a message named `billing.Batch`:

```sql
CREATE SOURCE batches
FROM KAFKA BROKER 'localhost:9092' TOPIC 'billing'
WITH (cache = true)
FORMAT PROTOBUF MESSAGE 'billing.Batch' USING SCHEMA FILE 'billing.pb';
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
FROM KAFKA BROKER 'localhost:9092' TOPIC 'billing' WITH (
    security_protocol = 'SSL',
    ssl_key_location = '/secrets/materialized.key',
    ssl_certificate_location = '/secrets/materialized.crt',
    ssl_ca_location = '/secrets/ca.crt',
    ssl_key_password = 'mzmzmz'
)
FORMAT PROTOBUF MESSAGE 'billing.Batch'
  USING SCHEMA FILE '[path to schema]';
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
FROM KAFKA BROKER 'localhost:9092' TOPIC 'billing'
WITH (start_offset=[0,10,100])
FORMAT PROTOBUF MESSAGE 'billing.Batch'
  USING SCHEMA FILE '[path to schema]';
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
