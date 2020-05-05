---
title: "CREATE SOURCE: Protobuf"
description: "Learn how to connect Materialize to a Protobuf-formatted Kafka topic"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/proto
    - /docs/sql/create-source/protobuf
    - /docs/sql/create-source/protobuf-source
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to an Protobuf-formatted Kafka
topic. For other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

Sources represent connections to resources outside Materialize that it can read
data from. For more information, see [API Components:
Sources](../../../overview/api-components#sources).

## Syntax

{{< diagram "create-source-protobuf-kafka.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic_ | The Kafka topic you want to subscribe to.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
_message&lowbar;name_ | The top-level Protobuf message name, in the format `<package>.<message name>`. For example, `billing.Batch`. For more detail, see [Top-level message](#top-level-message).
_schema&lowbar;file&lowbar;path_ | The absolute path to a file containing the [`FileDescriptorSet`](#filedescriptorset).
_inline&lowbar;schema_ | A string representing the [`FileDescriptorSet`](#filedescriptorset).

{{% create-source/kafka-with-options %}}

## Details

This document assumes you're using Kafka to send Protobuf-encoded data to
Materialize.

{{% create-source/kafka-source-details format="protobuf" %}}

### Protobuf format details

Protobuf-formatted external sources require:

- `FileDescriptSet`
- Top-level message name

#### `FileDescriptorSet`

The `FileDescriptorSet` encodes the Protobuf messages' schema, which Materialize
needs to decode incoming Protobuf data.

You can generate the `FileDescriptorSet` with `protoc`, e.g.

```shell
protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
```

#### Top-level message

Materialize needs to know which message from your `FileDescriptorSet` is the
top-level message to decode, along with its package name, in the following
format:

```shell
<package name>.<top-level message>
```

For example, if our `FileDescriptorSet` were from a `.proto` file in the
`billing` package, and our top-level message was called `Batch`, our
_message&lowbar;name_ value would be:

```nofmt
billing.Batch
```

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

Protobuf-encoded data can currently only be handled as append-only by
Materialize.

## Examples

### Receiving Protobuf messages

Assuming you've already generated a [`FileDescriptorSet`](#filedescriptorset)
named `SCHEMA`:

```sql
CREATE SOURCE batches
KAFKA BROKER 'localhost:9092' TOPIC 'billing'
FORMAT PROTOBUF MESSAGE '.billing.Batch'
    USING '[path to SCHEMA]';
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
KAFKA BROKER 'localhost:9092' TOPIC 'billing'
    WITH (
      security_protocol='SSL',
      ssl_key_location='/secrets/materialized.key',
      ssl_certificate_location='/secrets/materialized.crt',
      ssl_ca_location='/secrets/ca.crt',
      ssl_key_password='mzmzmz'
    )
FORMAT PROTOBUF MESSAGE '.billing.Batch'
    USING '[path to SCHEMA]';
```

This creates a source that...
- Connects to a Kafka broker that requires SSL authentication.
- Is append-only.
- Decodes data received from the `billing` topic published by Kafka running on
  `localhost:9092`.
- Decodes data as the `Batch` message from the `billing` package, as described
  in the [generated `FileDescriptorSet`](#filedescriptorset).

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
