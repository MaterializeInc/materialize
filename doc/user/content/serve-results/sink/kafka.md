---
title: "Kafka and Redpanda"
description: "How to export results from Materialize to Kafka/Redpanda."
aliases:
  - /serve-results/kafka/
menu:
  main:
    parent: sink
    name: "Kafka and Redpanda"
    weight: 25
---

<!-- Ported over content from sink-kafka.md. -->

## Connectors

Materialize bundles a **native connector** that allow writing data to Kafka and
Redpanda. When a user defines a sink to Kafka/Redpanda, Materialize
automatically generates the required schema and writes down the stream of
changes to that view or source. In effect, Materialize sinks act as change data
capture (CDC) producers for the given source or view.

For details on the connector, including syntax, supported formats and examples,
refer to [`CREATE SINK`](/sql/create-sink/kafka).

{{< tip >}}

Redpanda uses the same syntax as Kafka [`CREATE SINK`](/sql/create-sink/kafka).

{{< /tip >}}

## Features

### Memory use during creation

During creation, sinks need to load an entire snapshot of the data in memory.

### Automatic topic creation

If the specified Kafka topic does not exist, Materialize will attempt to create
it using the broker's default number of partitions, default replication factor,
default compaction policy, and default retention policy, unless any specific
overrides are provided as part of the [connection
options](/sql/create-sink/kafka#connection-options).

If the connection's [progress
topic](/sql/create-sink/kafka#exactly-once-processing) does not exist,
Materialize will attempt to create it with a single partition, the broker's
default replication factor, compaction enabled, and both size- and time-based
retention disabled. The replication factor can be overridden using the `PROGRESS
TOPIC REPLICATION FACTOR` option when creating a connection [`CREATE
CONNECTION`](/sql/create-connection).

To customize topic-level configuration, including compaction settings and other
values, use the `TOPIC CONFIG` option in the [connection
options](/sql/create-sink/kafka#connection-options) to set any relevant kafka
[topic configs](https://kafka.apache.org/documentation/#topicconfigs).

If you manually create the topic or progress topic in Kafka before
running `CREATE SINK`, observe the following guidance:

| Topic          | Configuration       | Guidance
|----------------|---------------------|---------
| Data topic     | Partition count     | Your choice, based on your performance and ordering requirements.
| Data topic     | Replication factor  | Your choice, based on your durability requirements.
| Data topic     | Compaction          | Your choice, based on your downstream applications' requirements. If using the [Upsert envelope](/sql/create-sink/kafka#upsert), enabling compaction is typically the right choice.
| Data topic     | Retention           | Your choice, based on your downstream applications' requirements.
| Progress topic | Partition count     | **Must be set to 1.** Using multiple partitions can cause Materialize to violate its [exactly-once guarantees](/sql/create-sink/kafka#exactly-once-processing).
| Progress topic | Replication factor  | Your choice, based on your durability requirements.
| Progress topic | Compaction          | We recommend enabling compaction to avoid accumulating unbounded state. Disabling compaction may cause performance issues, but will not cause correctness issues.
| Progress topic | Retention           | **Must be disabled.** Enabling retention can cause Materialize to violate its [exactly-once guarantees](/sql/create-sink/kafka#exactly-once-processing).
| Progress topic | Tiered storage      | We recommend disabling tiered storage to allow for more aggressive data compaction. Fully compacted data requires minimal storage, typically only tens of bytes per sink, making it cost-effective to maintain directly on local disk.
{{< warning >}}
{{% kafka-sink-drop %}}
{{</ warning >}}

### Exactly-once processing

By default, Kafka sinks provide [exactly-once processing guarantees](https://kafka.apache.org/documentation/#semantics), which ensures that messages are not duplicated or dropped in failure scenarios.

To achieve this, Materialize stores some internal metadata in an additional
*progress topic*. This topic is shared among all sinks that use a particular
[Kafka connection](/sql/create-connection/#kafka). The name of the progress
topic can be specified when [creating a
connection](/sql/create-connection/#kafka-options); otherwise, a default name of
`_materialize-progress-{REGION ID}-{CONNECTION ID}` is used. In either case,
Materialize will attempt to create the topic if it does not exist. The contents
of this topic are not user-specified.

#### End-to-end exactly-once processing

Exactly-once semantics are an end-to-end property of a system, but Materialize
only controls the initial produce step. To ensure _end-to-end_ exactly-once
message delivery, you should ensure that:

- The broker is configured with replication factor greater than 3, with unclean
  leader election disabled (`unclean.leader.election.enable=false`).
- All downstream consumers are configured to only read committed data
  (`isolation.level=read_committed`).
- The consumers' processing is idempotent, and offsets are only committed when
  processing is complete.

For more details, see [the Kafka documentation](https://kafka.apache.org/documentation/).

### Partitioning

By default, Materialize assigns a partition to each message using the following
strategy:

  1. Encode the message's key in the specified format.
  2. If the format uses a Confluent Schema Registry, strip out the
     schema ID from the encoded bytes.
  3. Hash the remaining encoded bytes using [SeaHash].
  4. Divide the hash value by the topic's partition count and assign the
     remainder as the message's partition.

If a message has no key, all messages are sent to partition 0.

To configure a custom partitioning strategy, you can use the `PARTITION BY`
option. This option allows you to specify a SQL expression that computes a hash
for each message, which determines what partition to assign to the message:

```sql
-- General syntax.
CREATE SINK ... INTO KAFKA CONNECTION <name> (PARTITION BY = <expression>) ...;

-- Example.
CREATE SINK ... INTO KAFKA CONNECTION <name> (
    PARTITION BY = kafka_murmur2(name || address)
) ...;
```

The expression:
  * Must have a type that can be assignment cast to [`uint8`].
  * Can refer to any column in the sink's underlying relation when using the
    [upsert envelope](/sql/create-sink/kafka#upsert-envelope).
  * Can refer to any column in the sink's key when using the
    [Debezium envelope](/sql/create-sink/kafka#debezium-envelope).

Materialize uses the computed hash value to assign a partition to each message
as follows:

  1. If the hash is `NULL` or computing the hash produces an error, assign
     partition 0.
  2. Otherwise, divide the hash value by the topic's partition count and assign
     the remainder as the message's partition (i.e., `partition_id = hash %
     partition_count`).

Materialize provides several [hash functions](/sql/functions/#hash-functions)
which are commonly used in Kafka partition assignment:

  * `crc32`
  * `kafka_murmur2`
  * `seahash`

For a full example of using the `PARTITION BY` option, see [Custom
partioning](/sql/create-sink/kafka#custom-partitioning).

### Kafka transaction markers

{{< include-md file="shared-content/kafka-transaction-markers.md" >}}
