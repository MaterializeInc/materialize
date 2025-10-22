---
title: "CREATE SOURCE: Kafka/Redpanda"
description: "Connecting Materialize to a MySQL database."
menu:
  main:
    parent: 'create-source'
    name: "Kafka/Redpanda"
---

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source-v1/kafka)" >}}

{{% create-source-intro external_source="Kafka/Redpanda"
create_table="/sql/create-table/kafka/" %}}

{{< note >}}
The syntax, supported formats, and features are the
same for Kafka and Redpanda broker. For simplicity, this page uses
"Kafka" to refer to both Kafka and Redpanda.
{{</ note >}}

{{% include-md file="shared-content/kafka-formats.md" %}}

## Prerequisites

{{< include-md file="shared-content/kafka-source-prereq.md" >}}

## Syntax

{{% include-example file="examples/create_source/example_kafka_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_kafka_source"
 example="syntax-options" %}}

## Details

### Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option
Read           | Group            | All group IDs starting with the specified [`GROUP ID PREFIX` option](#connection-options)

### Supported message formats

{{< include-md file="shared-content/kafka-formats.md" >}}

You specify the message format when creating a table from a Kafka source. See
[CREATE TABLE: Kafka](/sql/create-table/kafka/) for details.

### Monitoring source progress

[Creating a table](/sql/create-table/kafka/) from a source starts the data
ingestion process.

{{% include-md file="shared-content/kafka-monitoring.md"%}}

### Monitoring consumer lag

To support Kafka tools that monitor consumer lag, Kafka sources commit offsets
once the messages up through that offset have been durably recorded in
Materialize's storage layer.

However, rather than relying on committed offsets, Materialize suggests using
our native [progress monitoring](#monitoring-source-progress), which contains
more up-to-date information.

{{< note >}}
Some Kafka monitoring tools may indicate that Materialize's consumer groups have
no active members. This is **not a cause for concern**.

Materialize does not participate in the consumer group protocol nor does it
recover on restart by reading the committed offsets. The committed offsets are
provided solely for the benefit of Kafka monitoring tools.
{{< /note >}}

Committed offsets are associated with a consumer group specific to the source.
The ID of the consumer group consists of the prefix configured with the [`GROUP
ID PREFIX` option](#connection-options) followed by a Materialize-generated
suffix.

You should not make assumptions about the number of consumer groups that
Materialize will use to consume from a given source. The only guarantee is that
the ID of each consumer group will begin with the configured prefix.

The consumer group ID prefix for each Kafka source in the system is available in
the `group_id_prefix` column of the [`mz_kafka_sources`] table. To look up the
`group_id_prefix` for a source by name, use:

```mzsql
SELECT group_id_prefix
FROM mz_internal.mz_kafka_sources ks
JOIN mz_sources s ON s.id = ks.id
WHERE s.name = '<src_name>'
```

## Examples

### Prerequisites

{{< include-md file="shared-content/kafka-source-prereq.md" >}}

### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_kafka_source"
 example="create-source" %}}

{{% include-example file="examples/create_source/example_kafka_source"
 example="create-table" %}}

### Create a source with offsets

{{% include-example file="examples/create_source/example_kafka_source"
 example="create-source-with-offsets" %}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
- [Using Debezium](/integrations/debezium/)

[Avro]: /sql/create-source/#avro
[JSON]: /sql/create-source/#json
[Protobuf]: /sql/create-source/#protobuf
[Text/bytes]: /sql/create-source/#textbytes
[CSV]: /sql/create-source/#csv

[Append-only envelope]: /sql/create-source/#append-only-envelope
[Upsert envelope]: /sql/create-source/#upsert-envelope
[Debezium envelope]: /sql/create-source/#debezium-envelope
[`mz_kafka_sources`]: /sql/system-catalog/mz_catalog/#mz_kafka_sources
