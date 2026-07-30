---
title: "CREATE SOURCE: Kafka/Redpanda (New Syntax)"
description: "Connecting Materialize to a Kafka or Redpanda broker using the new source syntax"
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_kafka-v2
    name: Kafka/Redpanda (New Syntax)
    weight: 10
---

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/kafka/)" >}}

{{% create-source-intro external_source="Kafka or Redpanda broker" 
create_table="/sql/create-table/kafka" %}}

The decoding options (`FORMAT`, `INCLUDE`, and `ENVELOPE`) are set on the
[`CREATE TABLE ... FROM SOURCE`](/sql/create-table/kafka) statement that reads
from the source. For the full catalog of formats, envelopes, and exposed
metadata, see [CREATE TABLE: Kafka source table](/sql/create-table/kafka/).

{{< note >}}
The same syntax, supported formats and features can be used to connect to a
[Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

## Prerequisites

To create a source from Kafka/Redpanda broker, you first need to [create a
connection](/sql/create-connection/#kafka). Once created, a connection is
**reusable** across multiple `CREATE SOURCE` and `CREATE SINK` statements.

## Syntax

The `CREATE SOURCE` statement connects to a Kafka/Redpanda topic.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax" %}}

## Details

### Ingesting data

After the source is created, each [`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/kafka/) statement creates a table that decodes the
topic and starts ingesting data. You can create multiple tables from the same
source, each with its own format and envelope.

### Handling schema changes

Because each table pins its own reader schema when it is created, you can pick up
a [compatible upstream schema
change](https://avro.apache.org/docs/++version++/specification/#schema-resolution)
without downtime: create a new table that reads the evolved schema, recreate the
downstream objects, and swap them into place. See [Handle upstream schema changes
with zero downtime](/ingest-data/kafka/source-versioning/) for the full
procedure.

## Features

{{% include-headless "/headless/kafka-start-offsets.md" %}}

{{% include-headless "/headless/kafka-monitoring-source-progress.md" %}}

{{% include-headless "/headless/kafka-monitoring-consumer-lag.md" %}}

For spilling to disk, see the [Features section of the Kafka/Redpanda reference
page](/sql/create-source/kafka/#spilling-to-disk). This feature is configured on
the `CREATE SOURCE` statement and behaves the same regardless of syntax.

## Examples

### Prerequisite: Creating a connection

{{% include-headless "/headless/kafka-create-connection" %}}

### Create a source and table

```mzsql
CREATE SOURCE orders_src
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'orders');

CREATE TABLE orders
  FROM SOURCE orders_src
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

For connection setup, required Kafka ACLs, and worked examples for each format,
see the [Kafka/Redpanda reference page](/sql/create-source/kafka/).

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [CREATE SOURCE: Kafka/Redpanda (Legacy Syntax)](/sql/create-source/kafka/)
- [Handle upstream schema changes with zero downtime](/ingest-data/kafka/source-versioning/)
