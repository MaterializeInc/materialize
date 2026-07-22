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

{{< public-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/kafka/)" >}}

Materialize can read data from a Kafka or Redpanda broker. With the new syntax,
you first create a [source](/concepts/sources/) that connects to a topic, and
then use [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/) to decode the topic
and start ingesting data. Because each table pins its own reader schema, you can
pick up upstream schema changes without downtime. For a step-by-step
walkthrough, see [Handle upstream schema changes with zero
downtime](/ingest-data/kafka/source-versioning/).

To connect to a Kafka/Redpanda broker (and optionally a schema registry), you
first need to [create a
connection](/sql/create-source/kafka/#prerequisite-creating-a-connection) that
specifies access and authentication parameters. Once created, a connection is
**reusable** across multiple `CREATE SOURCE` and `CREATE SINK` statements.

{{< note >}}
The same syntax, supported formats and features can be used to connect to a
[Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

## Syntax

With the new syntax, the `CREATE SOURCE` statement connects to the topic, and the
decoding options (`FORMAT`, `INCLUDE`, and `ENVELOPE`) move to the [`CREATE TABLE
... FROM SOURCE`](/sql/create-table/) statement:

```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[IN CLUSTER <cluster_name>]
FROM KAFKA CONNECTION <connection_name> (
  TOPIC '<topic>'
  [, GROUP ID PREFIX '<group_id_prefix>']
  [, START OFFSET ( <partition_offset> [, ...] ) ]
  [, START TIMESTAMP <timestamp> ]
)
[EXPOSE PROGRESS AS <progress_subsource_name>];

CREATE TABLE [IF NOT EXISTS] <table_name>
FROM SOURCE <src_name>
[ FORMAT <format> | KEY FORMAT <format> VALUE FORMAT <format> ]
[INCLUDE
    KEY [AS <name>]
  | PARTITION [AS <name>]
  | OFFSET [AS <name>]
  | TIMESTAMP [AS <name>]
  | HEADERS [AS <name>]
  | HEADER '<key>' AS <name> [BYTES]
  [, ...]
]
[ENVELOPE
    NONE
  | DEBEZIUM
  | UPSERT [ ( VALUE DECODING ERRORS = INLINE [AS <name>] ) ]
];
```

Unlike other source types, `CREATE TABLE ... FROM SOURCE` does not need a
`(REFERENCE ...)` clause. A Kafka source exposes a single topic, and the table
reads from it automatically.

The available `FORMAT`, `INCLUDE`, and `ENVELOPE` options are the same as for the
[legacy syntax](/sql/create-source/kafka/#syntax). For the full catalog of
formats, envelopes, and exposed metadata, along with connection setup, required
Kafka ACLs, and source monitoring, see the [Kafka/Redpanda reference
page](/sql/create-source/kafka/).

{{< note >}}
The `TEXT COLUMNS` and `EXCLUDE COLUMNS` options are not supported for Kafka
`CREATE TABLE ... FROM SOURCE`. A Kafka table's columns are determined entirely
by its format (for Avro, the reader schema). To exclude or recast a field,
project or cast it in a view on top of the table.
{{< /note >}}

## Details

### Ingesting data

After the source is created, each [`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/) statement creates a table that decodes the topic and
starts ingesting data. You can create multiple tables from the same source, each
with its own format and envelope.

### Handling schema changes

Because each table pins its own reader schema when it is created, you can pick up
a [compatible upstream schema
change](https://avro.apache.org/docs/++version++/specification/#schema-resolution)
without downtime: create a new table that reads the evolved schema, recreate the
downstream objects, and swap them into place. See [Handle upstream schema changes
with zero downtime](/ingest-data/kafka/source-versioning/) for the full
procedure.

## Examples

### Create a source and table

```mzsql
CREATE SOURCE orders_src
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'orders');

CREATE TABLE orders
  FROM SOURCE orders_src
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

For connection setup and the full set of format, envelope, and metadata options,
see the [Kafka/Redpanda reference page](/sql/create-source/kafka/).

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [CREATE SOURCE: Kafka/Redpanda (Legacy Syntax)](/sql/create-source/kafka/)
- [Handle upstream schema changes with zero downtime](/ingest-data/kafka/source-versioning/)
