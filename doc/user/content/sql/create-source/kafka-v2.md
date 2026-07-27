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

With the new syntax, the `CREATE SOURCE` statement connects to the topic, and the
decoding options (`FORMAT`, `INCLUDE`, and `ENVELOPE`) move to the [`CREATE TABLE
... FROM SOURCE`](/sql/create-table/) statement.

Unlike other source types, `CREATE TABLE ... FROM SOURCE` does not need a
`(REFERENCE ...)` clause. A Kafka source exposes a single topic, and the table
reads from it automatically.

{{< note >}}
The `TEXT COLUMNS` and `EXCLUDE COLUMNS` options are not supported for Kafka
`CREATE TABLE ... FROM SOURCE`. A Kafka table's columns are determined entirely
by its format (for Avro, the reader schema). To exclude or recast a field,
project or cast it in a view on top of the table.
{{< /note >}}

## Syntax

{{< tabs >}}

{{< tab "Format Avro" >}}
### Format Avro

Materialize can decode Avro messages by integrating with a schema registry to
retrieve a schema, and automatically determine the columns and data types to use
in the table.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-avro" %}}

{{< include-md file="shared-content/kafka-format-avro-details.md" >}}

{{< /tab >}}

{{< tab "Format JSON" >}}
### Format JSON

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-json" %}}

{{< include-md file="shared-content/kafka-format-json-details.md" >}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}
### Format Text/Bytes

Materialize can:
- Parse **new-line delimited** data as plain text. Data is assumed to be **valid
  unicode** (UTF-8), and discarded if it cannot be converted to UTF-8.
  Text-formatted tables have a single column, by default named `text`. For details on casting, check the [`text`](/sql/types/text/) documentation.

- Read raw bytes without applying any formatting or decoding. Raw byte-formatted
tables have a single column, by default named `data`. For details on encodings
and casting, check the [`bytea`](/sql/types/bytea/) documentation.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}
### Format CSV

Materialize can parse CSV-formatted data. The data in CSV tables is read as
[`text`](/sql/types/text).

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-csv" %}}

{{< /tab >}}

{{< tab "Format Protobuf" >}}
### Format Protobuf

Materialize can decode Protobuf messages by integrating with a schema registry
or parsing an inline schema to retrieve a `.proto` schema definition. It can
then automatically define the columns and data types to use in the table.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-protobuf" %}}

{{< include-md file="shared-content/kafka-format-protobuf-details.md" >}}

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}
### KEY FORMAT VALUE FORMAT
By default, the message key is decoded using the same format as the message
value. However, you can set the key and value encodings explicitly using the
`KEY FORMAT ... VALUE FORMAT`.

{{% include-syntax file="examples/create_source_kafka_v2" example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

## Envelopes

{{< include-md file="shared-content/kafka-envelopes.md" >}}

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

## Features

### Exposing source metadata

{{< include-md file="shared-content/kafka-include-metadata.md" >}}

For spilling to disk, setting start offsets, and monitoring source progress and
consumer lag, see the [Features section of the Kafka/Redpanda reference
page](/sql/create-source/kafka/#features). These features are configured on the
`CREATE SOURCE` statement and behave the same regardless of syntax.

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

For connection setup, required Kafka ACLs, and worked examples for each format,
see the [Kafka/Redpanda reference page](/sql/create-source/kafka/).

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [CREATE SOURCE: Kafka/Redpanda (Legacy Syntax)](/sql/create-source/kafka/)
- [Handle upstream schema changes with zero downtime](/ingest-data/kafka/source-versioning/)
