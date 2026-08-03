---
title: "CREATE SOURCE: Kafka/Redpanda (Legacy Syntax)"
description: "Connecting Materialize to a Kafka or Redpanda broker"
pagerank: 40
menu:
  main:
    parent: 'create-source-legacy'
    identifier: cs_kafka
    name: Kafka/Redpanda (Legacy Syntax)
    weight: 11
aliases:
    - /sql/create-source/avro-kafka
    - /sql/create-source/json-kafka
    - /sql/create-source/protobuf-kafka
    - /sql/create-source/text-kafka
    - /sql/create-source/csv-kafka
---

{{< source-versioning-disambiguation is_new=false
other_ref="[new reference page](/sql/create-source/kafka-v2/)" >}}

{{% create-source/intro %}}

To connect to a Kafka/Redpanda broker (and optionally a schema registry), you
first need to [create a connection](#prerequisite-creating-a-connection) that specifies
access and authentication parameters. Once created, a connection is **reusable**
across multiple `CREATE SOURCE` and `CREATE SINK` statements. {{%
/create-source/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a
[Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

## Syntax

{{< tabs >}}

{{< tab "Format Avro" >}}
### Format Avro

Materialize can decode Avro messages by integrating with a schema registry to
retrieve a schema, and automatically determine the columns and data types to use
in the source.

{{% include-syntax file="examples/create_source_kafka" example="syntax-avro" %}}

{{% include-headless "/headless/kafka-format-avro-details" %}}

{{< /tab >}}

{{< tab "Format JSON" >}}
### Format JSON

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

{{% include-syntax file="examples/create_source_kafka" example="syntax-json" %}}

{{% include-headless "/headless/kafka-format-json-details" %}}

{{< /tab >}}

{{< tab "Format TEXT/BYTES" >}}
### Format Text/Bytes

Materialize can:
- Parse **new-line delimited** data as plain text. Data is assumed to be **valid
  unicode** (UTF-8), and discarded if it cannot be converted to UTF-8.
  Text-formatted sources have a single column, by default named `text`. For details on casting, check the [`text`](/sql/types/text/) documentation.

- Read raw bytes without applying any formatting or decoding. Raw byte-formatted
sources have a single column, by default named `data`. For details on encodings
and casting, check the [`bytea`](/sql/types/bytea/) documentation.


{{% include-syntax file="examples/create_source_kafka" example="syntax-text-bytes" %}}

{{< /tab >}}

{{< tab "Format CSV" >}}
### Format CSV

Materialize can parse CSV-formatted data. The data in CSV sources is read as
[`text`](/sql/types/text).

{{% include-syntax file="examples/create_source_kafka" example="syntax-csv" %}}

{{< /tab >}}

{{< tab "Format Protobuf" >}}
### Format Protobuf

Materialize can decode Protobuf messages by integrating with a schema registry
or parsing an inline schema to retrieve a `.proto` schema definition. It can
then automatically define the columns and data types to use in the source.

{{% include-syntax file="examples/create_source_kafka" example="syntax-protobuf" %}}

{{% include-headless "/headless/kafka-format-protobuf-details" %}}

{{< /tab >}}

{{< tab "KEY FORMAT VALUE FORMAT" >}}
### KEY FORMAT VALUE FORMAT
By default, the message key is decoded using the same format as the message
value. However, you can set the key and value encodings explicitly using the
`KEY FORMAT ... VALUE FORMAT`.

{{% include-syntax file="examples/create_source_kafka" example="syntax-key-value-format" %}}

{{< /tab >}}

{{< /tabs >}}

## Envelopes

{{% include-headless "/headless/kafka-envelopes" %}}

## Features



### Spilling to disk

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require storing
the current value for _each key_ in the source to produce retractions when keys
are updated. When using [standard cluster sizes](/sql/create-cluster/#available-sizes),
Materialize will automatically offload this state to disk, seamlessly handling
key spaces that are larger than memory.

Spilling to disk is not available with [legacy cluster sizes](/sql/create-cluster/#legacy-sizes).

### Exposing source metadata

{{% include-headless "/headless/kafka-include-metadata" %}}

{{% include-headless "/headless/kafka-start-offsets" %}}

{{% include-headless "/headless/kafka-monitoring-source-progress" %}}

{{% include-headless "/headless/kafka-monitoring-consumer-lag" %}}

## Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option
Read           | Group            | All group IDs starting with the specified [`GROUP ID PREFIX` option](#syntax)

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-source" %}}

## Examples

### Prerequisite: Creating a connection

{{% include-headless "/headless/kafka-create-connection" %}}

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

**Using Confluent Schema Registry**

```mzsql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

**Using AWS Glue Schema Registry** {{< private-preview-inline />}}

```mzsql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_connection (
    SCHEMA NAME = 'test_schema'
  );
```

{{< /tab >}}
{{< tab "JSON">}}

```mzsql
CREATE SOURCE json_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT JSON;
```

```mzsql
CREATE VIEW typed_kafka_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM json_source;
```

JSON-formatted messages are ingested as a JSON blob. We recommend creating a
parsing view on top of your Kafka source that maps the individual fields to
columns with the required data types. To avoid doing this tedious task
manually, you can use [this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

{{< /tab >}}
{{< tab "Text/bytes">}}

```mzsql
CREATE SOURCE text_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT TEXT
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< tab "CSV">}}

```mzsql
CREATE SOURCE csv_source (col_foo, col_bar, col_baz)
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT CSV WITH 3 COLUMNS;
```

{{< /tab >}}
{{< tab "Protobuf">}}

**Using Confluent Schema Registry**

```mzsql
CREATE SOURCE proto_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT PROTOBUF USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

**Using an inline schema**

If you're not using a schema registry, you can use the `MESSAGE...SCHEMA` clause
to specify a Protobuf schema descriptor inline. Protobuf does not serialize a
schema with the message, so before creating a source you must:

* Compile the Protobuf schema into a descriptor file using [`protoc`](https://grpc.io/docs/protoc-installation/):

  ```proto
  // example.proto
  syntax = "proto3";
  message Batch {
      int32 id = 1;
      // ...
  }
  ```

  ```bash
  protoc --include_imports --descriptor_set_out=example.pb example.proto
  ```

* Encode the descriptor file into a SQL byte string:

  ```bash
  $ printf '\\x' && xxd -p example.pb | tr -d '\n'
  \x0a300a0d62696...
  ```

* Create the source using the encoded descriptor bytes from the previous step
  (including the `\x` at the beginning):

  ```mzsql
  CREATE SOURCE proto_source
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
    FORMAT PROTOBUF MESSAGE 'Batch' USING SCHEMA '\x0a300a0d62696...';
  ```

{{< /tab >}}
{{< /tabs >}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)
- [Using Debezium](/integrations/debezium/)
