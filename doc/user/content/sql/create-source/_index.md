---
title: "CREATE SOURCE"
description: "`CREATE SOURCE` connects Materialize to an external data source."
disable_list: true
pagerank: 30
menu:
  main:
    parent: reference
    name: Sources
    identifier: 'create-source'
    weight: 20
---

A [source](../../overview/key-concepts/#sources) describes an external system you want Materialize to read data from, and provides details about how to decode and interpret that data. To create a source, you must specify a [connector](#connectors), a [format](#formats) and an [envelope](#envelopes).

[//]: # "TODO(morsapaes) Add short description about what the command gets going in the background."

## Connectors

Materialize bundles **native connectors** for the following external systems:

{{< multilinkbox >}}
{{< linkbox title="Message Brokers" >}}
- [Kafka](/sql/create-source/kafka)
- [Redpanda](/sql/create-source/kafka)
{{</ linkbox >}}
{{< linkbox title="Databases (CDC)" >}}
- [PostgreSQL](/sql/create-source/postgres)
{{</ linkbox >}}
{{< linkbox title="Datagen" >}}
- [Load generator](/sql/create-source/load-generator)
{{</ linkbox >}}
{{</ multilinkbox >}}

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

## Formats

To read from an external data source, Materialize must be able to determine how to decode raw bytes from different formats into data structures it can understand at runtime. This is handled by specifying a `FORMAT` in the `CREATE SOURCE` statement.

### Avro

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT AVRO</code></p>

Materialize can decode Avro messages by integrating with a schema registry to retrieve a schema, and automatically determine the columns and data types to use in the source.

##### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued. In the future, we expect to support specifying a different subject name strategy {{% gh 6170 %}}.

##### Schema evolution

As long as the writer schema changes in a [compatible way](https://avro.apache.org/docs/current/spec.html#Schema+Resolution), Materialize will continue using the original reader schema definition by mapping values from the new to the old schema version. To use the new version of the writer schema in Materialize, you need to **drop and recreate** the source.

##### Name collision

To avoid [case-sensitivity](/sql/identifiers/#case-sensitivity) conflicts with Materialize identifiers, we recommend double-quoting all field names when working with Avro-formatted sources.

##### Supported types

Materialize supports all [Avro types](https://avro.apache.org/docs/current/spec.html), _except for_ recursive types {{% gh 5803 %}} and union types in arrays {{% gh 8917 %}}.

### JSON

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT BYTES</code></p>

{{< note >}}
Support for the more ergonomic `FORMAT JSON` is in progress {{% gh 7186 %}}!
{{</ note >}}

Materialize cannot decode JSON directly from an external data source. Instead, you must create a source that reads the data as [raw bytes](#bytes), and handle the conversion to primitive types using [`jsonb`](/sql/types/jsonb) as an intermediate representation.

```sql
-- create raw byte array source
CREATE SOURCE my_bytea_source
  FROM ...
  FORMAT BYTES
  WITH (SIZE='3xsmall');

-- convert from byte array to jsonb
CREATE VIEW my_jsonb_source AS
  SELECT
    CONVERT_FROM(data, 'utf8')::jsonb AS data
  FROM bytea_source;
```

{{< note >}}
Raw byte-formatted sources have one column, by default named `data`. For more details on handling JSON-encoded messages, check the [`jsonb` type](/sql/types/jsonb) documentation.
{{</ note >}}

To avoid redundant processing and ensure a typed representation of the source is available across clusters, you should create a [materialized view](/overview/key-concepts/#materialized-views).

```sql
-- parse jsonb into typed columns
CREATE MATERIALIZED VIEW my_typed_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM my_jsonb_source;
```

##### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for JSON-formatted sources {{% gh 7186 %}}. This means that Materialize cannot decode messages serialized using the [JSON Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer) serialization format (`JSON_SR`).

### Protobuf

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT PROTOBUF</code></p>

Materialize can decode Protobuf messages by integrating with a schema registry to retrieve a `.proto` schema definition, and automatically define the columns and data types to use in the source. Unlike Avro, Protobuf does not serialize a schema with the message, so Materialize expects:

* A `FileDescriptorSet` that encodes the Protobuf message schema. You can generate the `FileDescriptorSet` with `protoc`, for example:

  ```shell
  protoc --include_imports --descriptor_set_out=SCHEMA billing.proto
  ```

* A top-level message name and its package name, so Materialize knows which message from the `FileDescriptorSet` is the top-level message to decode, in the following format:

  ```shell
  <package name>.<top-level message>
  ```

  For example, if the `FileDescriptorSet` were from a `.proto` file in the
    `billing` package, and the top-level message was called `Batch`, the
    _message&lowbar;name_ value would be `billing.Batch`.

##### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued. In the future, we expect to support specifying a different subject name strategy {{% gh 6170 %}}.

##### Schema evolution

As long as the `.proto` schema definition changes in a [compatible way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs), Materialize will continue using the original schema definition by mapping values from the new to the old schema version. To use the new version of the schema in Materialize, you need to **drop and recreate** the source.

##### Supported types

Materialize supports all [well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf) Protobuf types from the `proto2` and `proto3` specs, _except for_ recursive `Struct` values {{% gh 5803 %}} and map types.

##### Multiple message schemas

When using a schema registry with Protobuf sources, the registered schemas must contain exactly one `Message` definition. In the future, we expect to support schemas with multiple messages {{% gh 9598 %}}.

### Text/bytes

#### Text

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT TEXT</code></p>

Materialize can parse **new-line delimited** data as plain text. Data is assumed to be **valid unicode** (UTF-8), and discarded if it cannot be converted to UTF-8. Text-formatted sources have a single column, by default named `text`.

For details on casting, check the [`text`](/sql/types/text/) documentation.

#### Bytes

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT BYTES</code></p>

Materialize can read raw bytes without applying any formatting or decoding. Raw byte-formatted sources have a single column, by default named `data`.

For details on encodings and casting, check the [`bytea`](/sql/types/bytea/) documentation.

### CSV

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT CSV</code></p>

Materialize can parse CSV-formatted data using different methods to determine the number of columns to create and their respective names:

Method                 | Description
-----------------------|-----------------------
**HEADER**             | Materialize determines the _number of columns_ and the _name_ of each column using the header row. The header is **not** ingested as data.
**HEADER (** _name_list_ **)** | Same behavior as **HEADER**, with additional validation of the column names against the _name list_ specified. This allows decoding files that have headers but may not be populated yet, as well as overriding the source column names.
_n_ **COLUMNS**        | Materialize treats the source data as if it has _n_ columns. By default, columns are named `column1`, `column2`...`columnN`.

The data in CSV sources is read as [`text`](/sql/types/text). You can then handle the conversion to other types using explicit [casts](/sql/functions/cast/) when creating views.

##### Invalid rows

Any row that doesn't match the number of columns determined by the format is ignored, and Materialize logs an error.

## Envelopes

In addition to determining how to decode incoming records, Materialize also needs to understand how to interpret them. Whether a new record inserts, updates, or deletes existing data in Materialize depends on the `ENVELOPE` specified in the `CREATE SOURCE` statement.

### Append-only envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE NONE</code></p>

The append-only envelope treats all records as inserts. This is the **default** envelope, if no envelope is specified.

### Upsert envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE UPSERT</code></p>

The upsert envelope treats all records as having a **key** and a **value**, and supports inserts, updates and deletes within Materialize:

- If the key does not match a preexisting record, it inserts the record's key and value.

- If the key matches a preexisting record and the value is _non-null_, Materialize updates
  the existing record with the new value.

- If the key matches a preexisting record and the value is _null_, Materialize deletes the record.

### Debezium envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE DEBEZIUM</code></p>

Materialize provides a dedicated envelope to decode messages produced by [Debezium](https://debezium.io/). This envelope treats all records as [change events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events) with a diff structure that indicates whether each record should be interpreted as an insert, update or delete within Materialize:

- If the `before` field is _null_, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events) and Materialize inserts the record's key and value.

- If the `before` and `after` fields are _non-null_, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events) and Materialize updates the existing record with the new value.

- If the `after` field is _null_, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events) and Materialize deletes the record.

Materialize expects a specific message structure that includes the row data before and after the change event, which is **not guaranteed** for every Debezium connector. For more details, check the [Debezium integration guide](/integrations/debezium/).

[//]: # "TODO(morsapaes) Once DBZ transaction support is stable, add a dedicated sub-section here and adapt the respective snippet in both CDC guides."

##### Truncation

The Debezium envelope does not support upstream [`truncate` events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events) {{% gh 6596 %}}.

##### Debezium metadata

The envelope exposes the `before` and `after` value fields from change events. In the future, we expect to support additional metadata with information about the original context of the events, like `source.ts_ms`, `source.database` and `source.table` {{% gh 12077 %}}.

##### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted. Materialize makes a best-effort attempt to detect and filter out duplicates.

## Best practices

### Sizing a source

Some sources are low traffic and require relatively few resources to handle data ingestion, while others are high traffic and require hefty resource allocations. You choose the amount of CPU and memory available to a source using the `SIZE` option, and adjust the provisioned size after source creation using the [`ALTER SOURCE`](/sql/alter-source) command.

It's a good idea to size up a source when:

  * You want to **increase throughput**. Larger sources will typically ingest data
    faster, as there is more CPU available to read and decode data from the
    upstream external system.

  * You are using the [upsert envelope](#upsert-envelope) or [Debezium
    envelope](#debezium-envelope), and your source contains **many unique
    keys**. These envelopes must keep in-memory state proportional to the number
    of unique keys in the upstream external system. Larger sizes can store more
    unique keys.

Sources that specify the `SIZE` option are linked to a single-purpose cluster
dedicated to maintaining that source.

You can also choose to place a source in an existing
[cluster](/overview/key-concepts/#clusters) by using the `IN CLUSTER` option.
Sources in a cluster share the resource allocation of the cluster with all other
objects in the cluster.

Colocating multiple sources onto the same cluster can be more resource efficient
when you have many low-traffic sources that occasionally need some burst
capacity.

## Related pages

- [Key Concepts](../../overview/key-concepts/)
- [`SHOW SOURCES`](/sql/show-sources/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SOURCE`](/sql/show-create-source/)
