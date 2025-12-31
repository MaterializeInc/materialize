# CREATE SOURCE

`CREATE SOURCE` connects Materialize to an external data source.



A [source](/concepts/sources/) describes an external system you want Materialize to read data from, and provides details about how to decode and interpret that data. To create a source, you must specify a [connector](#connectors), a [format](#formats) and an [envelope](#envelopes).
Like other relations, sources are [namespaced](../namespaces/) by a database and schema.

[//]: # "TODO(morsapaes) Add short description about what the command gets going in the background."

## Connectors

Materialize bundles **native connectors** that allow ingesting data from the following external systems:

{{< include-md file="shared-content/multilink-box-native-connectors.md" >}}

For details on the syntax, supported formats and features of each connector, check out the dedicated `CREATE SOURCE` documentation pages.

**Sample data**

To get started with no external dependencies, you can use the [load generator source](/sql/create-source/load-generator/)
to produce sample data that is suitable for demo and performance test
scenarios.

## Formats

To read from an external data source, Materialize must be able to determine how to decode raw bytes from different formats into data structures it can understand at runtime. This is handled by specifying a `FORMAT` in the `CREATE SOURCE` statement.

### Avro

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT AVRO</code></p>

Materialize can decode Avro messages by integrating with a schema registry to retrieve a schema, and automatically determine the columns and data types to use in the source.

##### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

##### Schema evolution

As long as the writer schema changes in a [compatible way](https://avro.apache.org/docs/++version++/specification/#schema-resolution), Materialize will continue using the original reader schema definition by mapping values from the new to the old schema version. To use the new version of the writer schema in Materialize, you need to **drop and recreate** the source.

##### Name collision

To avoid [case-sensitivity](/sql/identifiers/#case-sensitivity) conflicts with Materialize identifiers, we recommend double-quoting all field names when working with Avro-formatted sources.

##### Supported types

Materialize supports all [Avro types](https://avro.apache.org/docs/++version++/specification/), _except for_ recursive types and union types in arrays.

### JSON

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT JSON</code></p>

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

If your JSON messages have a consistent shape, we recommend creating a parsing
[view](/concepts/views) that maps the individual fields to
columns with the required data types:

```mzsql
-- extract jsonb into typed columns
CREATE VIEW my_typed_source AS
  SELECT
    (data->>'field1')::boolean AS field_1,
    (data->>'field2')::int AS field_2,
    (data->>'field3')::float AS field_3
  FROM my_jsonb_source;
```

To avoid doing this tedious task manually, you can use [this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

##### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for JSON-formatted sources. This means that Materialize cannot decode messages serialized using the [JSON Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer) serialization format (`JSON_SR`).

### Protobuf

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT PROTOBUF</code></p>

Materialize can decode Protobuf messages by integrating with a schema registry or parsing an inline schema to retrieve a `.proto` schema definition. It can then automatically define the columns and data types to use in the source. Unlike Avro, Protobuf does not serialize a schema with the message, so Materialize expects:

* A `FileDescriptorSet` that encodes the Protobuf message schema. You can generate the `FileDescriptorSet` with [`protoc`](https://grpc.io/docs/protoc-installation/), for example:

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

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

##### Schema evolution

As long as the `.proto` schema definition changes in a [compatible way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs), Materialize will continue using the original schema definition by mapping values from the new to the old schema version. To use the new version of the schema in Materialize, you need to **drop and recreate** the source.

##### Supported types

Materialize supports all [well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf) Protobuf types from the `proto2` and `proto3` specs, _except for_ recursive `Struct` values and map types.

##### Multiple message schemas

When using a schema registry with Protobuf sources, the registered schemas must contain exactly one `Message` definition.

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

The Debezium envelope does not support upstream [`truncate` events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

##### Debezium metadata

The envelope exposes the `before` and `after` value fields from change events.

##### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted. Materialize makes a best-effort attempt to detect and filter out duplicates.

## Best practices

### Sizing a source

Some sources are low traffic and require relatively few resources to handle data ingestion, while others are high traffic and require hefty resource allocations. The cluster in which you place a source determines the amount of CPU, memory, and disk available to the source.

It's a good idea to size up the cluster hosting a source when:

  * You want to **increase throughput**. Larger sources will typically ingest data
    faster, as there is more CPU available to read and decode data from the
    upstream external system.

  * You are using the [upsert envelope](#upsert-envelope) or [Debezium
    envelope](#debezium-envelope), and your source contains **many unique
    keys**. These envelopes maintain state proportional to the number of unique
    keys in the upstream external system. Larger sizes can store more unique
    keys.

Sources share the resource allocation of their cluster with all other objects in
the cluster. Colocating multiple sources onto the same cluster can be more
resource efficient when you have many low-traffic sources that occasionally need
some burst capacity.

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster if the source is created in an existing cluster.
- `CREATECLUSTER` privileges on the system if the source is not created in an existing cluster.
- `USAGE` privileges on all connections and secrets used in the source definition.
- `USAGE` privileges on the schemas that all connections and secrets in the statement are contained in.

## Related pages

- [Sources](/concepts/sources/)
- [`SHOW SOURCES`](/sql/show-sources/)
- [`SHOW COLUMNS`](/sql/show-columns/)
- [`SHOW CREATE SOURCE`](/sql/show-create-source/)




---

## CREATE SOURCE: Kafka/Redpanda


{{% create-source/intro %}}
To connect to a Kafka broker (and optionally a schema registry), you first need
to [create a connection](#creating-a-connection) that specifies access and
authentication parameters. Once created, a connection is **reusable** across
multiple `CREATE SOURCE` and `CREATE SINK` statements.
{{% /create-source/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a
[Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

## Syntax

{{< diagram "create-source-kafka.svg" >}}

#### `format_spec`

{{< diagram "format-spec.svg" >}}

#### `key_strat`

{{< diagram "key-strat.svg" >}}

#### `val_strat`

{{< diagram "val-strat.svg" >}}

#### `strat`

{{< diagram "strat.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

{{% create-source/syntax-connector-details connector="kafka" envelopes="debezium upsert append-only" %}}

### `CONNECTION` options

Field                                         | Value     | Description
----------------------------------------------|-----------|-------------------------------------
**TOPIC**                                     | `text`    | The Kafka topic you want to subscribe to.
**GROUP ID PREFIX**                           | `text`    | The prefix of the consumer group ID to use. See [Monitoring consumer lag](#monitoring-consumer-lag).<br>Default: `materialize-{REGION-ID}-{CONNECTION-ID}-{SOURCE_ID}`
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Supported formats

|<div style="width:290px">Format</div> | [Append-only envelope] | [Upsert envelope] | [Debezium envelope] |
---------------------------------------|:----------------------:|:-----------------:|:-------------------:|
| [Avro]                               | ✓                      | ✓                 | ✓                   |
| [JSON]                               | ✓                      | ✓                 |                     |
| [Protobuf]                           | ✓                      | ✓                 |                     |
| [Text/bytes]                         | ✓                      | ✓                 |                     |
| [CSV]                                | ✓                      |                   |                     |

### Key-value encoding

By default, the message key is decoded using the same format as the message
value. However, you can set the key and value encodings explicitly using the
`KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

## Features

### Handling upserts

To create a source that uses the standard key-value convention to support
inserts, updates, and deletes within Materialize, you can use `ENVELOPE
UPSERT`:

```mzsql
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

Note that:

- Using this envelope is required to consume [log compacted topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

- This envelope can lead to high memory and disk utilization in the cluster
  maintaining the source. We recommend using a standard-sized cluster, rather
  than a legacy-sized cluster, to automatically spill the workload to disk. See
  [spilling to disk](#spilling-to-disk) for details.

#### Null keys

If a message with a `NULL` key is detected, Materialize sets the source into an
error state. To recover an errored source, you must produce a record with a
`NULL` value and a `NULL` key to the topic, to force a retraction.

As an example, you can use [`kcat`](https://docs.confluent.io/platform/current/clients/kafkacat-usage.html)
to produce an empty message:

```bash
echo ":" | kcat -b $BROKER -t $TOPIC -Z -K: \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-256 \
  -X sasl.username=$KAFKA_USERNAME \
  -X sasl.password=$KAFKA_PASSWORD
```

#### Value decoding errors

By default, if an error happens while decoding the value of a message for a
specific key, Materialize sets the source into an error state. You can
configure the source to continue ingesting data in the presence of value
decoding errors using the `VALUE DECODING ERRORS = INLINE` option:

```mzsql
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  KEY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT (VALUE DECODING ERRORS = INLINE);
```

When this option is specified the source will include an additional column named
`error` with type `record(description: text)`.

This column and all value columns will be nullable, such that if the most recent value
for the given Kafka message key cannot be decoded, this `error` column will contain
the error message. If the most recent value for a key has been successfully decoded,
this column will be `NULL`.

To use an alternative name for the error column, use `INLINE AS ..` to specify the
column name to use:

```mzsql
ENVELOPE UPSERT (VALUE DECODING ERRORS = (INLINE AS my_error_col))
```

It might be convenient to implement a parsing view on top of your Kafka upsert source that
excludes keys with decoding errors:

```mzsql
CREATE VIEW kafka_upsert_parsed
SELECT *
FROM kafka_upsert
WHERE error IS NULL;
```

### Using Debezium

{{< debezium-json >}}

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to decode Kafka
messages produced by [Debezium](https://debezium.io/). To create a source that
interprets Debezium messages:

```mzsql
CREATE SOURCE kafka_repl
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```

Any materialized view defined on top of this source will be incrementally
updated as new change events stream in through Kafka, as a result of `INSERT`,
`UPDATE` and `DELETE` operations in the original database.

For more details and a step-by-step guide on using Kafka+Debezium for Change
Data Capture (CDC), check [Using Debezium](/integrations/debezium/).

Note that:

- This envelope can lead to high memory utilization in the cluster maintaining
  the source. Materialize can automatically offload processing to
  disk as needed. See [spilling to disk](#spilling-to-disk) for details.

### Spilling to disk

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require storing
the current value for _each key_ in the source to produce retractions when keys
are updated. When using [standard cluster sizes](/sql/create-cluster/#size),
Materialize will automatically offload this state to disk, seamlessly handling
key spaces that are larger than memory.

Spilling to disk is not available with [legacy cluster sizes](/sql/create-cluster/#legacy-sizes).

### Exposing source metadata

In addition to the message value, Materialize can expose the message key,
headers and other source metadata fields to SQL.

#### Key

The message key is exposed via the `INCLUDE KEY` option. Composite keys are also
supported.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id;
```

Note that:

- This option requires specifying the key and value encodings explicitly using the `KEY FORMAT ... VALUE FORMAT` [syntax](#syntax).

- The `UPSERT` envelope always includes keys.

- The `DEBEZIUM` envelope is incompatible with this option.

#### Headers

Message headers can be retained in Materialize and exposed as part of the source data.

Note that:
- The `DEBEZIUM` envelope is incompatible with this option.

**All headers**

All of a message's headers can be exposed using `INCLUDE HEADERS`, followed by
an `AS <header_col>`.

This introduces column with the name specified or `headers` if none was
specified. The column has the type `record(key: text, value: bytea?) list`,
i.e. a list of records containing key-value pairs, where the keys are `text`
and the values are nullable `bytea`s.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADERS
  ENVELOPE NONE;
```

To simplify turning the headers column into a `map` (so individual headers can
be searched), you can use the [`map_build`](/sql/functions/#map_build) function:

```mzsql
SELECT
    id,
    seller,
    item,
    convert_from(map_build(headers)->'client_id', 'utf-8') AS client_id,
    map_build(headers)->'encryption_key' AS encryption_key,
FROM kafka_metadata;
```

<p></p>

```nofmt
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

**Individual headers**

Individual message headers can be exposed via the `INCLUDE HEADER key AS name`
option.

The `bytea` value of the header is automatically parsed into an UTF-8 string. To
expose the raw `bytea` instead, the `BYTES` option can be used.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE HEADER 'c_id' AS client_id, HEADER 'key' AS encryption_key BYTES,
  ENVELOPE NONE;
```

Headers can be queried as any other column in the source:

```mzsql
SELECT
    id,
    seller,
    item,
    client_id::numeric,
    encryption_key
FROM kafka_metadata;
```

<p></p>

```nofmt
 id | seller |        item        | client_id |    encryption_key
----+--------+--------------------+-----------+----------------------
  2 |   1592 | Custom Art         |        23 | \x796f75207769736821
  3 |   1411 | City Bar Crawl     |        42 | \x796f75207769736821
```

Note that:

- Messages that do not contain all header keys as specified in the source DDL
  will cause an error that prevents further querying the source.

- Header values containing badly formed UTF-8 strings will cause an error in the
  source that prevents querying it, unless the `BYTES` option is specified.

#### Partition, offset, timestamp

These metadata fields are exposed via the `INCLUDE PARTITION`, `INCLUDE OFFSET`
and `INCLUDE TIMESTAMP` options.

```mzsql
CREATE SOURCE kafka_metadata
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'data')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  INCLUDE PARTITION, OFFSET, TIMESTAMP AS ts
  ENVELOPE NONE;
```

```mzsql
SELECT "offset" FROM kafka_metadata WHERE ts > '2021-01-01';
```

<p></p>

```nofmt
offset
------
15
14
13
```

### Setting start offsets

To start consuming a Kafka stream from a specific offset, you can use the `START
OFFSET` option.

```mzsql
CREATE SOURCE kafka_offset
  FROM KAFKA CONNECTION kafka_connection (
    TOPIC 'data',
    -- Start reading from the earliest offset in the first partition,
    -- the second partition at 10, and the third partition at 100.
    START OFFSET (0, 10, 100)
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```

Note that:

- If fewer offsets than partitions are provided, the remaining partitions will
  start at offset 0. This is true if you provide `START OFFSET (1)` or `START
  OFFSET (1, ...)`.

- Providing more offsets than partitions is not supported.

#### Time-based offsets

It's also possible to set a start offset based on Kafka timestamps, using the
`START TIMESTAMP` option. This approach sets the start offset for each
available partition based on the Kafka timestamp and the source behaves as if
`START OFFSET` was provided directly.

It's important to note that `START TIMESTAMP` is a property of the source: it
will be calculated _once_ at the time the `CREATE SOURCE` statement is issued.
This means that the computed start offsets will be the **same** for all views
depending on the source and **stable** across restarts.

If you need to limit the amount of data maintained as state after source
creation, consider using [temporal filters](/sql/patterns/temporal-filters/)
instead.

#### `CONNECTION` options

Field               | Value | Description
--------------------|-------|--------------------
`START OFFSET`      | `int` | Read partitions from the specified offset. You cannot update the offsets once a source has been created; you will need to recreate the source. Offset values must be zero or positive integers.
`START TIMESTAMP`   | `int` | Use the specified value to set `START OFFSET` based on the Kafka timestamp. Negative values will be interpreted as relative to the current system time in milliseconds (e.g. `-1000` means 1000 ms ago). The offset for each partition will be the earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition. If no such offset exists for a partition, the partition's end offset will be used.

#### `KEY STRATEGY` and `VALUE STRATEGY`

It is possible to define how an Avro reader schema will be chosen for Avro sources by
using the `KEY STRATEGY` and `VALUE STRATEGY` keywords, as shown in the syntax diagram.

A strategy of `LATEST` (the default) will choose the latest writer schema from
the schema registry to use as a reader schema. `ID` or `INLINE` will allow
specifying a schema from the registry by ID or inline in the `CREATE SOURCE`
statement, respectively.

### Monitoring source progress

By default, Kafka sources expose progress metadata as a subsource that you can
use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`partition`    | `numrange`                               | The upstream Kafka partition.
`offset`       | [`uint8`](/sql/types/uint/#uint8-info)   | The greatest offset consumed from each upstream Kafka partition.

And can be queried using:

```mzsql
SELECT
  partition, "offset"
FROM
  (
    SELECT
      -- Take the upper of the range, which is null for non-partition rows
      -- Cast partition to u64, which is more ergonomic
      upper(partition)::uint8 AS partition, "offset"
    FROM
      <src_name>_progress
  )
WHERE
  -- Remove all non-partition rows
  partition IS NOT NULL;
```

As long as any offset continues increasing, Materialize is consuming data from
the upstream Kafka broker. For more details on monitoring source ingestion
progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

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

## Required permissions

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option
Read           | Group            | All group IDs starting with the specified [`GROUP ID PREFIX` option](#connection-options)

## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection) documentation page.

#### Broker

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```mzsql
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```
{{< /tab >}}
{{< tab "SASL">}}

```mzsql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```
{{< /tab >}}
{{< /tabs >}}

If your Kafka broker is not exposed to the public internet, you can [tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
    BROKERS (
        'broker1:9092' USING AWS PRIVATELINK privatelink_svc,
        'broker2:9092' USING AWS PRIVATELINK privatelink_svc (PORT 9093)
    )
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}

```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```mzsql
CREATE CONNECTION kafka_connection TO KAFKA (
BROKERS (
    'broker1:9092' USING SSH TUNNEL ssh_connection,
    'broker2:9092' USING SSH TUNNEL ssh_connection
    )
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

#### Confluent Schema Registry

{{< tabs tabID="1" >}}
{{< tab "SSL">}}
```mzsql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://unique-jellyfish-0000.us-east-1.aws.confluent.cloud:9093',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```
{{< /tab >}}
{{< tab "Basic HTTP Authentication">}}
```mzsql
CREATE SECRET IF NOT EXISTS csr_username AS '<CSR_USERNAME>';
CREATE SECRET IF NOT EXISTS csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password
);
```
{{< /tab >}}
{{< /tabs >}}

If your Confluent Schema Registry server is not exposed to the public internet,
you can [tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host:

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).
{{< /tab >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST '<SSH_BASTION_HOST>',
    USER '<SSH_BASTION_USER>',
    PORT <SSH_BASTION_PORT>
);
```

```mzsql
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://my-confluent-schema-registry:8081',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check [this guide](/ops/network-security/ssh-tunnel/).
{{< /tab >}}
{{< /tabs >}}

### Creating a source

{{< tabs tabID="1" >}}
{{< tab "Avro">}}

**Using Confluent Schema Registry**

```mzsql
CREATE SOURCE avro_source
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'test_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
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

  For more details about Protobuf message names and descriptors, check the
  [Protobuf format](../#protobuf) documentation.

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
{{< /tabs >}}

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




---

## CREATE SOURCE: Load generator


{{% create-source/intro %}}
Load generator sources produce synthetic data for use in demos and performance
tests.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-load-generator.svg" >}}

#### `load_generator_option`

{{< diagram "load-generator-option.svg" >}}

#### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**AUCTION**  | Use the [auction](#auction) load generator.
**MARKETING**| Use the [marketing](#marketing) load generator.
**TPCH**     | Use the [tpch](#tpch) load generator.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists.
**TICK INTERVAL**  | The interval at which the next datum should be emitted. Defaults to one second.
**AS OF**  | The tick at which to start producing data. Defaults to 0. {{< warn-if-unreleased-inline "v0.101" >}}
**UP TO**  | The tick before which to stop producing data. Defaults to infinite. {{< warn-if-unreleased-inline "v0.101" >}}
**SCALE FACTOR**   | The scale factor for the `TPCH` generator. Defaults to `0.01` (~ 10MB).
**FOR ALL TABLES** | Creates subsources for all tables in the load generator.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress subsource for the source. If this is not specified, the subsource will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Description

Materialize has several built-in load generators, which provide a quick way to
get up and running with no external dependencies before plugging in your own
data sources. If you would like to see an additional load generator, please
submit a [feature request].

### Auction

The auction load generator simulates an auction house, where users are bidding
on an ongoing series of auctions. The auction source will be automatically demuxed
into multiple subsources when the `CREATE SOURCE` command is executed. This will
create the following subsources:

  * `organizations` describes the organizations known to the auction
    house.

    Field | Type       | Description
    ------|------------|------------
    id    | [`bigint`] | A unique identifier for the organization.
    name  | [`text`]   | The organization's name.

  * `users` describes the users that belong to each organization.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the user.
    `org_id`  | [`bigint`] | The identifier of the organization to which the user belongs. References `organizations.id`.
    `name`    | [`text`]   | The user's name.

  * `accounts` describes the account associated with each organization.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the account.
    `org_id`  | [`bigint`] | The identifier of the organization to which the account belongs. References `organizations.id`.
    `balance` | [`bigint`] | The balance of the account in dollars.

  * `auctions` describes all past and ongoing auctions.

    Field      | Type                         | Description
    -----------|------------------------------|------------
    `id`       | [`bigint`]                   | A unique identifier for the auction.
    `seller`   | [`bigint`]                   | The identifier of the user selling the item. References `users.id`.
    `item`     | [`text`]                     | The name of the item being sold.
    `end_time` | [`timestamp with time zone`] | The time at which the auction closes.

  * `bids` describes the bids placed in each auction.

    Field        | Type                         | Description
    -------------|------------------------------|------------
    `id`         | [`bigint`]                   | A unique identifier for the bid.
    `buyer`      | [`bigint`]                   | The identifier vof the user placing the bid. References `users.id`.
    `auction_id` | [`bigint`]                   | The identifier of the auction in which the bid is placed. References `auctions.id`.
    `amount`     | [`bigint`]                   | The bid amount in dollars.
    `bid_time`   | [`timestamp with time zone`] | The time at which the bid was placed.

The organizations, users, and accounts are fixed at the time the source
is created. Each tick interval, either a new auction is started, or a new bid
is placed in the currently ongoing auction.


### Marketing

The marketing load generator simulates a marketing organization that is using a machine learning model to send coupons to potential leads. The marketing source will be automatically demuxed
into multiple subsources when the `CREATE SOURCE` command is executed. This will
create the following subsources:

  * `customers` describes the customers that the marketing team may target.

    Field     | Type       | Description
    ----------|------------|------------
    `id`      | [`bigint`] | A unique identifier for the customer.
    `email`   | [`text`]   | The customer's email.
    `income`  | [`bigint`] | The customer's income in pennies.

  * `impressions` describes online ads that have been seen by a customer.

    Field             | Type                         | Description
    ------------------|------------------------------|------------
    `id`              | [`bigint`]                   | A unique identifier for the impression.
    `customer_id`     | [`bigint`]                   | The identifier of the customer that saw the ad. References `customers.id`.
    `impression_time` | [`timestamp with time zone`] | The time at which the ad was seen.

  * `clicks` describes clicks of ads.

    Field             | Type                         | Description
    ------------------|------------------------------|------------
    `impression_id`   | [`bigint`]                   | The identifier of the impression that was clicked. References `impressions.id`.
    `click_time`      | [`timestamp with time zone`] | The time at which the impression was clicked.

  * `leads` describes a potential lead for a purchase.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `id`                | [`bigint`]                   | A unique identifier for the lead.
    `customer_id`       | [`bigint`]                   | The identifier of the customer we'd like to convert. References `customers.id`.
    `created_at`        | [`timestamp with time zone`] | The time at which the lead was created.
    `converted_at`      | [`timestamp with time zone`] | The time at which the lead was converted.
    `conversion_amount` | [`bigint`]                   | The amount the lead converted for in pennies.

  * `coupons` describes coupons given to leads.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `id`                | [`bigint`]                   | A unique identifier for the coupon.
    `lead_id`           | [`bigint`]                   | The identifier of the lead we're attempting to convert. References `leads.id`.
    `created_at`        | [`timestamp with time zone`] | The time at which the coupon was created.
    `amount`            | [`bigint`]                   | The amount the coupon is for in pennies.

  * `conversion_predictions` describes the predictions made by a highly sophisticated machine learning model.

    Field               | Type                         | Description
    --------------------|------------------------------|------------
    `lead_id`           | [`bigint`]                   | The identifier of the lead we're attempting to convert. References `leads.id`.
    `experiment_bucket`| [`text`]                     | Whether the lead is a control or experiment.
    `created_at`        | [`timestamp with time zone`] | The time at which the prediction was made.
    `score`             | [`numeric`]                  | The predicted likelihood the lead will convert.

### TPCH

The TPCH load generator implements the [TPC-H benchmark specification](https://www.tpc.org/tpch/default5.asp).
The TPCH source must be used with `FOR ALL TABLES`, which will create the standard TPCH relations.
If `TICK INTERVAL` is specified, after the initial data load, an order and its lineitems will be changed at this interval.
If not specified, the dataset will not change over time.

### Monitoring source progress

By default, load generator sources expose progress metadata as a subsource that
you can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type        | Meaning
---------------|-------------|--------
`offset`       | [`uint8`]   | The minimum offset for which updates to this sources are still undetermined.

And can be queried using:

```mzsql
SELECT "offset"
FROM <src_name>_progress;
```

As long as the offset continues increasing, Materialize is generating data. For
more details on monitoring source ingestion progress and debugging related
issues, see [Troubleshooting](/ops/troubleshooting/).

## Examples

### Creating an auction load generator

To create a load generator source that simulates an auction house and emits new data every second:

```mzsql
CREATE SOURCE auction_house
  FROM LOAD GENERATOR AUCTION
  (TICK INTERVAL '1s')
  FOR ALL TABLES;
```

To display the created subsources:

```mzsql
SHOW SOURCES;
```
```nofmt
          name          |      type
------------------------+----------------
 accounts               | subsource
 auction_house          | load-generator
 auction_house_progress | progress
 auctions               | subsource
 bids                   | subsource
 organizations          | subsource
 users                  | subsource
```

To examine the simulated bids:

```mzsql
SELECT * from bids;
```
```nofmt
 id | buyer | auction_id | amount |          bid_time
----+-------+------------+--------+----------------------------
 10 |  3844 |          1 |     59 | 2022-09-16 23:24:07.332+00
 11 |  1861 |          1 |     40 | 2022-09-16 23:24:08.332+00
 12 |  3338 |          1 |     97 | 2022-09-16 23:24:09.332+00
```

### Creating a marketing load generator

To create a load generator source that simulates an online marketing campaign:

```mzsql
CREATE SOURCE marketing
  FROM LOAD GENERATOR MARKETING
  FOR ALL TABLES;
```

To display the created subsources:

```mzsql
SHOW SOURCES;
```

```nofmt
          name          |      type
------------------------+---------------
 clicks                 | subsource
 conversion_predictions | subsource
 coupons                | subsource
 customers              | subsource
 impressions            | subsource
 leads                  | subsource
 marketing              | load-generator
 marketing_progress     | progress
```

To find all impressions and clicks associated with a campaign over the last 30 days:

```mzsql
WITH
    click_rollup AS
    (
        SELECT impression_id AS id, count(*) AS clicks
        FROM clicks
        WHERE click_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY impression_id
    ),
    impression_rollup AS
    (
        SELECT id, campaign_id, count(*) AS impressions
        FROM impressions
        WHERE impression_time - INTERVAL '30' DAY <= mz_now()
        GROUP BY id, campaign_id
    )
SELECT campaign_id, sum(impressions) AS impressions, sum(clicks) AS clicks
FROM impression_rollup LEFT JOIN click_rollup USING(id)
GROUP BY campaign_id;
```

```nofmt
 campaign_id | impressions | clicks
-------------+-------------+--------
           0 |         350 |     33
           1 |         325 |     28
           2 |         319 |     24
           3 |         315 |     38
           4 |         305 |     28
           5 |         354 |     31
           6 |         346 |     25
           7 |         337 |     36
           8 |         329 |     38
           9 |         305 |     24
          10 |         345 |     27
          11 |         323 |     30
          12 |         320 |     29
          13 |         331 |     27
          14 |         310 |     22
          15 |         324 |     28
          16 |         315 |     32
          17 |         329 |     36
          18 |         329 |     28
```

### Creating a TPCH load generator

To create the load generator source and its associated subsources:

```mzsql
CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)
  FOR ALL TABLES;
```

To display the created subsources:

```mzsql
SHOW SOURCES;
```
```nofmt
      name     |      type
---------------+---------------
 tpch          | load-generator
 tpch_progress | progress
 supplier      | subsource
 region        | subsource
 partsupp      | subsource
 part          | subsource
 orders        | subsource
 nation        | subsource
 lineitem      | subsource
 customer      | subsource
```

To run the Pricing Summary Report Query (Q1), which reports the amount of
billed, shipped, and returned items:

```mzsql
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
```
```nofmt
 l_returnflag | l_linestatus | sum_qty  | sum_base_price | sum_disc_price  |    sum_charge     |      avg_qty       |     avg_price      |      avg_disc       | count_order
--------------+--------------+----------+----------------+-----------------+-------------------+--------------------+--------------------+---------------------+-------------
 A            | F            | 37772997 |    56604341792 |  54338346989.17 |  57053313118.2657 | 25.490380624798817 | 38198.351517998075 | 0.04003729114831228 |     1481853
 N            | F            |   986796 |     1477585066 |   1418531782.89 |   1489171757.0798 | 25.463731840115603 |  38128.27564317601 | 0.04007431682708436 |       38753
 N            | O            | 74281600 |   111337230039 | 106883023012.04 | 112227399730.9018 |  25.49430183051871 | 38212.221432873834 | 0.03999775539657235 |     2913655
 R            | F            | 37770949 |    56610551077 |   54347734573.7 |  57066196254.4557 | 25.496431466814634 |  38213.68205054471 | 0.03997848687172654 |     1481421
```

## Related pages

- [`CREATE SOURCE`](../)

[`bigint`]: /sql/types/bigint
[`numeric`]: /sql/types/numeric
[`text`]: /sql/types/text
[`bytea`]: /sql/types/bytea
[`interval`]: /sql/types/interval
[`uint8`]: /sql/types/uint/#uint8-info
[`timestamp with time zone`]: /sql/types/timestamp
[feature request]: https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests




---

## CREATE SOURCE: MySQL


{{% create-source/intro %}}
Materialize supports MySQL (5.7+) as a real-time data source. To connect to a
MySQL database, you first need to tweak its configuration to enable
[GTID-based binary log (binlog) replication](#change-data-capture), and then
[create a connection](#creating-a-connection) in Materialize that specifies
access and authentication parameters.
{{% /create-source/intro %}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

## Syntax

{{< note >}}
Although `schema` and `database` are [synonyms in MySQL](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema),
the MySQL source documentation and syntax **standardize on `schema`** as the
preferred keyword.
{{< /note >}}

{{< diagram "create-source-mysql.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the MySQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#mysql) documentation page.
**FOR ALL TABLES** | Create subsources for all tables in all schemas upstream. The [`mysql` system schema](https://dev.mysql.com/doc/refman/8.3/en/system-schema.html) is ignored.
**FOR SCHEMAS (** _schema_list_ **)** | Create subsources for specific schemas upstream.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables upstream. Requires fully-qualified table names (`<schema>.<table>`).
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress collection for the source. If this is not specified, the progress collection will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

### `CONNECTION` options

Field             | Value                           | Description
------------------|---------------------------------|-------------------------------------
`EXCLUDE COLUMNS` | A list of fully-qualified names | Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize.
`TEXT COLUMNS`    | A list of fully-qualified names | Decode data as `text` for specific columns that contain MySQL types that are [unsupported in Materialize](#supported-types).

## Features

### Change data capture

{{< note >}}
For step-by-step instructions on enabling GTID-based binlog replication for your
MySQL service, see the integration guides:
[Amazon RDS](/ingest-data/mysql/amazon-rds/),
[Amazon Aurora](/ingest-data/mysql/amazon-aurora/),
[Azure DB](/ingest-data/mysql/azure-db/),
[Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/ingest-data/mysql/self-hosted/).
{{< /note >}}

The source uses MySQL's binlog replication protocol to **continually ingest
changes** resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database. This process is known as _change data capture_.

The replication method used is based on [global transaction identifiers (GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html),
and guarantees **transactional consistency** — any operation inside a MySQL
transaction is assigned the same timestamp in Materialize, which means that the
source will never show partial results based on partially replicated
transactions.

Before creating a source in Materialize, you **must** configure the upstream
MySQL database for GTID-based binlog replication. Ensure the upstream MySQL
database has been configured for GTID-based binlog replication:

{{% mysql-direct/ingesting-data/mysql-configs %}}

If you're running MySQL using a managed service, additional configuration
changes might be required. For step-by-step instructions on enabling GTID-based
binlog replication for your MySQL service, see the integration guides.

#### Binlog retention

{{< warning >}}
If Materialize tries to resume replication and finds GTID gaps due to missing
binlog files, the source enters an errored state and you have to drop and
recreate it.
{{< /warning >}}

By default, MySQL retains binlog files for **30 days** (i.e., 2592000 seconds)
before automatically removing them. This is configurable via the
[`binlog_expire_logs_seconds`](https://dev.mysql.com/doc/mysql-replication-excerpt/8.0/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds)
system variable. We recommend using the default value for this configuration in
order to not compromise Materialize's ability to resume replication in case of
failures or restarts.

In some MySQL managed services, binlog expiration can be overriden by a
service-specific configuration parameter. It's important that you double-check
if such a configuration exists, and ensure it's set to the maximum interval
available.

As an example, [Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/) has its
own configuration parameter for binlog retention ([`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours))
that overrides `binlog_expire_logs_seconds` and is set to `NULL` by default.

#### Creating a source

Materialize ingests the raw replication stream data for all (or a specific set
of) tables in your upstream MySQL database.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **subsource** for each original table upstream, and perform an
   initial, snapshot-based sync of the tables before it starts ingesting change
   events.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type    |  cluster  |
    ----------------------+-----------+------------
     mz_source            | mysql     |
     mz_source_progress   | progress  |
     table_1              | subsource |
     table_2              | subsource |
    ```

1. Incrementally update any materialized or indexed views that depend on the
   source as change events stream in, as a result of `INSERT`, `UPDATE` and
   `DELETE` operations in the upstream MySQL database.

##### MySQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

### Monitoring source progress

[//]: # "TODO(morsapaes) Replace this section with guidance using the new
progress metrics in mz_source_statistics + console monitoring, when available
(also for PostgreSQL)."

By default, MySQL sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field              | Type                                                    | Details
-------------------|---------------------------------------------------------|--------------
`source_id_lower`  | [`uuid`](/sql/types/uuid/)  | The lower-bound GTID `source_id` of the GTIDs covered by this range.
`source_id_upper`  | [`uuid`](/sql/types/uuid/)  | The upper-bound GTID `source_id` of the GTIDs covered by this range.
`transaction_id`   | [`uint8`](/sql/types/uint/#uint8-info)                  | The `transaction_id` of the next GTID possible from the GTID `source_id`s covered by this range.

And can be queried using:

```mzsql
SELECT transaction_id
FROM <src_name>_progress;
```

Progress metadata is represented as a [GTID set](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
of future possible GTIDs, which is similar to the [`gtid_executed`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed)
system variable on a MySQL replica. The reported `transaction_id` should
increase as Materialize consumes **new** binlog records from the upstream MySQL
database. For more details on monitoring source ingestion progress and
debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{% include-from-yaml data="mysql_source_details"
name="mysql-considerations" %}}

## Examples

{{< important >}}
Before creating a MySQL source, you must enable GTID-based binlog replication in the
upstream database. For step-by-step instructions, see the integration guide for
your MySQL service: [Amazon RDS](/ingest-data/mysql/amazon-rds/),
[Amazon Aurora](/ingest-data/mysql/amazon-aurora/),
[Azure DB](/ingest-data/mysql/azure-db/),
[Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/ingest-data/mysql/self-hosted/).
{{< /important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#mysql) documentation page.

```mzsql
CREATE SECRET mysqlpass AS '<MYSQL_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'materialize',
    PASSWORD SECRET mysqlpass
);
```

If your MySQL server is not exposed to the public internet, you can [tunnel the
connection](/sql/create-connection/#network-security-connections) through an AWS
PrivateLink service (Materialize Cloud) or an SSH bastion host SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass,
    AWS PRIVATELINK privatelink_svc
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize'
);
```

```mzsql
CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

_Create subsources for all tables in MySQL_

```mzsql
CREATE SOURCE mz_source
    FROM MYSQL CONNECTION mysql_connection
    FOR ALL TABLES;
```

_Create subsources for all tables from specific schemas in MySQL_

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR SCHEMAS (mydb, project);
```

_Create subsources for specific tables in MySQL_

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

#### Handling unsupported types

If you're replicating tables that use [data types unsupported](#supported-types)
by Materialize, use the `TEXT COLUMNS` option to decode data as `text` for the
affected columns. This option expects the upstream fully-qualified names of the
replicated table and column (i.e. as defined in your MySQL database).

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    TEXT COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
```

#### Excluding columns

MySQL doesn't provide a way to filter out columns from the replication stream.
To exclude specific upstream columns from being ingested, use the `EXCLUDE
COLUMNS` option.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_to_ignore)
  )
  FOR ALL TABLES;
```

### Handling errors and schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

To handle upstream [schema changes](#schema-changes) or errored subsources, use
the [`DROP SOURCE`](/sql/alter-source/#context) syntax to drop the affected
subsource, and then [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to add
the subsource back to the source.

```mzsql
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- MySQL integration guides:
  - [Amazon RDS](/ingest-data/mysql/amazon-rds/)
  - [Amazon Aurora](/ingest-data/mysql/amazon-aurora/)
  - [Azure DB](/ingest-data/mysql/azure-db/)
  - [Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/)
  - [Self-hosted](/ingest-data/mysql/self-hosted/)




---

## CREATE SOURCE: PostgreSQL (Legacy Syntax)


{{< source-versioning-disambiguation is_new=false
other_ref="[new reference page](/sql/create-source/postgres-v2)"
include_blurb=true >}}

{{% create-source/intro %}}
Materialize supports PostgreSQL (11+) as a data source. To connect to a
PostgreSQL instance, you first need to [create a connection](#creating-a-connection)
that specifies access and authentication parameters.
Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements.
{{% /create-source/intro %}}

{{< warning >}}
Before creating a PostgreSQL source, you must set up logical replication in the
upstream database. For step-by-step instructions, see the integration guide for
your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).
{{< /warning >}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the PostgreSQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.
**FOR ALL TABLES** | Create subsources for all tables in the publication.
**FOR SCHEMAS (** _schema_list_ **)** | Create subsources for specific schemas in the publication.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables in the publication.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress collection for the source. If this is not specified, the progress collection will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

### `CONNECTION` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`PUBLICATION`                        | `text`    | **Required.** The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).
`TEXT COLUMNS`                       | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.
`EXCLUDE COLUMNS`                    | A list of fully-qualified names | Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize.

## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest
changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database — a process also known as _change data capture_.

For this reason, you must configure the upstream PostgreSQL database to support
logical replication before creating a source in Materialize. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).

#### Creating a source

To avoid creating multiple replication slots in the upstream PostgreSQL database
and minimize the required bandwidth, Materialize ingests the raw replication
stream data for some specific set of tables in your publication.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **replication slot** in the upstream PostgreSQL database (see
   [PostgreSQL replication slots](#postgresql-replication-slots)).

    The name of the replication slot created by Materialize is prefixed with
    `materialize_` for easy identification, and can be looked up in
    `mz_internal.mz_postgres_sources`.

    ```mzsql
    SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
    ```

    ```
       id   |             replication_slot
    --------+----------------------------------------------
     u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
    ```
1. Create a **subsource** for each original table in the publication.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type
    ----------------------+-----------
     mz_source            | postgres
     mz_source_progress   | progress
     table_1              | subsource
     table_2              | subsource
    ```

    And perform an initial, snapshot-based sync of the tables in the publication
    before it starts ingesting change events.

1. Incrementally update any materialized or indexed views that depend on the
source as change events stream in, as a result of `INSERT`, `UPDATE` and
`DELETE` operations in the upstream PostgreSQL database.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in the
specified publication using **a single** replication slot. This allows you to
minimize the performance impact on the upstream database, as well as reuse the
same source across multiple materializations.

{{< tip >}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-replication-slots-tip-list" %}}

{{</ tip >}}

##### PostgreSQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1);
```

### Monitoring source progress

By default, PostgreSQL sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`lsn`          | [`uint8`](/sql/types/uint/#uint8-info)   | The last Log Sequence Number (LSN) consumed from the upstream PostgreSQL replication stream.

And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported LSN should increase as Materialize consumes **new** WAL records
from the upstream PostgreSQL database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

### Schema changes

Materialize supports schema changes in the upstream database as follows:

#### Compatible schema changes (Legacy syntax)

{{% include-from-yaml data="postgres_source_details"
name="postgres-compatible-schema-changes-legacy" %}}

#### Incompatible schema changes

{{% include-from-yaml data="postgres_source_details"
name="postgres-incompatible-schema-changes-legacy" %}}

### Publication membership

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership-mitigation-legacy" %}}

### Supported types

{{% include-from-yaml data="postgres_source_details"
name="postgres-supported-types" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-unsupported-types" %}}

### Truncation

{{% include-from-yaml data="postgres_source_details"
name="postgres-truncation-restriction" %}}

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables-action-legacy" %}}

## Examples

{{< important >}}
Before creating a PostgreSQL source, you must set up logical replication in the
upstream database. For step-by-step instructions, see the integration guide for
your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).
{{< /important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

If your PostgreSQL server is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service (Materialize Cloud) or an SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

```mzsql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL ssh_connection,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

_Create subsources for all tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

_Create subsources for all tables from specific schemas included in the
 PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR SCHEMAS (public, project);
```

_Create subsources for specific tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2);
```

#### Handling unsupported types

If the publication contains tables that use [data types](/sql/types/)
unsupported by Materialize, use the `TEXT COLUMNS` option to decode data as
`text` for the affected columns. This option expects the upstream names of the
replicated table and column (i.e. as defined in your PostgreSQL database).

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (upstream_table_name.column_of_unsupported_type)
  ) FOR ALL TABLES;
```

### Handling errors and schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

To handle upstream [schema changes](#schema-changes) or errored subsources, use
the [`DROP SOURCE`](/sql/alter-source/#context) syntax to drop the affected
subsource, and then [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to add
the subsource back to the source.

```mzsql
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```
#### Adding subsources

When adding subsources to a PostgreSQL source, Materialize opens a temporary
replication slot to snapshot the new subsources' current states. After
completing the snapshot, the table will be kept up-to-date, like all other
tables in the publication.

#### Dropping subsources

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- PostgreSQL integration guides:
  - [AlloyDB](/ingest-data/postgres-alloydb/)
  - [Amazon RDS](/ingest-data/postgres-amazon-rds/)
  - [Amazon Aurora](/ingest-data/postgres-amazon-aurora/)
  - [Azure DB](/ingest-data/postgres-azure-db/)
  - [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/)
  - [Self-hosted](/ingest-data/postgres-self-hosted/)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html




---

## CREATE SOURCE: PostgreSQL (New Syntax)


{{< private-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/postgres/)" include_blurb=true >}}

{{% create-source-intro external_source="PostgreSQL" version="11+"
create_table="/sql/create-table/" %}}

## Prerequisites

{{% include-from-yaml data="postgres_source_details"
name="postgres-source-prereq" %}}

## Syntax

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="syntax-options" %}}

## Details

### Ingesting data

After a source is created, you can create tables from the source, referencing
the tables in the publication, to start ingesting data. You can create multiple
tables that reference the same table in the publication.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Handling table schema changes

The use of the `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of certain upstream DDL
changes without downtime.

See [`CREATE TABLE FROM
SOURCE`](/sql/create-table/#handling-table-schema-changes) for details.

#### Supported types

With the new syntax, after a PostgreSQL source is created, you [`CREATE TABLE
FROM SOURCE`](/sql/create-table/) to create a corresponding table in
Matererialize and start ingesting data.

{{% include-from-yaml data="postgres_source_details"
name="postgres-supported-types" %}}

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

#### Upstream table truncation restrictions

{{% include-from-yaml data="postgres_source_details"
name="postgres-truncation-restriction" %}}

For additional considerations, see also [`CREATE TABLE`](/sql/create-table/).

### Publication membership

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-publication-membership-mitigation-legacy" %}}

### PostgreSQL replication slots

When you define a source, Materialize will automatically create a **replication
slot** in the upstream PostgreSQL database (see [PostgreSQL replication
slots](#postgresql-replication-slots)). Each source ingests the raw replication
stream data for all tables in the specified publication using **a single**
replication slot. This allows you to minimize the performance impact on the
upstream database as well as reuse the same source across multiple
materializations.

The name of the replication slot created by Materialize is prefixed with
`materialize_`. In Materialize, you can query the
`mz_internal.mz_postgres_sources` to find the replication slots created:

```mzsql
SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
```

```
    id   |             replication_slot
---------+----------------------------------------------
  u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
```


{{< tip >}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-replication-slots-tip-list" %}}

{{</ tip >}}

## Examples

### Prerequisites

{{% include-from-yaml data="postgres_source_details"
name="postgres-source-prereq" %}}


### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_postgres_source"
 example="example-prereq" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-source" %}}

{{% include-example file="examples/create_source/example_postgres_source"
 example="create-table" %}}

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [PostgreSQL integration guides](/ingest-data/postgres/#integration-guides)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html




---

## CREATE SOURCE: SQL Server


{{% create-source/intro %}}
Materialize supports SQL Server (2016+) as a real-time data source. To connect to a
SQL Server database, you first need to tweak its configuration to enable [Change Data
Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server)
and [`SNAPSHOT` transaction isolation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)
for the database that you would like to replicate. Then [create a connection](#creating-a-connection)
in Materialize that specifies access and authentication parameters.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-sql-server.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the SQL Server connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#sql-server) documentation page.
**FOR ALL TABLES** | Create subsources for all tables with CDC enabled in all schemas upstream.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables upstream. Requires fully-qualified table names (`<schema>.<table>`).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Creating a source

Materialize ingests the CDC stream for all (or a specific set of) tables in your
upstream SQL Server database that have [Change Data Capture enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server).

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **subsource** for each capture instance upstream, and perform an
   initial, snapshot-based sync of the associated tables before it starts
   ingesting change events.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type     |  cluster  |
    ----------------------+------------+------------
     mz_source            | sql-server |
     mz_source_progress   | progress   |
     table_1              | subsource  |
     table_2              | subsource  |
    ```

1. Incrementally update any materialized or indexed views that depend on the
   source as change events stream in, as a result of `INSERT`, `UPDATE` and
   `DELETE` operations in the upstream SQL Server database.

##### SQL Server schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

### Monitoring source progress

[//]: # "TODO(morsapaes) Replace this section with guidance using the new
progress metrics in mz_source_statistics + console monitoring, when available
(also for PostgreSQL)."

By default, SQL Server sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field     | Type                          | Details
----------|-------------------------------|--------------
`lsn`     | [`bytea`](/sql/types/bytea/)  | The upper-bound [Log Sequence Number](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide) replicated thus far into Materialize.


And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported `lsn` should increase as Materialize consumes **new** CDC events
from the upstream SQL Server database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{% include-md file="shared-content/sql-server-considerations.md" %}}

## Examples

{{< important >}}
Before creating a SQL Server source, you must enable Change Data Capture and
`SNAPSHOT` transaction isolation in the upstream database.
{{</ important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#sql-server) documentation page.

```mzsql
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 1433,
    USER 'materialize',
    PASSWORD SECRET sqlserver_pass,
    DATABASE '<DATABASE_NAME>'
);
```

If your SQL Server instance is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through and SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
    DATABASE '<DATABASE_NAME>'
);
```

```mzsql
CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection,
    DATABASE '<DATABASE_NAME>'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

You **must** enable Change Data Capture, see [Enable Change Data Capture SQL Server Instructions](/ingest-data/sql-server/self-hosted/#a-configure-sql-server).

Once CDC is enabled for all of the relevant tables, you can create a `SOURCE` in
Materialize to begin replicating data!

_Create subsources for all tables in SQL Server_

```mzsql
CREATE SOURCE mz_source
    FROM SQL SERVER CONNECTION sqlserver_connection
    FOR ALL TABLES;
```

_Create subsources for specific tables in SQL Server_

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

#### Handling unsupported types

If you're replicating tables that use [data types unsupported](#supported-types)
by SQL Server's CDC feature, use the `EXCLUDE COLUMNS` option to exclude them from
replication. This option expects the upstream fully-qualified names of the
replicated table and column (i.e. as defined in your SQL Server database).

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
```

### Handling errors and schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

To handle upstream [schema changes](#schema-changes) or errored subsources, use
the [`DROP SOURCE`](/sql/alter-source/#context) syntax to drop the affected
subsource, and then [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to add
the subsource back to the source.

```mzsql
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)




---

## CREATE SOURCE: Webhook


{{% create-source/intro %}}
Webhook sources expose a [public URL](#webhook-url) that allows your applications to push webhook events into Materialize.
{{% /create-source/intro %}}

## Syntax

```mzsql
CREATE SOURCE [IF NOT EXISTS] <src_name>
[ IN CLUSTER <cluster_name> ]
FROM WEBHOOK
  BODY FORMAT <TEXT | JSON [ARRAY] | BYTES>
  [ INCLUDE HEADER <header_name> AS <column_alias> [BYTES]  |
    INCLUDE HEADERS [ ( [NOT] <header_name> [, [NOT] <header_name> ... ] ) ]
  ][...]
  [ CHECK (
      [ WITH ( <BODY|HEADERS|SECRET <secret_name>> [AS <alias>] [BYTES] [, ...])]
      <check_expression>
    )
  ]
```


### `webhook_check_option`

Field                            | Use
---------------------------------|--------------------------
  _src_name_                     | The name for the source.
 **IN CLUSTER** _cluster_name_   | The [cluster](/sql/create-cluster) to maintain this source.
 **INCLUDE HEADER**              | Map a header value from a request into a column.
 **INCLUDE HEADERS**             | Include a column named `'headers'` of type `map[text => text]` containing the headers of the request.
 **CHECK**                       | Specify a boolean expression that is used to validate each request received by the source.

### `CHECK WITH` options

Field                  | Type                | Description
-----------------------|---------------------|--------------
`BODY`                 | `text` or `bytea`   | Provide a `body` column to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the data type can be changed to `bytea` with the optional **BYTES** keyword.
`HEADERS`              | `map[text=>text]` or `map[text=>bytea]` | Provide a column `'headers'` to the check expression. The column can be renamed with the optional **AS** _alias_ statement, and the data type can be changed to `map[text => bytea]` with the optional **BYTES** keyword.
`SECRET` _secret_name_ | `text` or `bytea`    | Securely provide a [`SECRET`](/sql/create-secret) to the check expression. The `constant_time_eq` validation function **does not support** fully qualified secret names: if the secret is in a different namespace to the source, the column can be renamed with the optional **AS** _alias_ statement. The data type can also be changed to `bytea` using the optional **BYTES** keyword.

## Supported formats

|<div style="width:290px">Body format</div> | Type      | Description       |
--------------------------------------------| --------- |-------------------|
| `BYTES`                                   | `bytea`   | Does **no parsing** of the request, and stores the body of a request as it was received. |
| `JSON`                                    | `jsonb`   | Parses the body of a request as JSON. Also accepts events batched as newline-delimited JSON (`NDJSON`). If the body is not valid JSON, a response of `400` Bad Request will be returned. |
| `JSON ARRAY`                              | `jsonb`   | Parses the body of a request as a list of JSON objects, automatically expanding the list of objects to individual rows. Also accepts a single JSON object. If the body is not valid JSON, a response of `400` Bad Request will be returned. |
| `TEXT`                                    | `text`    | Parses the body of a request as `UTF-8` text. If the body is not valid `UTF-8`, a response of `400` Bad Request will be returned. |

## Output

If source creation is successful, you'll have a new source object with
name _src_name_ and, based on what you defined with `BODY FORMAT` and `INCLUDE
HEADERS`, the following columns:

Column     | Type                        | Optional?                                      |
-----------|-----------------------------|------------------------------------------------|
 `body`    | `bytea`, `jsonb`, or `text` |                                                |
 `headers` | `map[text => text]`         | ✓ . Present if `INCLUDE HEADERS` is specified. |

### Webhook URL

After source creation, the unique URL that allows you to **POST** events to the
source can be looked up in the [`mz_internal.mz_webhook_sources`](/sql/system-catalog/mz_internal/#mz_webhook_sources)
system catalog table. The URL will have the following format:

```
https://<HOST>/api/webhook/<database>/<schema>/<src_name>
```

A breakdown of each component is as follows:

- `<HOST>`: The Materialize instance URL, which can be found on the [Materialize console](/console/).
- `<database>`: The name of the database where the source is created (default is `materialize`).
- `<schema>`: The schema name where the source gets created (default is `public`).
- `<src_name>`: The name you provided for your source at the time of creation.

{{< note >}}
This is a public URL that is open to the internet and has no security. To
validate that requests are legitimate, see [Validating requests](#validating-requests).
For limits imposed on this endpoint, see [Request limits](#request-limits).
{{< /note >}}

## Features

### Exposing headers

In addition to the request body, Materialize can expose headers to SQL. If a
request header exists, you can map its fields to columns using the `INCLUDE
HEADER` syntax.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADER 'timestamp' as ts
  INCLUDE HEADER 'x-event-type' as event_type;
```

This example would have the following columns:

Column      | Type    | Nullable? |
------------|---------|-----------|
 body       | `jsonb` | No        |
 ts         | `text`  | Yes       |
 event_type | `text`  | Yes       |

All of the header columns are nullable. If the header of a request does not
contain a specified field, the `NULL` value will be used as a default.

#### Excluding header fields

To exclude specific header fields from the mapping, use the `INCLUDER HEADERS`
syntax in combination with the `NOT` option. This can be useful if, for
example, you need to accept a dynamic list of fields but want to exclude
sensitive information like authorization.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  INCLUDE HEADERS ( NOT 'authorization', NOT 'x-api-key' );
```

This example would have the following columns:

Column      | Type                | Nullable?  |
------------|---------------------|------------|
 body       | `jsonb`             | No         |
 headers    | `map[text => text]` | No         |

All header fields but `'authorization'` and `'x-api-key'` will get included in
the `headers` map column.

### Validating requests

{{< warning >}}
Without a `CHECK` statement, **all requests will be accepted**. To prevent bad
actors from injecting data into your source, it is **strongly encouraged** that
you define a `CHECK` statement with your webhook sources.
{{< /warning >}}

It's common for applications using webhooks to provide a method for validating a
request is legitimate. You can specify an expression to do this validation for
your webhook source using the `CHECK` clause.

For example, the following source HMACs the request body using the `sha256`
hashing algorithm, and asserts the result is equal to the value provided in the
`x-signature` header, decoded with `base64`.

```mzsql
CREATE SOURCE my_webhook_source FROM WEBHOOK
  BODY FORMAT JSON
  CHECK (
    WITH (
      HEADERS, BODY AS request_body,
      SECRET my_webhook_shared_secret AS validation_secret
    )
    -- The constant_time_eq validation function **does not support** fully
    -- qualified secret names. We recommend always aliasing the secret name
    -- for ease of use.
    constant_time_eq(
        decode(headers->'x-signature', 'base64'),
        hmac(request_body, validation_secret, 'sha256')
    )
  );
```

The headers and body of the request are only subject to validation if `WITH
( BODY, HEADERS, ... )` is specified as part of the `CHECK` statement. By
default, the type of `body` used for validation is `text`, regardless of the
`BODY FORMAT` you specified for the source. In the example above, the `body`
column for `my_webhook_source` has a type of `jsonb`, but `request_body` as
used in the validation expression has type `text`. Futher, the request headers
are not persisted as part of `my_webhook_source`, since `INCLUDE HEADERS` was
not specified — but they are provided to the validation expression.

#### Debugging validation

It can be difficult to get your `CHECK` statement correct, especially if your
application does not have a way to send test events. If you're having trouble
with your `CHECK` statement, we recommend creating a temporary source without
`CHECK` and using that to iterate more quickly.

```mzsql
CREATE SOURCE my_webhook_temporary_debug FROM WEBHOOK
  -- Specify the BODY FORMAT as TEXT or BYTES,
  -- which is how it's provided to CHECK.
  BODY FORMAT TEXT
  INCLUDE HEADERS;
```

Once you have a few events in _my_webhook_temporary_debug_, you can query it with your would-be
`CHECK` statement.

```mzsql
SELECT
  -- Your would-be CHECK statement.
  constant_time_eq(
    decode(headers->'signature', 'base64'),
    hmac(headers->'timestamp' || body, 'my key', 'sha512')
  )
FROM my_webhook_temporary_debug
LIMIT 10;
```

{{< note >}}
It's not possible to use secrets in a `SELECT` statement, so you'll need to
provide these values as raw text for debugging.
{{< /note >}}

### Handling duplicated and partial events

Given any number of conditions, e.g. a network hiccup, it's possible for your application to send
an event more than once. If your event contains a unique identifier, you can de-duplicate these events
using a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) and the `DISTINCT ON` clause.

```mzsql
CREATE MATERIALIZED VIEW my_webhook_idempotent AS (
  SELECT DISTINCT ON (body->>'unique_id') *
  FROM my_webhook_source
  ORDER BY id
);
```

We can take this technique a bit further to handle partial events. Let's pretend our application
tracks the completion of build jobs, and it sends us JSON objects with following structure.

Key           | Value   | Optional? |
--------------|---------|-----------|
_id_          | `text`  | No
_started_at_  | `text`  | Yes
_finished_at_ | `text`  | Yes

When a build job starts we receive an event containing _id_ and the _started_at_ timestamp. When a
build finished, we'll receive a second event with the same _id_ but now a _finished_at_ timestamp.
To merge these events into a single row, we can again use the `DISTINCT ON` clause.

```mzsql
CREATE MATERIALIZED VIEW my_build_jobs_merged AS (
  SELECT DISTINCT ON (id) *
  FROM (
    SELECT
      body->>'id' as id,
      try_parse_monotonic_iso8601_timestamp(body->>'started_at') as started_at,
      try_parse_monotonic_iso8601_timestamp(body->>'finished_at') as finished_at
    FROM my_build_jobs_source
  )
  ORDER BY id, finished_at NULLS LAST, started_at NULLS LAST
);
```

{{< note >}}
When casting from `text` to `timestamp` you should prefer to use the [`try_parse_monotonic_iso8601_timestamp`](/sql/functions/pushdown/)
function, which enables [temporal filter pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).
{{< /note >}}

### Handling batch events

The application pushing events to your webhook source may batch multiple events
into a single HTTP request. The webhook source supports parsing batched events
in the following formats:

#### JSON arrays

You can automatically expand a batch of requests formatted as a JSON array into
separate rows using `BODY FORMAT JSON ARRAY`.

```mzsql
-- Webhook source that parses request bodies as a JSON array.
CREATE SOURCE webhook_source_json_array FROM WEBHOOK
  BODY FORMAT JSON ARRAY
  INCLUDE HEADERS;
```

If you `POST` a JSON array of three elements to `webhook_source_json_array`,
three rows will get appended to the source.

```bash
POST webhook_source_json_array
[
  { "event_type": "a" },
  { "event_type": "b" },
  { "event_type": "c" }
]
```

```mzsql
SELECT COUNT(body) FROM webhook_source_json_array;
----
3
```

You can also post a single object to the source, which will get appended as one
row.

```bash
POST webhook_source_json_array
{ "event_type": "d" }
```

```mzsql
SELECT body FROM webhook_source_json_array;
----
{ "event_type": "a" }
{ "event_type": "b" }
{ "event_type": "c" }
{ "event_type": "d" }
```

#### Newline-delimited JSON (NDJSON)

You can automatically expand a batch of requests formatted as NDJSON into
separate rows using `BODY FORMAT JSON`.

```mzsql
-- Webhook source that parses request bodies as NDJSON.
CREATE SOURCE webhook_source_ndjson FROM WEBHOOK
BODY FORMAT JSON;
```

If you `POST` two elements delimited by newlines to `webhook_source_ndjson`, two
rows will get appended to the source.

```bash
POST 'webhook_source_ndjson'
  { 'event_type': 'foo' }
  { 'event_type': 'bar' }
```

```mzsql
SELECT COUNT(body) FROM webhook_source_ndjson;
----
2
```

## Request limits

Webhook sources apply the following limits to received requests:

* The maximum size of the request body is **`2MB`**. Requests larger than this
  will fail with `413 Payload Too Large`.
* The rate of concurrent requests/second across **all** webhook sources
  is **500**. Trying to connect when the server is at capacity will fail with
  `429 Too Many Requests`.
* Requests that contain a header name specified more than once will be rejected
  with `401 Unauthorized`.

## Examples

### Using basic authentication

[Basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication#basic_authentication_scheme)
enables a simple and rudimentary way to grant authorization to your webhook
source.

To store the sensitive credentials and make them reusable across multiple
`CREATE SOURCE` statements, use [secrets](/sql/create-secret/).

```mzsql
CREATE SECRET basic_hook_auth AS 'Basic <base64_auth>';
```

### Creating a source

After a successful secret creation, you can use the same secret to create
different webhooks with the same basic authentication to check if a request is
valid.

```mzsql
CREATE SOURCE webhook_with_basic_auth
FROM WEBHOOK
    BODY FORMAT JSON
    CHECK (
      WITH (
        HEADERS,
        BODY AS request_body,
        SECRET basic_hook_auth AS validation_secret
      )
      -- The constant_time_eq validation function **does not support** fully
      -- qualified secret names. We recommend always aliasing the secret name
      -- for ease of use.
      constant_time_eq(headers->'authorization', validation_secret)
    );
```

Your new webhook is now up and ready to accept requests using basic
authentication.

#### JSON parsing

Webhook data is ingested as a JSON blob. We recommend creating a parsing view on
top of your webhook source that maps the individual fields to columns with the
required data types. To avoid doing this tedious task manually, you can use
[this **JSON parsing widget**](/sql/types/jsonb/#parsing)!

### Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE SOURCE`](../)
- [`SHOW SOURCES`](/sql/show-sources)
- [`DROP SOURCE`](/sql/drop-source)



