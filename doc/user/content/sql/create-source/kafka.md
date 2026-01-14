---
title: "CREATE SOURCE: Kafka/Redpanda"
description: "Connecting Materialize to a Kafka or Redpanda broker"
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_kafka
    name: Kafka/Redpanda
    weight: 20
aliases:
    - /sql/create-source/avro-kafka
    - /sql/create-source/json-kafka
    - /sql/create-source/protobuf-kafka
    - /sql/create-source/text-kafka
    - /sql/create-source/csv-kafka
---

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


#### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

#### Schema evolution

As long as the writer schema changes in a [compatible way](https://avro.apache.org/docs/++version++/specification/#schema-resolution), Materialize will continue using the original reader schema definition by mapping values from the new to the old schema version. To use the new version of the writer schema in Materialize, you need to **drop and recreate** the source.

#### Name collision

To avoid [case-sensitivity](/sql/identifiers/#case-sensitivity) conflicts with Materialize identifiers, we recommend double-quoting all field names when working with Avro-formatted sources.

#### Supported types

Materialize supports all [Avro
types](https://avro.apache.org/docs/++version++/specification/), _except for_
recursive types and union types in arrays.

{{< /tab >}}

{{< tab "Format JSON" >}}
### Format JSON

Materialize can decode JSON messages into a single column named `data` with type
`jsonb`. Refer to the [`jsonb` type](/sql/types/jsonb) documentation for the
supported operations on this type.

{{% include-syntax file="examples/create_source_kafka" example="syntax-json" %}}

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

To avoid doing this task manually, you can use [this **JSON parsing
widget**](/sql/types/jsonb/#parsing).


#### Schema registry integration

Retrieving schemas from a schema registry is not supported yet for JSON-formatted sources. This means that Materialize cannot decode messages serialized using the [JSON Schema](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer-and-deserializer) serialization format (`JSON_SR`).

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

Unlike Avro, Protobuf does not serialize a schema with the message, so Materialize expects:

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

#### Schema versioning

The _latest_ schema is retrieved using the [`TopicNameStrategy`](https://docs.confluent.io/current/schema-registry/serdes-develop/index.html) strategy at the time the `CREATE SOURCE` statement is issued.

#### Schema evolution

As long as the `.proto` schema definition changes in a [compatible way](https://developers.google.com/protocol-buffers/docs/overview#updating-defs), Materialize will continue using the original schema definition by mapping values from the new to the old schema version. To use the new version of the schema in Materialize, you need to **drop and recreate** the source.

#### Supported types

Materialize supports all [well-known](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf) Protobuf types from the `proto2` and `proto3` specs, _except for_ recursive `Struct` values and map types.

#### Multiple message schemas

When using a schema registry with Protobuf sources, the registered schemas must contain exactly one `Message` definition.

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

In addition to determining how to decode incoming records, Materialize also needs to understand how to interpret them. Whether a new record inserts, updates, or deletes existing data in Materialize depends on the `ENVELOPE` specified in the `CREATE SOURCE` statement.

### Append-only envelope

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE NONE</code></p>

The append-only envelope treats all records as inserts. This is the **default** envelope, if no envelope is specified.

### Upsert envelope

To create a source that uses the standard key-value convention to support
inserts, updates, and deletes within Materialize, you can use `ENVELOPE
UPSERT`. For example:

```mzsql
CREATE SOURCE kafka_upsert
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'events')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

The upsert envelope treats all records as having a **key** and a **value**, and supports inserts, updates and deletes within Materialize:

- If the key does not match a preexisting record, it inserts the record's key and value.

- If the key matches a preexisting record and the value is _non-null_, Materialize updates
  the existing record with the new value.

- If the key matches a preexisting record and the value is _null_, Materialize deletes the record.

{{< note >}}

- Using this envelope is required to consume [log compacted topics](https://docs.confluent.io/platform/current/kafka/design.html#log-compaction).

- This envelope can lead to high memory and disk utilization in the cluster
  maintaining the source. We recommend using a standard-sized cluster, rather
  than a legacy-sized cluster, to automatically spill the workload to disk. See
  [spilling to disk](#spilling-to-disk) for details.

{{< /note >}}

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

### Debezium envelope

{{< debezium-json >}}

Materialize provides a dedicated envelope (`ENVELOPE DEBEZIUM`) to decode Kafka
messages produced by [Debezium](https://debezium.io/). For example:

```mzsql
CREATE SOURCE kafka_repl
  FROM KAFKA CONNECTION kafka_connection (TOPIC 'my_table1')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```

Any materialized view defined on top of this source will be incrementally
updated as new change events stream in through Kafka, as a result of `INSERT`,
`UPDATE` and `DELETE` operations in the original database.

This envelope treats all records as [change events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events) with a diff structure that indicates whether each record should be interpreted as an insert, update or delete within Materialize:

|    |   |
 ----|---
 **Insert** | If the `before` field is _null_, the record represents an upstream [`create` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events), and Materialize inserts the record's key and value.
 **Update** | If the `before` and `after` fields are _non-null_, the record represents an upstream [`update` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-update-events), and Materialize updates the existing record with the new value.
 **Delete** | If the `after` field is _null_, the record represents an upstream [`delete` event](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-delete-events), and Materialize deletes the record.


{{< note>}}

- This envelope can lead to high memory utilization in the cluster maintaining
  the source. Materialize can automatically offload processing to
  disk as needed. See [spilling to disk](#spilling-to-disk) for details.

- Materialize expects a specific message structure that includes the row data
  before and after the change event, which is **not guaranteed** for every
  Debezium connector. For more details, check the [Debezium integration
  guide](/integrations/debezium/).

{{</ note >}}

#### Truncation

The Debezium envelope does not support upstream [`truncate` events](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events).

#### Debezium metadata

The envelope exposes the `before` and `after` value fields from change events.

#### Duplicate handling

Debezium may produce duplicate records if the connector is interrupted. Materialize makes a best-effort attempt to detect and filter out duplicates.

## Features



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
ID PREFIX` option](#syntax) followed by a Materialize-generated
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

## Required Kafka ACLs

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type | Resource type    | Resource name
---------------|------------------|--------------
Read           | Topic            | The specified `TOPIC` option
Read           | Group            | All group IDs starting with the specified [`GROUP ID PREFIX` option](#syntax)

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-source.md" >}}

## Examples

### Prerequisite: Creating a connection

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
