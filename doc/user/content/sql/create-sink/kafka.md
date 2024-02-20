---
title: "CREATE SINK: Kafka"
description: "Connecting Materialize to a Kafka or Redpanda broker sink"
pagerank: 40
aliases:
    - /sql/create-sink/

---

{{% create-sink/intro %}}
To use a Kafka broker (and optionally a schema registry) as a sink, make sure that a connection that specifies access and authentication parameters to that broker already exists; otherwise, you first need to [create a connection](#creating-a-connection). Once created, a connection is **reusable** across multiple `CREATE SINK` and `CREATE SOURCE` statements.
{{% /create-sink/intro %}}

{{< note >}}
The same syntax, supported formats and features can be used to connect to a [Redpanda](/integrations/redpanda/) broker.
{{</ note >}}

Sink source type      | Description
----------------------|------------
**Source**            | Simply pass all data received from the source to the sink without modifying it.
**Table**             | Stream all changes to the specified table out to the sink.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](/sql/create-materialized-view), and _does not_ work with [non-materialized views](/sql/create-view).

## Syntax

{{< diagram "create-sink-kafka.svg" >}}

#### `sink_definition`

{{< diagram "sink-definition.svg" >}}

#### `sink_format_spec`

{{< diagram "sink-format-spec.svg" >}}

#### `kafka_sink_connection`

{{< diagram "kafka-sink-connection.svg" >}}


#### `csr_connection`

{{< diagram "csr-connection.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this sink. If not specified, the `SIZE` option must be specified.
_item&lowbar;name_ | The name of the source, table or materialized view you want to send to the sink.
**CONNECTION** _connection_name_ | The name of the connection to use in the sink. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.
**KEY (** _key&lowbar;column_ **)** | An optional list of columns to use as the Kafka message key. If unspecified, the Kafka key is left unset.
**NOT ENFORCED** | Whether to disable validation of key uniqueness when using the upsert envelope. See [Upsert key selection](#upsert-key-selection) for details.
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](#debezium-envelope) to capture changes in the input view or source.
**ENVELOPE UPSERT** | The sink emits data with [upsert semantics](#upsert-envelope).

### `CONNECTION` options

Field                      | Value  | Description
---------------------------|--------|------------
`TOPIC`                    | `text` | The name of the Kafka topic to write to.
`COMPRESSION TYPE`         | `text` | Default: `none`. The type of compression to apply to messages before they are sent to Kafka: `none`, `gzip`, `snappy`, `lz4`, or `zstd`.
`TRANSACTIONAL ID PREFIX`  | `text` | The prefix of the transactional ID to use when producing to the Kafka topic.<br>Default: `materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}`
`PROGRESS GROUP ID PREFIX` | `text` | The prefix of the consumer group ID to use when reading from the progress topic.<br>Default: `materialize-{REGION ID}-{CONNECTION ID}-{SINK ID}`


### CSR `CONNECTION` options

Field                | Value  | Description
---------------------|--------|------------
`AVRO KEY FULLNAME`  | `text` | Default: `row`. Sets the Avro fullname on the generated key schema, if a `KEY` is specified. When used, a value must be specified for `AVRO VALUE FULLNAME`.
`AVRO VALUE FULLNAME`| `text` | Default: `envelope`. Sets the Avro fullname on the generated value schema. When `KEY` is specified, `AVRO KEY FULLNAME` must additionally be specified.
`NULL DEFAULTS`      | `bool` | Default: `false`. Whether to automatically default nullable fields to `null` in the generated schemas.
`DOC ON`             | `text` | Add a documentation comment to the generated Avro schemas. See [`DOC ON` option syntax](#doc-on-option-syntax) below.

#### `DOC ON` option syntax

{{< private-preview />}}

{{< diagram "create-sink-doc-on-option.svg" >}}

The `DOC ON` option has special syntax, shown above, with the following
mechanics:

  * The `KEY` and `VALUE` options specify whether the comment applies to the
    key schema or the value schema. If neither `KEY` or `VALUE` is specified, the
    comment applies to both types of schemas.

  * The `TYPE` clause names a SQL type or relation, e.g. `my_app.point`.

  * The `COLUMN` clause names a column of a SQL type or relation, e.g.
    `my_app.point.x`.

See [Avro schema documentation](#avro-schema-documentation) for details on
how documentation comments are added to the generated Avro schemas.

### `WITH` options

Field                | Value  | Description
---------------------|--------|------------
`SNAPSHOT`           | `bool` | Default: `true`. Whether to emit the consolidated results of the query before the sink was created at the start of the sink. To see only results after the sink is created, specify `WITH (SNAPSHOT = false)`.

## Formats

The `FORMAT` option controls the encoding of the message key and value that
Materialize writes to Kafka.

**Known limitation:** Materialize does not permit specifying the key
format independently from the value format.

### Avro

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT AVRO</code></p>

When using the Avro format, the value of each Kafka message is an Avro record
containing a field for each column of the sink's underlying relation. The names
and ordering of the fields in the record match the names and ordering of the
columns in the relation.

If the `KEY` option is specified, the key of each Kafka message is an Avro
record containing a field for each key column, in the same order and with
the same names.

If a column name is not a valid Avro name, Materialize adjusts the name
according to the following rules:

  * Replace all non-alphanumeric characters with underscores.
  * If the name begins with a number, add an underscore at the start of the
    name.
  * If the adjusted name is not unique, add the smallest number possible to
    the end of the name to make it unique.

For example, consider a table with two columns named `col-a` and `col@a`.
Materialize will use the names `col_a` and `col_a1`, respectively, in the
generated Avro schema.

When using a Confluent Schema Registry:

  * Materialize will automatically publish Avro schemas for the key, if present,
    and the value to the registry.

  * You can specify the
    [fullnames](https://avro.apache.org/docs/current/specification/#names) for the
    Avro schemas Materialize generates using the `AVRO KEY FULLNAME` and `AVRO
    VALUE FULLNAME` [syntax](#syntax).

  * You can automatically have nullable fields in the Avro schemas default to `null`
    by using the [`NULL DEFAULTS` option](#syntax).

  * You can [add `doc` fields](#avro-schema-documentation) to the Avro schemas.

SQL types are converted to Avro types according to the following conversion
table:

SQL type                         | Avro type
---------------------------------|----------
[`bigint`]                       | `"long"`
[`boolean`]                      | `"boolean"`
[`bytea`]                        | `"bytes"`
[`date`]                         | `{"type": "int", "logicalType": "date"}`
[`double precision`]             | `"double"`
[`integer`]                      | `"int"`
[`interval`]                     | `{"type": "fixed", "size": 16, "name": "com.materialize.sink.interval"}`
[`jsonb`]                        | `{"type": "string", "connect.name": "io.debezium.data.Json"}`
[`map`]                          | `{"type": "map", "values": ...}`
[`list`]                         | `{"type": "array", "items": ...}`
[`numeric(p,s)`][`numeric`]      | `{"type": "bytes", "logicalType": "decimal", "precision": p, "scale": s}`
[`oid`]                          | `{"type": "fixed", "size": 4, "name": "com.materialize.sink.uint4"}`
[`real`]                         | `"float"`
[`record`]                       | `{"type": "record", "name": ..., "fields": ...}`
[`smallint`]                     | `"int"`
[`text`]                         | `"string"`
[`time`]                         | `{"type": "long", "logicalType": "time-micros"}`
[`uint2`]                        | `{"type": "fixed", "size": 2, "name": "com.materialize.sink.uint2"}`
[`uint4`]                        | `{"type": "fixed", "size": 4, "name": "com.materialize.sink.uint4"}`
[`uint8`]                        | `{"type": "fixed", "size": 8, "name": "com.materialize.sink.uint8"}`
[`timestamp (p)`][`timestamp`]   | If precision `p` is less than or equal to 3:<br>`{"type": "long", "logicalType: "timestamp-millis"}`<br>Otherwise:<br>`{"type": "long", "logicalType: "timestamp-micros"}`
[`timestamptz (p)`][`timestamp`] | Same as `timestamp (p)`.
[Arrays]                         | `{"type": "array", "items": ...}`

If a SQL column is nullable, and its type converts to Avro type `t`
according to the above table, the Avro type generated for that column
will be `["null", t]`, since nullable fields are represented as unions
in Avro.

In the case of a sink on a materialized view, Materialize may not be
able to infer the non-nullability of columns in all cases, and will
conservatively assume the columns are nullable, thus producing a union
type as described above. If this is not desired, the materialized view
may be created using [non-null assertions](../../create-materialized-view#non-null-assertions).

#### Avro schema documentation

{{< private-preview />}}

Materialize allows control over the `doc` attribute for record fields and types
in the generated Avro schemas for the sink.

For the container record type (named `row` for the key schema and `envelope` for
the value schema, unless overridden by the [`AVRO ... FULLNAME`
options](#csr-connection-options)), Materialize searches for documentation in
the following locations, in order:

1. For the key schema, a [`KEY DOC ON TYPE` option](#doc-on-option-syntax)
   naming the sink's underlying relation. For the value schema, a
   [`VALUE DOC ON TYPE` option](#doc-on-option-syntax) naming the
   sink's underlying relation.
2. A [comment](/sql/comment-on) on the sink's underlying relation.

For record types within the container record type, Materialize searches for
documentation in the following locations, in order:

1. For the key schema, a [`KEY DOC ON TYPE` option](#doc-on-option-syntax)
   naming the SQL type corresponding to the record type. For the value schema, a
   [`VALUE DOC ON TYPE` option](#doc-on-option-syntax) naming the SQL type
   corresponding to the record type.
2. A [`DOC ON TYPE` option](#doc-on-option-syntax) naming the SQL type
   corresponding to the record type.
3. A [comment](/sql/comment-on) on the SQL type corresponding to the record
   type.

Similarly, for each field of each record type in the Avro schema, Materialize
documentation in the following locations, in order:

1. For the key schema, a [`KEY DOC ON COLUMN` option](#doc-on-option-syntax)
   naming the SQL column corresponding to the field. For the value schema, a
   [`VALUE DOC ON COLUMN` option](#doc-on-option-syntax) naming the column
   corresponding to the field.
2. A [`DOC ON COLUMN` option](#doc-on-option-syntax) naming the SQL column
   corresponding to the field.
3. A [comment](/sql/comment-on) on the SQL column corresponding to the field.

For each field or type, Materialize uses the documentation from the first
location that exists. If no documentation is found for a given field or type,
the `doc` attribute is omitted for that field or type.

### JSON

<p style="font-size:14px"><b>Syntax:</b> <code>FORMAT JSON</code></p>

When using the JSON format, the value of each Kafka message is a JSON object
containing a field for each column of the sink's underlying relation. The names
and ordering of the fields in the record match the names and ordering of the
columns in the relation.

If the `KEY` option is specified, the key of each Kafka message is a JSON
object containing a field for each key column, in the same order and with the
same names.

SQL values are converted to JSON values according to the following conversion
table:

SQL type                     | Conversion
-----------------------------|-------------------------------------
[`array`][`arrays`]          | Values are converted to JSON arrays.
[`bigint`]                   | Values are converted to JSON numbers.
[`boolean`]                  | Values are converted to `true` or `false`.
[`integer`]                  | Values are converted to JSON numbers.
[`list`]                     | Values are converted to JSON arrays.
[`numeric`]                  | Values are converted to a JSON string containing the decimal representation of the number.
[`record`]                   | Records are converted to JSON objects. The names and ordering of the fields in the object match the names and ordering of the fields in the record.
[`smallint`]                 | values are converted to JSON numbers.
[`timestamp`][`timestamp with time zone`] | Values are converted to JSON strings containing the number of milliseconds since the Unix epoch.
[`uint2`]                    | Values are converted to JSON numbers.
[`uint4`]                    | Values are converted to JSON numbers.
[`uint8`]                    | Values are converted to JSON numbers.
Other                        | Values are cast to [`text`] and then converted to JSON strings.

## Envelopes

The sink's envelope determines how changes to the sink's underlying relation are
mapped to Kafka messages.

There are two fundamental types of change events:

  * An **insertion** event is the addition of a new row to the underlying
    relation.
  * A **deletion** event is the removal of an existing row from the underlying
    relation.

When a `KEY` is specified, an insertion event and deletion event that occur at
the same time are paired together into a single **update** event that contains
both the old and new value for the given key.

### Upsert

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE UPSERT</code></p>

The upsert envelope:

  * Requires that you specify a unique key for the sink's underlying relation
    using the `KEY` option. See [upsert key selection](#upsert-key-selection)
    for details.
  * For an insertion event, emits the row without additional decoration.
  * For an update event, emits the new row without additional decoration. The
    old row is not emitted.
  * For a deletion event, emits a message with a `null` value (i.e., a
    _tombstone_).

Consider using the upsert envelope if:

  * You need to follow standard Kafka conventions for upsert semantics.
  * You want to enable key-based compaction on the sink's Kafka topic while
    retaining the most recent value for each key.

### Debezium

<p style="font-size:14px"><b>Syntax:</b> <code>ENVELOPE DEBEZIUM</code></p>

The Debezium envelope wraps each event in an object containing a `before` and
`after` field to indicate whether the event was an insertion, deletion, or
update event:

```json
// Insertion event.
{"before": null, "after": {"field1": "val1", ...}}

// Deletion event.
{"before": {"field1": "val1", ...}, "after": null}

// Update event.
{"before": {"field1": "oldval1", ...}, "after": {"field1": "newval1", ...}}
```

Note that the sink will only produce update events if a `KEY` is specified.

Consider using the Debezium envelope if:

  * You have downstream consumers that want update events to contain both the
    old and new value of the row.
  * There is no natural `KEY` for the sink.

## Features

### Automatic topic creation

If the specified Kafka topic does not exist, Materialize will attempt to create
it using the broker's default number of partitions, default replication factor,
default compaction policy, and default retention policy.

If the connection's [progress topic](#exactly-once-processing) does not exist,
Materialize will attempt to create it with a single partition, the broker's
default replication factor, compaction enabled, and both size- and time-based
retention disabled.

To customize a topic's configuration, manually create the topic in Kafka with
the desired configuration (e.g., using the [`kafka-topics.sh`] tool) before
running `CREATE SINK`. If you choose to do so, observe the following guidance:

| Topic          | Configuration       | Guidance
|----------------|---------------------|---------
| Data topic     | Partition count     | Your choice, based on your performance and ordering requirements.
| Data topic     | Replication factor  | Your choice, based on your durability requirements.
| Data topic     | Compaction          | Your choice, based on your downstream applications' requirements. If using the [Upsert envelope](#upsert), enabling compaction is typically the right choice.
| Data topic     | Retention           | Your choice, based on your downstream applications' requirements.
| Progress topic | Partition count     | **Must be set to 1.** Using multiple partitions can cause Materialize to violate its [exactly-once guarantees](#exactly-once-processing).
| Progress topic | Replication factor  | Your choice, based on your durability requirements.
| Progress topic | Compaction          | We recommend enabling compaction to avoid accumulating unbounded state. Disabling compaction may cause performance issues, but will not cause correctness issues.
| Progress topic | Retention           | **Must be disabled.** Enabling retention can cause Materialize to violate its [exactly-once guarantees](#exactly-once-processing).

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

Exactly-once semantics are an end-to-end property of a system, but Materialize only controls the initial produce step. To ensure _end-to-end_ exactly-once message delivery, you should ensure that:

- The broker is configured with replication factor greater than 3, with unclean leader election disabled (`unclean.leader.election.enable=false`).
- All downstream consumers are configured to only read committed data (`isolation.level=read_committed`).
- The consumers' processing is idempotent, and offsets are only committed when processing is complete.

For more details, see [the Kafka documentation](https://kafka.apache.org/documentation/).

## Required permissions

The access control lists (ACLs) on the Kafka cluster must allow Materialize
to perform the following operations on the following resources:

Operation type  | Resource type    | Resource name
----------------|------------------|--------------
Read, Write     | Topic            | Consult `mz_kafka_connections.sink_progress_topic` for the sink's connection
Write           | Topic            | The specified [`TOPIC` option](#connection-options)
Write           | Transactional ID | All transactional IDs beginning with the specified [`TRANSACTIONAL ID PREFIX` option](#connection-options)
Read            | Group            | All group IDs beginning with the specified [`PROGRESS GROUP ID PREFIX` option](#connection-options)

When using [automatic topic creation](#automatic-topic-creation), Materialize
additionally requires access to the following operations:

Operation type   | Resource type    | Resource name
-----------------|------------------|--------------
DescribeConfigs  | Cluster          | n/a
Create           | Topic            | The specified `TOPIC` option

## Troubleshooting

### Upsert key selection

The `KEY` that you specify for an upsert envelope sink must be a unique key of
the sink's underlying relation.

Materialize will attempt to validate the uniqueness of the specified key. If
validation fails, you'll receive an error message like one of the following:

```
ERROR:  upsert key could not be validated as unique
DETAIL: Materialize could not prove that the specified upsert envelope key
("col1") is a unique key of the underlying relation. There are no known
valid unique keys for the underlying relation.

ERROR:  upsert key could not be validated as unique
DETAIL: Materialize could not prove that the specified upsert envelope key
("col1") is a unique key of the underlying relation. The following keys
are known to be unique for the underlying relation:
  ("col2")
  ("col3", "col4")
```

The first error message indicates that Materialize could not prove the existence
of any unique keys for the sink's underlying relation. The second error message
indicates that Materialize could prove that `col2` and `(col3, col4)` were
unique keys of the sink's underlying relation, but could not provide the
uniqueness of the specified upsert key of `col1`.

There are three ways to resolve this error:

* Change the sink to use one of the keys that Materialize determined to be
  unique, if such a key exists and has the appropriate semantics for your
  use case.

* Create a materialized view that deduplicates the input relation by the
  desired upsert key:

  ```sql
  -- For each row with the same key `k`, the `ORDER BY` clause ensures we
  -- keep the row with the largest value of `v`.
  CREATE MATERIALIZED VIEW deduped AS
  SELECT DISTINCT ON (k) v
  FROM original_input
  ORDER BY k, v DESC;

  -- Materialize can now prove that `k` is a unique key of `deduped`.
  CREATE SINK s IN CLUSTER my_io_cluster
  FROM deduped
  INTO KAFKA CONNECTION kafka_connection (TOPIC 't')
  KEY (k)
  FORMAT JSON ENVELOPE UPSERT;
  ```

  {{< note >}}
  Maintaining the `deduped` materialized view requires memory proportional to the
  number of records in `original_input`. Be sure to assign `deduped`
  to a cluster with adequate resources to handle your data volume.
  {{< /note >}}

* Use the `NOT ENFORCED` clause to disable Materialize's validation of the key's
  uniqueness:

  ```sql
  CREATE SINK s IN CLUSTER my_io_cluster
  FROM original_input
  INTO KAFKA CONNECTION kafka_connection (TOPIC 't')
  -- We have outside knowledge that `k` is a unique key of `original_input`, but
  -- Materialize cannot prove this, so we disable its key uniqueness check.
  KEY k NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT;
  ```

  You should only disable this verification if you have outside knowledge of
  the properties of your data that guarantees the uniqueness of the key you
  have specified.

  {{< warning >}}
  If the key is not in fact unique, downstream consumers may not be able to
  correctly interpret the data in the topic, and Kafka key compaction may
  incorrectly garbage collect records from the topic.
  {{< /warning >}}



## Examples

### Creating a connection

A connection describes how to connect and authenticate to an external system you want Materialize to write data to.

Once created, a connection is **reusable** across multiple `CREATE SINK` statements. For more details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection) documentation page.

#### Broker

{{< tabs tabID="1" >}}
{{< tab "SSL">}}

```sql
CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'rp-f00000bar.data.vectorized.cloud:30365',
    SSL KEY = SECRET kafka_ssl_key,
    SSL CERTIFICATE = SECRET kafka_ssl_crt
);
```

{{< /tab >}}
{{< tab "SASL">}}

```sql
CREATE SECRET kafka_password AS '<BROKER_PASSWORD>';

CREATE CONNECTION kafka_connection TO KAFKA (
    BROKER 'unique-jellyfish-0000-kafka.upstash.io:9092',
    SASL MECHANISMS = 'SCRAM-SHA-256',
    SASL USERNAME = 'foo',
    SASL PASSWORD = SECRET kafka_password
);
```

{{< /tab >}}
{{< /tabs >}}

#### Confluent Schema Registry

{{< tabs tabID="1" >}}
{{< tab "SSL">}}

```sql
CREATE SECRET csr_ssl_crt AS '<CSR_SSL_CRT>';
CREATE SECRET csr_ssl_key AS '<CSR_SSL_KEY>';
CREATE SECRET csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_ssl TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://rp-f00000bar.data.vectorized.cloud:30993',
    SSL KEY = SECRET csr_ssl_key,
    SSL CERTIFICATE = SECRET csr_ssl_crt,
    USERNAME = 'foo',
    PASSWORD = SECRET csr_password
);
```

{{< /tab >}}
{{< tab "Basic HTTP Authentication">}}

```sql
CREATE SECRET IF NOT EXISTS csr_username AS '<CSR_USERNAME>';
CREATE SECRET IF NOT EXISTS csr_password AS '<CSR_PASSWORD>';

CREATE CONNECTION csr_basic_http
  FOR CONFLUENT SCHEMA REGISTRY
  URL '<CONFLUENT_REGISTRY_URL>',
  USERNAME = SECRET csr_username,
  PASSWORD = SECRET csr_password;
```

{{< /tab >}}
{{< /tabs >}}

### Creating a sink

#### Upsert envelope

{{< tabs >}}
{{< tab "Avro">}}

```sql
CREATE SINK avro_sink
  IN CLUSTER my_io_cluster
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  KEY (key_col)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< tab "JSON">}}

```sql
CREATE SINK json_sink
  IN CLUSTER my_io_cluster
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_json_topic')
  KEY (key_col)
  FORMAT JSON
  ENVELOPE UPSERT;
```

{{< /tab >}}
{{< /tabs >}}

#### Debezium envelope

{{< tabs >}}
{{< tab "Avro">}}

```sql
CREATE SINK avro_sink
  IN CLUSTER my_io_cluster
  FROM <source, table or mview>
  INTO KAFKA CONNECTION kafka_connection (TOPIC 'test_avro_topic')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
  ENVELOPE DEBEZIUM;
```
{{< /tab >}}
{{< /tabs >}}

#### Documentation comments

Consider the following sink, `docs_sink`, built on top of a relation `t` with
several [SQL comments](/sql/comment-on) attached.

```sql
CREATE TABLE t (key int NOT NULL, value text NOT NULL);
COMMENT ON TABLE t IS 'SQL comment on t';
COMMENT ON COLUMN t.value IS 'SQL comment on t.value';

CREATE SINK docs_sink IN CLUSTER ...
FROM t
INTO KAFKA CONNECTION kafka_connection (TOPIC 'doc-commont-example')
KEY (key)
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection (
    DOC ON TYPE t = 'Top-level comment for container record in both key and value schemas',
    KEY DOC ON COLUMN t.key = 'Comment on column only in key schema',
    VALUE DOC ON COLUMN t.key = 'Comment on column only in value schema'
)
ENVELOPE UPSERT;
```

When `docs_sink` is created, Materialize will publish the following Avro schemas
to the Confluent Schema Registry:

  * Key schema:

    ```json
    {
      "type": "record",
      "name": "row",
      "doc": "Top-level comment for container record in both key and value schemas",
      "fields": [
        {
          "name": "key",
          "type": "int",
          "doc": "Comment on column only in key schema"
        }
      ]
    }
    ```

  * Value schema:

    ```json
    {
      "type": "record",
      "name": "envelope",
      "doc": "Top-level comment for container record in both key and value schemas",
      "fields": [
        {
          "name": "key",
          "type": "int",
          "doc": "Comment on column only in value schema"
        },
        {
          "name": "value",
          "type": "string",
          "doc": "SQL comment on t.value"
        }
      ]
    }
    ```

See [Avro schema documentation](#avro-schema-documentation) for details
about the rules by which Materialize attaches `doc` fields to records.

## Related pages

- [`SHOW SINKS`](/sql/show-sinks)
- [`DROP SINK`](/sql/drop-sink)

[`bigint`]: ../../types/integer
[`boolean`]: ../../types/boolean
[`bytea`]: ../../types/bytea
[`date`]: ../../types/date
[`double precision`]: ../../types/float
[`integer`]: ../../types/integer
[`interval`]: ../../types/interval
[`jsonb`]: ../../types/jsonb
[`map`]: ../../types/map
[`list`]: ../../types/list
[`numeric`]: ../../types/numeric
[`oid`]: ../../types/oid
[`real`]: ../../types/float
[`record`]: ../../types/record
[`smallint`]: ../../types/integer
[`text`]: ../../types/text
[`time`]: ../../types/time
[`uint2`]: ../../types/uint
[`uint4`]: ../../types/uint
[`uint8`]: ../../types/uint
[`timestamp`]: ../../types/timestamp
[`timestamp with time zone`]: ../../types/timestamp
[arrays]: ../../types/array
[`kafka-topics.sh`]: https://docs.confluent.io/kafka/operations-tools/kafka-tools.html#kafka-topics-sh
