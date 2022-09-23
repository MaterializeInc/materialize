---
title: "CREATE SINK"
description: "`CREATE SINK` sends data from Materialize to an external sink."
pagerank: 10
menu:
  # This should also have a "non-content entry" under Connect, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE SINK` sends data from Materialize to an external sink.

## Conceptual framework

Sinks let you stream data out of Materialize, using either sources or views.

Sink source type | Description
-----------------|------------
**Source** | Simply pass all data received from the source to the sink without modifying it.
**Table** | Stream all changes to the specified table out to the sink.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](../create-materialized-view), and _does not_ work with [non-materialized views](../create-view).

## Syntax

{{< diagram "create-sink.svg" >}}

#### `sink_kafka_connector`

{{< diagram "sink-kafka-connector.svg" >}}

#### `sink_format_spec`

{{< diagram "sink-format-spec.svg" >}}

#### `consistency_format_spec`

{{< diagram "consistency-format-spec.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
_item&lowbar;name_ | The name of the source or view you want to send to the sink.
**KAFKA CONNECTION** _conn_ | The Kafka [connection](../create-connection) where you want to sink data.
**TOPIC** _topic&lowbar;prefix_ | The prefix used to generate the Kafka topic name to create and write to.
**KEY (** _key&lowbar;column_ **)** | An optional list of columns to use for the Kafka key. If unspecified, the Kafka key is left unset.
_sink&lowbar;with&lowbar;options_ | Options affecting sink creation. For more detail, see [`WITH` options](#with-options).
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](#debezium-envelope-details) to capture changes in the input view or source. This is the default.
**ENVELOPE UPSERT** | The sink emits data with upsert semantics: updates and inserts for the given key are expressed as a value, and deletes are expressed as a null value payload in Kafka. For more detail, see [Handling upserts](/sql/create-source/kafka/#handling-upserts).

### `WITH` options

The following options are valid within the `WITH` clause.

Field                | Value type | Description
---------------------|------------|------------
`partition_count`    | `int`      | Set the sink Kafka topic's partition count. This defaults to -1 (use the broker default).
`replication_factor` | `int`      | Set the sink Kafka topic's replication factor. This defaults to -1 (use the broker default).
`acks`               | `text`     | Sets the number of Kafka replicas that must acknowledge Materialize writes. Accepts values [-1,1000]. `-1` (the default) specifies all replicas.
`retention_ms`       | `long`     | Sets the maximum time Kafka will retain a log.  Accepts values [-1, ...]. `-1` specifics no time limit.  If not set, uses the broker default.
`retention_bytes`    | `long`     | Sets the maximum size a Kafka partition can grow before removing old logs.  Accepts values [-1, ...]. `-1` specifics no size limit.  If not set, uses the broker default.
`avro_key_fullname`  | `text`     | Sets the Avro fullname on the generated key schema, if a `KEY` is specified. When used, a value must be specified for `avro_value_fullname`. The default fullname is `row`.
`avro_value_fullname`| `text`     | Sets the Avro fullname on the generated value schema. When `KEY` is specified, `avro_key_fullname` must additionally be specified. The default fullname is `envelope`.

### `WITH SNAPSHOT` or `WITHOUT SNAPSHOT`

By default, each `SINK` is created with a `SNAPSHOT` which contains the consolidated results of the
query before the sink was created. Any further updates to these results are produced at the time when
they occur. To only see results after the sink is created, specify `WITHOUT SNAPSHOT`.

## Detail

- Materialize currently only supports Avro or JSON-formatted sinks that write to a Kafka topic.
- Materialize stores information about actual topic names in the `mz_kafka_sinks` log sources. See the [examples](#examples) below for more details.
- For Avro-formatted sinks, Materialize generates Avro schemas for views and sources that are stored in the sink. If needed, the fullnames for these schemas can be specified with the `avro_key_fullname` and `avro_value_fullname` options.

### Debezium envelope details

The Debezium envelope provides a "diff envelope", which describes the decoded
records' old and new values; this is roughly equivalent to the notion of Change
Data Capture, or CDC. Materialize can write the data in this diff envelope to
represent inserts, updates, or deletes to the underlying source or view being
written for the sink.

#### Format implications

Using the Debezium envelope changes the schema of your Avro-encoded data
to include something akin to the following field:

```json
{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
        "name": "before",
        "type": [
            {
            "name": "row",
            "type": "record",
            "fields": [
                {"name": "a", "type": "long"},
                {"name": "b", "type": "long"}
            ]
            },
            "null"
        ]
        },
        { "name": "after", "type": ["row", "null"] }
    ]
}
```

Note that:

- You don't need to manually create this schema. Materialize generates it for you.
- The following section depends on the column's names and types, and is unlikely
  to match our example:
    ```json
    ...
    "fields": [
            {"name": "a", "type": "long"},
            {"name": "b", "type": "long"}
        ]
    ...
    ```


### Kafka sinks

When sinking into Kafka, Materialize will write all the changes from the specified source, table, or materialized view into the topic you specify.
If the topic does not exist, Materialize will use the Kafka Admin API to create the topic.

For Avro-encoded sinks, Materialize will publish the sink's Avro schema to the Confluent Schema Registry. Materialize will not publish schemas for JSON-encoded sinks.

You can find the topic name and other metadata for each Kafka sink by querying `mz_kafka_sinks`.

{{< note >}}
{{% kafka-sink-drop  %}}
{{</ note >}}

To achieve its exactly-once processing guarantees, Materialize needs to store some internal metadata in an additional *progress topic*. This topic is shared among all sinks that use a particular Kafka connection. The name of this progress topic can be specified when [creating a connection](/sql/create-connection); otherwise, a default is chosen based on the Materialize environment `id` and the connection `id`. In either case, Materialize will attempt to create the topic if it does not exist. The contents of this topic are not user-specified.

## Examples

### Avro sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'quotes-sink')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE MATERIALIZED VIEW frank_quotes AS
    SELECT * FROM quotes
    WHERE attributed_to = 'Frank McSherry';
```
```sql
CREATE SINK frank_quotes_sink
FROM frank_quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'frank-quotes-sink')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

#### Get actual Kafka topic names

```sql
SELECT sink_id, name, topic
FROM mz_sinks
JOIN mz_kafka_sinks ON mz_sinks.id = mz_kafka_sinks.sink_id
```

```nofmt
  sink_id  |              name                    |                        topic
-----------+--------------------------------------+------------------------------------------------------
 u5        | materialize.public.quotes_sink       | quotes-sink-u6-1586024632-15401700525642547992
 u6        | materialize.public.frank_quotes_sink | frank-quotes-sink-u5-1586024632-15401700525642547992
```

### JSON sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'quotes-sink')
FORMAT JSON;
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE MATERIALIZED VIEW frank_quotes AS
    SELECT * FROM quotes
    WHERE attributed_to = 'Frank McSherry';
```
```sql
CREATE SINK frank_quotes_sink
FROM frank_quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'frank-quotes-sink')
FORMAT JSON;
```


## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
