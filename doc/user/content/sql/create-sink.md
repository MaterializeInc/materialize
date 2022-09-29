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

{{< diagram "create-sink-kafka.svg" >}}

#### `kafka_sink_connection`

{{< diagram "kafka-sink-connection.svg" >}}

#### `sink_format_spec`

{{< diagram "sink-format-spec.svg" >}}

#### `csr_connection`

{{< diagram "csr-connection.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
_item&lowbar;name_ | The name of the source or view you want to send to the sink.
**CONNECTION** _connection_name_ | The name of the connection to use in the sink. For details on creating connections, check the [`CREATE CONNECTION`](../create-connection) documentation page.
**KEY (** _key&lowbar;column_ **)** | An optional list of columns to use for the Kafka key. If unspecified, the Kafka key is left unset.
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](#debezium-envelope-details) to capture changes in the input view or source.
**ENVELOPE UPSERT** | The sink emits data with upsert semantics: updates and inserts for the given key are expressed as a value, and deletes are expressed as a null value payload in Kafka. For more detail, see [Handling upserts](/sql/create-source/kafka/#handling-upserts).

### `CONNECTION` options

Field                | Value type | Description
---------------------|------------|------------
`TOPIC`              | `text`     | The prefix used to generate the Kafka topic name to create and write to.

### CSR `CONNECTION` options

Field                | Value type | Description
---------------------|------------|------------
`AVRO KEY FULLNAME`  | `text`     | Sets the Avro fullname on the generated key schema, if a `KEY` is specified. When used, a value must be specified for `AVRO VALUE FULLNAME`. The default fullname is `row`.
`AVRO VALUE FULLNAME`| `text`     | Default: `envelope`. Sets the Avro fullname on the generated value schema. When `KEY` is specified, `AVRO KEY FULLNAME` must additionally be specified.

### `WITH SNAPSHOT` or `WITHOUT SNAPSHOT`

By default, each `SINK` is created with a `SNAPSHOT` which contains the consolidated results of the
query before the sink was created. Any further updates to these results are produced at the time when
they occur. To only see results after the sink is created, specify `WITHOUT SNAPSHOT`.

## Detail

- Materialize currently only supports Avro or JSON-formatted sinks that write to a Kafka topic.
- Materialize stores information about the sink's topic name in the [`mz_kafka_sinks`](/sql/system-catalog/#mz_kafka_sinks) system table. See the [examples](#examples) below for more details.
- For Avro-formatted sinks, Materialize generates Avro schemas for views and sources that are stored in the sink. If needed, the fullnames for these schemas can be specified with the `AVRO KEY FULLNAME` and `AVRO VALUE FULLNAME` options.

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

You can find the topic name and other metadata for each Kafka sink by querying [`mz_kafka_sinks`](/sql/system-catalog/#mz_kafka_sinks).

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
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'quotes-sink')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE DEBEZIUM;
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
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
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE DEBEZIUM;
```

#### Get Kafka topic names

```sql
SELECT sink_id, name, topic
FROM mz_sinks
JOIN mz_kafka_sinks USING (id);
```

```nofmt
  sink_id  |              name                    |       topic
-----------+--------------------------------------+---------------------
 u5        | materialize.public.quotes_sink       | quotes
 u6        | materialize.public.frank_quotes_sink | frank-quotes
```

### JSON sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection (TOPIC 'quotes-sink')
FORMAT JSON
ENVELOPE DEBEZIUM;
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection (TOPIC 'quotes')
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection;
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
FORMAT JSON
ENVELOPE DEBEZIUM;
```

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`DROP SINK`](../drop-sink)
