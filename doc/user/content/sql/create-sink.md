---
title: "CREATE SINK"
description: "`CREATE SINK` sends data from Materialize to an external sink."
pagerank: 10
draft: true
#menu:
  # This should also have a "non-content entry" under Connect, which is
  # configured in doc/user/config.toml
  #main:
    #parent: 'commands'
---

[//]: # "NOTE(morsapaes) Once we're ready to bring sinks back, check #13104 to restore the previous docs state."

`CREATE SINK` sends data from Materialize to an external sink.

## Conceptual framework

Sinks let you stream data out of Materialize, using either sources or views.

Sink source type | Description
-----------------|------------
**Source** | Simply pass all data received from the source to the sink without modifying it.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](../create-materialized-view), and _does not_ work with [non-materialized views](../create-view).

### Sinks + clusters

Materialize maintains sinks using dataflows. Each dataflow must belong to a
[cluster](/overview/key-concepts#clusters).

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
_cluster_name_ | The [cluster](/sql/create-cluster) to maintain this sink. If not provided, uses the session's `cluster` variable.
_item&lowbar;name_ | The name of the source or view you want to send to the sink.
**KAFKA CONNECTION** _conn_ | The Kafka [connection](../create-connection) where you want to sink data.
**TOPIC** _topic&lowbar;prefix_ | The prefix used to generate the Kafka topic name to create and write to.
**KEY (** _key&lowbar;column_ **)** | An optional list of columns to use for the Kafka key. If unspecified, the Kafka key is left unset.
**TOPIC** _consistency&lowbar;topic_ | Makes the sink emit additional [consistency metadata](#consistency-metadata) to the named topic. Only valid for Kafka sinks. If `reuse_topic` is `true`, a default naming convention will be used when the topic name is not explicitly set. This is formed by appending `-consistency` to the output topic name.
_sink&lowbar;with&lowbar;options_ | Options affecting sink creation. For more detail, see [`WITH` options](#with-options).
_with&lowbar;options_ | Options affecting Materialize's connection to Kafka. For more detail, see [Authentication](#authentication).
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](#debezium-envelope-details) to capture changes in the input view or source. This is the default.
**ENVELOPE UPSERT** | The sink emits data with upsert semantics: updates and inserts for the given key are expressed as a value, and deletes are expressed as a null value payload in Kafka. For more detail, see [Handling upserts](/sql/create-source/kafka/#handling-upserts).

### `WITH` options

The following options are valid within the `WITH` clause.

Field                | Value type | Description
---------------------|------------|------------
`partition_count`    | `int`      | Set the sink Kafka topic's partition count. This defaults to -1 (use the broker default).
`replication_factor` | `int`      | Set the sink Kafka topic's replication factor. This defaults to -1 (use the broker default).
`reuse_topic`        | `bool`     | Use the existing Kafka topic after Materialize restarts, instead of creating a new one. The default is false. See [Enabling topic reuse after restart](/sql/create-sink/#exactly-once-sinks-with-topic-reuse-after-restart) for details.
`security_protocol`  | `text`     | Use [`ssl`](#authentication) or, for [Kerberos](#authentication), `sasl_plaintext`, `sasl-scram-sha-256`, or `sasl-sha-512` to connect to the Kafka cluster.
`acks`               | `text`     | Sets the number of Kafka replicas that must acknowledge Materialize writes. Accepts values [-1,1000]. `-1` (the default) specifies all replicas.
`retention_ms`       | `long`     | Sets the maximum time Kafka will retain a log.  Accepts values [-1, ...]. `-1` specifics no time limit.  If not set, uses the broker default.
`retention_bytes`    | `long`     | Sets the maximum size a Kafka partition can grow before removing old logs.  Accepts values [-1, ...]. `-1` specifics no size limit.  If not set, uses the broker default.
`avro_key_fullname`  | `text`     | Sets the Avro fullname on the generated key schema, if a `KEY` is specified. When used, a value must be specified for `avro_value_fullname`. The default fullname is `row`.
`avro_value_fullname`| `text`     | Sets the Avro fullname on the generated value schema. When `KEY` is specified, `avro_key_fullname` must additionally be specified. The default fullname is `envelope`.

#### Authentication

Kafka sinks support the same authentication scheme and options as Kafka sources (`SSL`, `SASL`).
Check the [Kafka source documentation](/sql/create-source/kafka/#authentication) for more details and examples.

### `WITH SNAPSHOT` or `WITHOUT SNAPSHOT`

By default, each `SINK` is created with a `SNAPSHOT` which contains the consolidated results of the
query before the sink was created. Any further updates to these results are produced at the time when
they occur. To only see results after the sink is created, specify `WITHOUT SNAPSHOT`.

## Detail

- Materialize currently only supports Avro or JSON-formatted sinks that write to a Kafka topic.
- For most sinks, Materialize creates new, distinct topics for each sink on restart. A beta feature enables the use of the same topic after restart. For details, see [Exactly-once sinks](#exactly-once-sinks-with-topic-reuse-after-restart).
- Materialize stores information about actual topic names in the `mz_kafka_sinks` log sources. See the [examples](#examples) below for more details.
- For Avro-formatted sinks, Materialize generates Avro schemas for views and sources that are stored in the sink. If needed, the fullnames for these schemas can be specified with the `avro_key_fullname` and `avro_value_fullname` options.
- Materialize can also optionally emit transaction information for changes. This is only supported for Kafka sinks and adds transaction information inline with the data, and adds a separate transaction metadata topic.

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

When creating sinks, Materialize will either reuse the last sink topic (if `reuse_topic` is `true`) or it will generate a new topic name using the format below.
```nofmt
{topic_prefix}-{sink_global_id}-{materialize-startup-time}-{nonce}
```
If the topic does not exist, Materialize will use the Kafka Admin API to create the topic.

For Avro-encoded sinks, Materialize will publish the sink's Avro schema to the Confluent Schema Registry. Materialize will not publish schemas for JSON-encoded sinks.

You can find the topic name for each Kafka sink by querying `mz_kafka_sinks`.

{{< note >}}
{{% kafka-sink-drop  %}}
{{</ note >}}

#### Exactly-once sinks (with topic reuse after restart)

{{< beta />}}

By default, Materialize creates new, distinct topics for sinks on restart. To enable the reuse of an existing topic instead and achieve exactly-once processing guarantees, Materialize must:

* **Determine the point in time where it left off processing**

   Each sink writes an additional [consistency topic](#consistency-metadata) with useful metadata that allows Materialize to keep track of the timestamps it has published data for. Following a crash, this consistency topic is used to determine the latest complete timestamp for a sink and resume processing.

* **Reconstruct the history of the sink and all the objects it depends on**

   For each source, timestamp/offset bindings (i.e. which timestamps were assigned to which offsets) are persisted to the on-disk [system catalog](/sql/system-catalog/). This ensures that Materialize can preserve correctness on restart and avoid publishing duplicate data to the sink.

    {{< note >}}
For some deployment setups, you may need to persist the system catalog to stable storage across restarts. Without that metadata, new timestamps will be reassigned to existing offsets and data **will be republished** to the sink.
   {{</ note >}}

In practice, each incoming event affects the final results exactly once, even if the stream is disrupted or Materialize is restarted. Because the implementation effectively relies on source events having replayable timestamps, only **Kafka sources** and materialized views defined on top of Kafka sources can be used in this context.

**Syntax**

```sql
CREATE CONNECTION kafka_connection
  FOR KAFKA
    BROKER 'localhost:9092';

CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection TOPIC 'quotes-eo-sink'
CONSISTENCY (TOPIC 'quotes-eo-sink-consistency'
             FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081')
WITH (reuse_topic=true)
FORMAT JSON;
```

To create a sink with exactly-once processing guarantees, you need to:

* Set the `reuse_topic` option to `true`;
* Optionally name the consistency topic. This name must be unique across all sinks in the Materialize instance. If not specified, a default name will be created by appending `-consistency` to the sink topic name.
* Specify the [format](/sql/create-sink/#consistency_format_spec) of the consistency topic. Only Avro is supported. If you are using a JSON-formatted sink, you must use Avro as well (at least for now!).

Note that:

* The sink consistency topic shouldn't be written to by any other process, including other sinks or Materialize instances;
* Key-based compaction is supported for the consistency topic and can be useful for controlling the growth of the topic.

This feature is still in beta, so we strongly recommend that you start with test data, rather than with production. Please [let us know](https://github.com/MaterializeInc/materialize/issues/new/choose) if you run into any issues!

#### Consistency metadata

When requested, Materialize will produce consistency metadata that describes timestamps and relates the change data stream to them.

Materialize sends two main pieces of information:
- A timestamp for each change. This is sent inline with the change itself.
- A count of how many changes occurred for each timestamp. This is sent as part of a separate consistency topic.
- A count of how many changes occurred for each collection covered by the topic.
  In Materialize, this will always correspond to the above count as each topic
  uniquely maps to a single collection.

Materialize uses a simplified version of the Debezium [transaction metadata](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata) protocol to send this information.
The generated [diff envelope](#debezium-envelope-details) schema used for data messages is decorated with a `transaction` field which has the following schema:

```
{
    "name": "transaction",
    "type": {
        "type": "record",
        "name": "transaction_metadata",
        "fields": [
            {
                "name": "id",
                "type": "string"
            }
        ]
    }
}
```
Each message sent to Kafka has a `transaction` field and a transaction `id`, in addition to it's regular `before` / `after` data fields. The transaction `id` is equivalent to the Materialize timestamp for this record.

In addition to the inline information, Materialize creates an additional consistency topic that stores counts of how many changes were generated per transaction `id`. The name of the consistency topic uses the following convention:

```nofmt
{consistency_topic_prefix}-{sink_global_id}-{materialize-startup-time}-{nonce}
```

{{< note >}}
If `reuse_topic` is enabled, the naming convention is ignored. Instead, the topic name specified via the `CONSISTENCY` option is used.
{{</ note >}}

Each message in the consistency topic has the schema below.

```json
{
    "type": "record",
    "name": "envelope",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "status",
            "type": "string"
        },
        {
            "name": "event_count",
            "type": [
                null,
                "long"
            ]
        },
        {
            "name": "data_collections",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "data_collection",
                        "fields": [
                            {
                                "name": "data_collection",
                                "type": "string"
                            },
                            {
                                "name": "event_count",
                                "type": "long"
                            },
                        ]
                    }
                }
            ],
            "default": null,
        }
    ]
}
```

Field | Use
------|-----
_id_ | The transaction `id` this record refers to.
_status_ | Either `BEGIN` or `END`. Materialize sends a record with `BEGIN` the first time it writes a data message for `id`, and an `END` record after it has written all data messages for `id`.
_event&lowbar;count_ | This field is null for `BEGIN` records, and for `END` records it contains the number of messages Materialize wrote for that `id`.
_data&lowbar;collections_ | This field is null for `BEGIN` records, and for `END` records it contains the number of messages Materialize wrote for that `id` and collection.

##### Consistency information details
- Materialize writes consistency output to a different topic per sink.
- There are no ordering guarantees on transaction `id` in the consistency topic.
- Multiple transactions can be interleaved in the consistency topic, so it's possible that some `ids` don't have a corresponding `BEGIN` or `END` record.

## Examples

### Avro sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection TOPIC 'quotes'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection TOPIC 'quotes-sink'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection TOPIC 'quotes'
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
INTO KAFKA CONNECTION kafka_connection TOPIC 'frank-quotes-sink'
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
FROM KAFKA CONNECTION kafka_connection TOPIC 'quotes'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA CONNECTION kafka_connection TOPIC 'quotes-sink'
FORMAT JSON;
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA CONNECTION kafka_connection TOPIC 'quotes'
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
INTO KAFKA CONNECTION kafka_connection TOPIC 'frank-quotes-sink'
FORMAT JSON;
```


## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
