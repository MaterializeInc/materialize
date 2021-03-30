---
title: "CREATE SINK"
description: "`CREATE SINK` sends data from Materialize to an external sink."
menu:
  main:
    parent: 'sql'
---

`CREATE SINK` sends data from Materialize to an external sink.

## Conceptual framework

Sinks let you stream data out of Materialize, using either sources or views.

Sink source type | Description
-----------------|------------
**Source** | Simply pass all data received from the source to the sink without modifying it.
**Materialized view** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values. Note that this feature only works with [materialized views](../create-materialized-view), and _does not_ work with [non-materialized views](../create-view).

## Syntax

{{< diagram "create-sink.svg" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
_item&lowbar;name_ | The name of the source or view you want to send to the sink.
**AVRO OCF** _path_ | The absolute path and file name of the Avro Object Container file (OCF) to create and write to. The filename will be modified to let Materialize create a unique file each time Materialize starts, but the file extension will not be modified. You can find more details [here](#avro-ocf-sinks).
**ENVELOPE DEBEZIUM** | The generated schemas have a [Debezium-style diff envelope](#debezium-envelope-details) to capture changes in the input view or source. This is the default.
**ENVELOPE UPSERT** | The sink emits data with upsert semantics: updates and inserts for the given key are expressed as a value, and deletes are expressed as a null value payload in Kafka. For more detail, see [Upsert source details](/sql/create-source/text-kafka/#upsert-envelope-details).

{{< version-changed v0.7.1 >}}
The `AS OF` option was removed.
{{< /version-changed >}}

### Kafka connector

{{< diagram "sink-kafka-connector.svg" >}}

Field | Use
------|-----
**KAFKA BROKER** _host_ | The Kafka broker's host name without the security protocol, which is specified by the [`WITH` options](#with-options).) If you wish to specify multiple brokers (bootstrap servers) as an additional safeguard, use a comma-separated list. For example: `localhost:9092, localhost:9093`.
**TOPIC** _topic&lowbar;prefix_ | The prefix used to generate the Kafka topic name to create and write to.
**WITH OPTIONS (** _option&lowbar;_ **)** | Options affecting sink creation. For more details see [`WITH` options](#with-options).
**CONFLUENT SCHEMA REGISTRY** _url_ | The URL of the Confluent schema registry to get schema information from.
**KEY (** _key&lowbar;column&lowbar;list_ **)** | An optional list of columns to use for the Kafka key. If unspecified, the Kafka key is left unset. {{< version-added v0.5.1 />}}

### `WITH` options

The following options are valid within the `WITH` clause.

Field                | Value type | Description
---------------------|------------|------------
`partition_count`    | `int`      | Set the sink Kafka topic's partition count. This defaults to -1 (use the broker default).
`replication_factor` | `int`      | Set the sink Kafka topic's replication factor. This defaults to -1 (use the broker default).
`consistency`        | `boolean`  | Makes the sink emit additional [consistency metadata](#consistency-metadata). Only valid for Kafka sinks. This defaults to false.

#### SSL `WITH` options

Use the following options to connect Materialize to an SSL-encrypted Kafka
cluster. For more detail, see [SSL-encrypted Kafka details](/sql/create-source/avro-kafka/#ssl-encrypted-kafka-details).

Field | Value | Description
------|-------|------------
`ssl_certificate_location` | `text` | The absolute path to your SSL certificate. Required for SSL client authentication.
`ssl_key_location` | `text` | The absolute path to your SSL certificate's key. Required for SSL client authentication.
`ssl_key_password` | `text` | Your SSL key's password, if any.
`ssl_ca_location` | `text` | The absolute path to the certificate authority (CA) certificate. Used for both SSL client and server authentication. If unspecified, uses the system's default CA certificates.

#### Kerberos `WITH` options

Use the following options to connect Materialize using SASL.

For more detail, see [Kerberized Kafka details](/sql/create-source/avro-kafka/#kerberized-kafka-details).

Field | Value | Description
------|-------|------------
`sasl_mechanism` | `text` | The authentication method used for SASL connections. Required if `security_protocol` is `sasl_plain` or `sasl_ssl`. Supported mechanisms are `gssapi`, `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer`.
`sasl_username` | `text` | Your SASL username. Required if `sasl_mechanism` is `plain`, `scram-sha-256`, or `scram-sha-512`.
`sasl_password` | `text` | Your SASL password. Required if `sasl_mechanism` is `plain`, `scram-sha-256`, or `scram-sha-512`.
`sasl_kerberos_keytab` | `text` | The absolute path to your keytab. Required if `sasl_mechanism` is `gssapi`.
`sasl_kerberos_kinit_cmd` | `text` | Shell command to refresh or acquire the client's Kerberos ticket. Required if `sasl_mechanism` is `gssapi`.
`sasl_kerberos_min_time_before_relogin` | `text` | Minimum time in milliseconds between key refresh attempts. Disable automatic key refresh by setting this property to 0. Required if `sasl_mechanism` is `gssapi`.
`sasl_kerberos_principal` | `text` | Materialize Kerberos principal name. Required if `sasl_mechanism` is `gssapi`.
`sasl_kerberos_service_name` | `text` | Kafka's service name on its host, i.e. the service principal name not including `/hostname@REALM`. Required if `sasl_mechanism` is `gssapi`.

### `WITH SNAPSHOT` or `WITHOUT SNAPSHOT`

By default, each `SINK` is created with a `SNAPSHOT` which contains the consolidated results of the
query before the sink was created. Any further updates to these results are produced at the time when
they occur. To only see results after the sink is created, specify `WITHOUT SNAPSHOT`.

## Detail

- Materialize currently only supports Avro formatted sinks that write to either a topic or an Avro object container file.
- On each restart, Materialize creates new, distinct topics and files for each sink.
- Materialize stores information about actual topic names and actual file names in the `mz_kafka_sinks` and `mz_avro_ocf_sinks` log sources. See the [examples](#examples) below for more details.
- Materialize generates Avro schemas for views and sources that are stored in sinks.
- Materialize can also optionally emit transaction information for changes. This is only supported for Kafka sinks and adds transaction id information inline with the data, and adds a separate transaction metadata topic.

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

When creating Kafka sinks, Materialize uses the Kafka Admin API to create a new topic, and registers its Avro schema in the Confluent Schema Registry. Materialize names the new topic using the format below.
```nofmt
{topic_prefix}-{sink_global_id}-{materialize-startup-time}-{nonce}
```
You can find the topic name for each Kafka sink by querying `mz_kafka_sinks`.

{{% kafka-sink-drop  %}}

#### Consistency metadata

When requested, Materialize will send consistency metadata that describes timestamps (also called transaction IDs) and relates the change data stream to them.

Materialize sends two main pieces of information:
- A timestamp for each change. This is sent inline with the change itself.
- A count of how many changes occurred for each timestamp. This is sent as part of a separate consistency topic.

Materialize uses a simplified version of the Debezium [transaction metadata](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-transaction-metadata) protocol to send this information.
The generated [diff envelope](#debezium-envelope-details) schema used for data messages is decorated with a `transaction` field which has the following schema.

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
Each message sent to Kafka has a `transaction` field, along with a `transaction_id`, in addition to it's regular `before` / `after` data fields. The `transaction_id` is equivalent to the Materialize timestamp for this record.

In addition to the inline information, Materialize creates a new "consistency topic" that stores counts of how many changes were generated per `transaction_id`. This consistency topic is named using the format below.
```nofmt
{topic_prefix}-{sink_global_id}-{materialize-startup-time}-{nonce}-consistency
```

Each message in the consistency topic has the schema below.
```
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
        }
    ]
}
```

Field | Use
------|-----
_id_ | The transaction id this record refers to.
_status_ | Either `BEGIN` or `END`. Materialize sends a record with `BEGIN` the first time it writes a data message for `id`, and it sends a `END` record after it has written all data messages for `id`.
_event&lowbar;count_ | This field is null for `BEGIN` records, and for `END` records it contains the number of messages Materialize wrote for that `id`.

##### Consistency information details
- Materialize writes consistency output to a different topic per sink.
- There are no ordering guarantees on transaction IDs in the consistency topic.
- Multiple transactions can be interleaved in the consistency topic. In other words, there can be multiple transaction IDs that have a `BEGIN` record but no corresponding `END` record simultaneously.

### Avro OCF sinks

When creating Avro Object Container File (OCF) sinks, Materialize creates a new sink OCF and appends the Avro schema data in its header. Materialize names the new file using the format below.
```nofmt
{path.base_directory}-{path.file_stem}-{sink_global_id}-{materialize-startup_time}-{nonce}-{path.file_extension}
```
You can query `mz_avro_ocf_sinks` to get file name information for each Avro OCF sink. Look [here](#avro-ocf-sinks-1) for a more concrete example.

## Examples

### Kafka sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA BROKER 'localhost' TOPIC 'quotes'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO KAFKA BROKER 'localhost' TOPIC 'quotes-sink'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081' AS OF now();
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA BROKER 'localhost' TOPIC 'quotes'
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
INTO KAFKA BROKER 'localhost' TOPIC 'frank-quotes-sink'
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

### Avro OCF sinks

#### From sources

```sql
CREATE SOURCE quotes
FROM KAFKA BROKER 'localhost' TOPIC 'quotes'
FORMAT AVRO USING
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE SINK quotes_sink
FROM quotes
INTO AVRO OCF '/path/to/sink-file.ocf;'
```

#### From materialized views

```sql
CREATE SOURCE quotes
FROM KAFKA BROKER 'localhost' TOPIC 'quotes'
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
INTO AVRO OCF '/path/to/frank-sink-file.ocf;'
```

#### Get actual file names

Materialize stores the actual path as a byte array so we need to use the `convert_from` function to convert it to a UTF-8 string.

```sql
SELECT sink_id, name, convert_from(path, 'utf8')
FROM mz_sinks
JOIN mz_avro_ocf_sinks ON mz_sinks.id = mz_avro_ocf_sinks.sink_id
```

```nofmt
 sink_id   |        name       |                           path
-----------+-------------------+----------------------------------------------------------------
 u10       | quotes_sink       | /path/to/sink-file-u10-1586108399-8671224166353132585.ocf
 u11       | frank_quotes_sink | /path/to/frank-sink-file-u11-1586108399-8671224166353132585.ocf
```

## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
