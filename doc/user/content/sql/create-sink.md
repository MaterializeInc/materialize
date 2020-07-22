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
_timestamp&lowbar;expression_ | The logical time to tail from onwards (either a number of milliseconds since the Unix epoch, or a `TIMESTAMP` or `TIMESTAMPTZ`).

### Kafka connector

{{< diagram "kafka-connector.svg" >}}

Field | Use
------|-----
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic&lowbar;prefix_ | The prefix used to generate the Kafka topic name to create and write to.
**WITH OPTIONS (** _option&lowbar;_ **)** | Options affecting sink creation. For more details see [`WITH` options](#with-options).
**CONFLUENT SCHEMA REGISTRY** _url_ | The URL of the Confluent schema registry to get schema information from.

### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value type | Description
------|------------|------------
`replication_factor` | `int` | Set the sink Kafka topic's replication factor. This defaults to 1.

### AS OF

`AS OF` is the specific point in time to start emitting all events for a given `SINK`. If you don't
use `AS OF`, Materialize will pick a timestamp itself.

### WITH SNAPSHOT or WITHOUT SNAPSHOT

By default, each `SINK` is created with a `SNAPSHOT` which contains the results of the query at its `AS OF` timestamp.
Any further updates to these results are produced at the time when they occur. To only see results after the
`AS OF` timestamp, specify `WITHOUT SNAPSHOT`.

## Detail

- Materialize currently only supports Avro formatted sinks that write to either a single partition topic or a Avro object container file.
- On each restart, Materialize creates new, distinct topics and files for each sink.
- Materialize stores information about actual topic names and actual file names in the `mz_kafka_sinks` and `mz_avro_ocf_sinks` log sources. See the [examples](#examples) below for more details.
- Materialize generates Avro schemas for views and sources that are stored in sinks. The generated schemas have a [Debezium-style diff envelope](/overview/api-components/#envelopes) to capture changes in the input view or source.

### Kafka sinks

When creating Kafka sinks, Materialize uses the Kafka Admin API to create a new topic, and registers its Avro schema in the Confluent Schema Registry. Materialize names the new topic using the format below.
```nofmt
{topic_prefix}-{sink_global_id}-{materialize-startup-time}-{nonce}
```
You can find the topic name for each Kafka sink by querying `mz_kafka_sinks`.

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
SELECT * FROM mz_catalog_names NATURAL JOIN mz_kafka_sinks;
```

```nofmt
 global_id |              name                    |                        topic
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
SELECT global_id, name, convert_from(path, 'utf8') FROM  mz_catalog_names NATURAL JOIN mz_avro_ocf_sinks;
```

```nofmt
 global_id |                 name                 |                           path
-----------+--------------------------------------+----------------------------------------------------------------
 u10       | materialize.public.quotes_sink       | /path/to/sink-file-u10-1586108399-8671224166353132585.ocf
 u11       | materialize.public.frank_quotes_sink | /path/to/frank-sink-file-u11-1586108399-8671224166353132585.ocf
```

## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
