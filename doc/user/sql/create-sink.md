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

{{< diagram "create-sink.html" >}}

Field | Use
------|-----
**IF NOT EXISTS** | If specified, _do not_ generate an error if a sink of the same name already exists. <br/><br/>If _not_ specified, throw an error if a sink of the same name already exists. _(Default)_
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
_item&lowbar;name_ | The name of the source or view you want to send to the sink.
**KAFKA BROKER** _host_ | The Kafka broker's host name.
**TOPIC** _topic-prefix_ | The prefix used to generate the Kafka topic name to create and write to.
**CONFLUENT SCHEMA REGISTRY** _url_ | The URL of the Confluent schema registry to get schema information from.
**FILE** _path-prefix_ | The absolute path and file name prefix of file to create and write to.

## Detail

- Materialize currently only supports Avro formatted sinks that write to either a single partition topic or a Avro object container file.
- On each restart, Materialize creates new, distinct topics and files for each sinks. Topic and file names are suffixed with {topic-name}-{sink global-id}-{startup-time}-{nonce}, and file names preserve the given extension.
- Materialize stores information about actual topic names and actual file names in the `mz_kafka_sinks` and `mz_avro_ocf_sinks` log sources.
- Materialize generates Avro schemas for views and sources that are stored in sinks. The generated schemas have a Debezium style diff envelope to capture changes in the input view or source.

### Kafka sinks

When creating Kafka sinks, Materialize uses the Kafka Admin API to create a new topic, and registers its Avro schema in the Confluent Schema Registry.

## Examples

### From sources

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
    CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';
```

### From materialized views

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

### Get actual topic names

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

When creating Avro Object Container File (OCF) sinks, Materialize creates a new sink file using the naming scheme defined above and appends the Avro schema data in its header.

## Examples

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

### From materialized views

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

### Get actual file names

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
