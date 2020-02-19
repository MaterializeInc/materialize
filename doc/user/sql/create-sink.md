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
**View** | Stream all changes to the view to the sink. This lets you use Materialize to process a stream, and then stream the processed values.

## Syntax

{{< diagram "create-sink.html" >}}

Field | Use
------|-----
_sink&lowbar;name_ | A name for the sink. This name is only used within Materialize.
_source&lowbar;name_ | The name of the source whose values you want to pass through to the sink.
_sink&lowbar;target_ | The path to write values to the sink.
**WITH (** _option&lowbar;list_ **)** | Additional options for creating a sink. For more detail, see [`WITH` options](#with-options).

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value
------|-----
`schema_registry_url` | If using a Kafka sink, use the Schema Registry at the URL of `value`.

## Detail

- Materialize currently only supports Kafka sinks.

### Kafka sinks

When creating sinks, Materialize expects either:
- You are publishing to a sink that already exists with a schema that matches the sink's source within Materialize
- Your Kafka instances have [`auto.create.topics.enable`](https://kafka.apache.org/documentation/) enabled. This lets Kafka automatically create new topics when it receives messages from topics it hasn't seen before.

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
INTO 'kafka://localhost/quotes-sink'
WITH
    (
        schema_registry_url = 'http://localhost:8081'
    );
```

## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
