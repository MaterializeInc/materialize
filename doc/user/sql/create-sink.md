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

### From sources

```sql
CREATE SOURCE quotes
FROM 'kafka://localhost/quotes'
USING SCHEMA REGISTRY 'http://localhost:8081';
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

### From materialized views

```sql
CREATE SOURCE quotes
FROM 'kafka://localhost/quotes'
USING SCHEMA REGISTRY 'http://localhost:8081';
```
```sql
CREATE MATERIALIZED VIEW frank_quotes AS
    SELECT * FROM quotes
    WHERE attributed_to = 'Frank McSherry';
```
```sql
CREATE SINK frank_quotes_sink
FROM frank_quotes
INTO 'kafka://localhost/quotes-sink'
WITH
    (
        schema_registry_url = 'http://localhost:8081'
    );
```

## Related pages

- [`SHOW SINK`](../show-sinks)
- [`DROP SINK`](../drop-sink)
