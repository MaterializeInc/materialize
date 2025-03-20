---
title: "EXPLAIN SCHEMA"
description: "`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` is used to see the generated schemas for a `CREATE SINK` statement"
menu:
  main:
    parent: commands
---

`EXPLAIN KEY SCHEMA` or `EXPLAIN VALUE SCHEMA` shows the generated schemas for a `CREATE SINK` statement without creating the sink.

{{< warning >}}
`EXPLAIN` is not part of Materialize's stable interface and is not subject to
our backwards compatibility guarantee. The syntax and output of `EXPLAIN` may
change arbitrarily in future versions of Materialize.
{{< /warning >}}

## Syntax

{{< diagram "explain-schema.svg" >}}

#### `sink_definition`

{{< diagram "sink-definition.svg" >}}

### Output format

Only `JSON` can be specified as the output format.

Output type | Description
------|-----
**JSON** | Format the explanation output as a JSON object.

## Details
When creating a an Avro-formatted Kafka sink, Materialize automatically generates Avro schemas for the message key and value and publishes them to a schema registry.
This command shows what the generated schemas would look like, without creating the sink.

## Examples

```mzsql
CREATE TABLE t (c1 int, c2 text);
COMMENT ON TABLE t IS 'materialize comment on t';
COMMENT ON COLUMN t.c2 IS 'materialize comment on t.c2';

EXPLAIN VALUE SCHEMA FOR
  CREATE SINK
  FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'test_avro_topic')
  KEY (c1)
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
```

```
                   Schema
--------------------------------------------
 {                                         +
   "type": "record",                       +
   "name": "envelope",                     +
   "doc": "materialize comment on t",      +
   "fields": [                             +
     {                                     +
       "name": "c1",                       +
       "type": [                           +
         "null",                           +
         "int"                             +
       ]                                   +
     },                                    +
     {                                     +
       "name": "c2",                       +
       "type": [                           +
         "null",                           +
         "string"                          +
       ],                                  +
       "doc": "materialize comment on t.c2"+
     }                                     +
   ]                                       +
 }
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all items in the query are contained in.
