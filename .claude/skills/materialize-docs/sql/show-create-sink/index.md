---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-sink/
complexity: intermediate
description: '`SHOW CREATE SINK` returns the statement used to create the sink.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE SINK
- SHOW CREATE
product_area: Sinks
status: stable
title: SHOW CREATE SINK
---

# SHOW CREATE SINK

## Purpose
`SHOW CREATE SINK` returns the statement used to create the sink.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE SINK` returns the statement used to create the sink.



`SHOW CREATE SINK` returns the DDL statement used to create the sink.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE SINK <sink_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available sink names, see [`SHOW SINKS`](/sql/show-sinks).

## Examples

This section covers examples.

```mzsql
SHOW SINKS
```text

```nofmt
     name
--------------
 my_view_sink
```text

```mzsql
SHOW CREATE SINK my_view_sink;
```text

```nofmt
               name              |                                                                                                        create_sql
---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.my_view_sink | CREATE SINK "materialize"."public"."my_view_sink" IN CLUSTER "c" FROM "materialize"."public"."my_view" INTO KAFKA CONNECTION "materialize"."public"."kafka_conn" (TOPIC 'my_view_sink') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection ENVELOPE DEBEZIUM
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the sink.


## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)

