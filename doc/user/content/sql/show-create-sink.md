---
title: "SHOW CREATE SINK"
description: "`SHOW CREATE SINK` returns the statement used to create the sink."
menu:
  main:
    parent: commands
---

`SHOW CREATE SINK` returns the DDL statement used to create the sink.

## Syntax

```sql
SHOW CREATE SINK <sink_name>
```

For available sink names, see [`SHOW SINKS`](/sql/show-sinks).

## Examples

```mzsql
SHOW SINKS
```

```nofmt
     name
--------------
 my_view_sink
```

```mzsql
SHOW CREATE SINK my_view_sink;
```

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
