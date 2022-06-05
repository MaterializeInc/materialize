---
title: "SHOW CREATE SINK"
description: "`SHOW CREATE SINK` returns the statement used to create the sink."
menu:
  main:
    parent: commands
---

`SHOW CREATE SINK` returns the DDL statement used to create the sink.

## Syntax

{{< diagram "show-create-sink.svg" >}}

Field | Use
------|-----
_sink&lowbar;name_ | The sink you want use. You can find available sink names through [`SHOW SINKS`](../show-sinks).

## Examples

```sql
SHOW SINKS
```

```nofmt
     name
--------------
 my_view_sink
```

```sql
SHOW CREATE SINK my_view_sink;
```

```nofmt
               Sink              |                                                                                                        Create Sink
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.my_view_sink | CREATE SINK "materialize"."public"."my_view_sink" FROM "materialize"."public"."my_view" INTO KAFKA BROKER 'localhost:9092' TOPIC 'my_view_sink' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081'
```

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
