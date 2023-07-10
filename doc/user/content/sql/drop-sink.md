---
title: "DROP SINK"
description: "`DROP SINK` removes a sink from Materialize."
menu:
  main:
    parent: commands
---

`DROP SINK` removes a sink from Materialize.

{{% kafka-sink-drop  %}}

## Syntax

{{< diagram "drop-sink.svg" >}}

Field | Use
------|-----
_sink&lowbar;name_ | The sink you want to drop. You can find available sink names through [`SHOW SINKS`](../show-sinks).

## Examples

```sql
SHOW SINKS;
```
```nofmt
my_sink
```
```sql
DROP SINK my_sink;
```
```nofmt
DROP SINK
```

## Privileges

{{< alpha />}}

The privileges required to execute this statement are:

- Ownership of the dropped sink.
- `USAGE` privileges on the containing schema.

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
- [DROP OWNED](../drop-owned)
