---
title: "DROP SINK"
description: "`DROP SINK` removes a sink from your Materialize instances."
draft: true
#menu:
  #main:
    #parent: commands
---

`DROP SINK` removes a sink from your Materialize instances.

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

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
