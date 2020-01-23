---
title: "DROP SINK"
description: "`DROP SINK` removes a sink from your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`DROP SINK` removes a sink from your Materialize instances.

## Syntax

{{< diagram "drop-sink.html" >}}

Field | Use
------|-----
_sink&lowbar;name_ | The view you sink to drop. You can find available view names through [`SHOW SINKS`](../show-sinks).

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
