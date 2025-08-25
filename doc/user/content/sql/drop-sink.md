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

```mzsql
SHOW SINKS;
```
```nofmt
my_sink
```
```mzsql
DROP SINK my_sink;
```
```nofmt
DROP SINK
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/drop-sink.md" >}}

## Related pages

- [`SHOW SINKS`](../show-sinks)
- [`CREATE SINK`](../create-sink)
- [`DROP OWNED`](../drop-owned)
