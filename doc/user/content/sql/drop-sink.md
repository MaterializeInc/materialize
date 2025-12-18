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

```mzsql
DROP SINK [IF EXISTS] <sink_name>;
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified sink does not exist.
`<sink_name>` | The sink you want to drop. You can find available sink names through [`SHOW SINKS`](../show-sinks).

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
