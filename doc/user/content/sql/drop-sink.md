---
title: "DROP SINK"
description: "`DROP SINK` removes a sink from your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`DROP SINK` removes a sink from your Materialize instances.

{{% kafka-sink-drop  %}}

## Syntax

```sql
DROP SINK [ IF EXISTS ] sink_name
```

<br/>
<details>
<summary>Diagram</summary>
<br>

{{< diagram "drop-sink.svg" >}}

</details>
<br/>

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
