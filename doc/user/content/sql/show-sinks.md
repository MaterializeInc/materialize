---
title: "SHOW SINKS"
description: "`SHOW SINKS` returns a list of all sinks available to your Materialize instances."
menu:
  main:
    parent: 'sql'
aliases:
    - /sql/show-sink
---

`SHOW SINKS` returns a list of all sinks available to your Materialize instances.

## Syntax

{{< diagram "show-sinks.svg" >}}

## Details

### Output format

`SHOW SINKS`'s output is a table with one column, `name`.

{{< version-changed v0.5.0 >}}
The output column is renamed from `SINKS` to `name`.
{{< /version-changed >}}

## Examples

```sql
SHOW SINKS;
```
```nofmt
my_sink
```

## Related pages

- [`CREATE SINK`](../create-sink)
- [`DROP SINK`](../drop-sink)
