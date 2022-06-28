---
title: "SHOW SINKS"
description: "`SHOW SINKS` returns a list of all sinks available to your Materialize instances."
draft: true
#menu:
  #main:
    #parent: commands
aliases:
    - /sql/show-sink
---

`SHOW SINKS` returns a list of all sinks available to your Materialize instances.

## Syntax

{{< diagram "show-sinks.svg" >}}

## Details

Field | Use
------|-----
_schema&lowbar;name_ | The schema to show sinks from. Defaults to `public` in the current database. For available schemas, see [`SHOW SCHEMAS`](../show-schemas).
**FULL** | Return details about your sinks.

### Output format

`SHOW FULL SINKS`'s output is a table, with this structure:

```nofmt
cluster | name  | type
--------+-------+-------
...     | ...   | ...
```

Field | Meaning
------|--------
**cluster** | The name of the [cluster](/overview/key-concepts/#clusters) containing the sink.
**name** | The name of the sink.
**type** | Whether the sink was created by the `user` or the `system`.

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
