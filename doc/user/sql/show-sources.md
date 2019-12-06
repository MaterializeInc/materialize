---
title: "SHOW SOURCES"
description: "`SHOW SOURCES` returns a list of all sources available to your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`SHOW SOURCES` returns a list of all sources available to your Materialize instances.

## Syntax

{{< diagram "show-sources.html" >}}

## Details

Materialize maintains a number of views on your behalf, all of which begin with the name
`mz_`, e.g. `mz_dataflow_channels`.

All other sources that display are sources you added to your instances.

## Examples

```sql
SHOW SOURCES;
```
```bash
mz_dataflow_channels
mz_view_dependencies
mz_views
...
my_source
```

## Related pages

- [`SHOW CREATE SOURCE`](../show-create-source)
- [`CREATE SOURCE`](../create-source)
