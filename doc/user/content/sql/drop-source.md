---
title: "DROP SOURCE"
description: "`DROP SOURCE` removes a source from your Materialize instances."
menu:
  main:
    parent: 'sql'
---

`DROP SOURCE` removes a source from your Materialize instances.

## Conceptual framework

Materialize maintains your instances' sources by attaching the source to its
internal Differential dataflow engine. If you no longer need the source, you can
drop it, which will remove it from Differential.

However, if views depend on the source you must either [drop the views
explicitly](../drop-view) or use the **CASCADE** option.

## Syntax

{{< diagram "drop-source.html" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named source does not exist.
_source&lowbar;name_ | The source you want to drop. You can find available source names through [`SHOW SOURCES`](../show-sources).
**RESTRICT** | Do not drop this source if any views depend on it. _(Default)_
**CASCADE** | Drop all views that depend on this source.

## Details

Before you can drop a source, you must [drop all views](../drop-view) which use
it.

## Examples

```sql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```sql
DROP SOURCE my_source;
```
```nofmt
DROP SOURCE
```

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
