---
title: "DROP SOURCE"
description: "`DROP SOURCE` removes a source from your Materialize instances."
menu:
  main:
    parent: commands
---

`DROP SOURCE` removes a source from your Materialize instances.

## Conceptual framework

Materialize maintains your instances' sources by attaching the source to its
internal Differential dataflow engine. If you no longer need the source, you can
drop it, which will remove it from Differential.

However, if views depend on the source you must either [drop the views
explicitly](../drop-view) or use the **CASCADE** option.

## Syntax

{{< diagram "drop-source.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named source does not exist.
_source&lowbar;name_ | The name of the source you want to remove.
**CASCADE** | Remove the source and its dependent views.
**RESTRICT** | Do not remove this source if any views depend on it. _(Default.)_

## Examples

### Remove a source with no dependent views

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

### Remove a source with dependent views

```sql
SHOW SOURCES;
```
```nofmt
...
my_source
```
```sql
DROP SOURCE my_source CASCADE;
```

### Remove a source only if it has no dependent views

You can use either of the following commands:

- ```sql
  DROP SOURCE my_source;
  ```
- ```sql
  DROP SOURCE my_source RESTRICT;
  ```

### Do not issue an error if attempting to remove a nonexistent source

```sql
DROP SOURCE IF EXISTS my_source;
```

## Related pages

- [`CREATE SOURCE`](../create-source)
- [`SHOW SOURCES`](../show-sources)
- [`SHOW CREATE SOURCE`](../show-create-source)
- [DROP OWNED](../drop-owned)
