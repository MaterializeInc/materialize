---
title: "DROP MATERIALIZED VIEW"
description: "`DROP MATERIALIZED VIEW` removes a materialized view from your Materialize instances."
menu:
  main:
    parent: commands
---

`DROP MATERIALIZED VIEW` removes a materialized view from your Materialize instances.

## Conceptual framework

Materialize maintains materialized views after you create them. If you no longer need the
materialized view, you can remove it.

Because materialized views rely on receiving data from sources, you must drop all materialized views that
rely on a source before you can [drop the source](../drop-source) itself. You can achieve this easily using **DROP SOURCE...CASCADE**.

## Syntax

{{< diagram "drop-materialized-view.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named materialized view does not exist.
_view&lowbar;name_ | The materialized view you want to drop. You can find available materialized view names through [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).
**RESTRICT** | Do not drop this materialized view if any other views depend on it. _(Default)_
**CASCADE** | Drop all views that depend on this materialized view.

## Examples

```sql
SHOW MATERIALIZED VIEWS;
```
```nofmt
         name
----------------------
 my_materialized_view
```
```sql
DROP MATERIALIZED VIEW my_materialized_view;
```
```nofmt
DROP MATERIALIZED VIEW
```

## Related pages

- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
