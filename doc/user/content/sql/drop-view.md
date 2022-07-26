---
title: "DROP VIEW"
description: "`DROP VIEW` removes a view from your Materialize instances."
menu:
  main:
    parent: commands
---

`DROP VIEW` removes a view from your Materialize instances.

## Conceptual framework

Materialize maintains views after you create them. If you no longer need the
view, you can remove it.

Because views rely on receiving data from sources, you must drop all views that
rely on a source before you can [drop the source](../drop-source) itself. You can achieve this easily using **DROP SOURCE...CASCADE**.

## Syntax

{{< diagram "drop-view.svg" >}}

Field | Use
------|-----
**IF EXISTS** | Do not return an error if the named view does not exist.
_view&lowbar;name_ | The view you want to drop. You can find available view names through [`SHOW VIEWS`](../show-views).
**RESTRICT** | Do not drop this view if any other views depend on it. _(Default)_
**CASCADE** | Drop all views that depend on this view.

## Examples

```sql
SHOW VIEWS;
```
```nofmt
  name
---------
 my_view
```
```sql
DROP VIEW my_view;
```
```nofmt
DROP VIEW
```

## Related pages

- [`CREATE VIEW`](../create-view)
- [`SHOW VIEWS`](../show-views)
- [`SHOW CREATE VIEW`](../show-create-view)
