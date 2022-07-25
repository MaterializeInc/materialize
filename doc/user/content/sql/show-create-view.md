---
title: "SHOW CREATE VIEW"
description: "`SHOW CREATE VIEW` returns the `SELECT` statement used to create the view."
menu:
  main:
    parent: commands
---

`SHOW CREATE VIEW` returns the [`SELECT`](../select) statement used to create the view.

## Syntax

{{< diagram "show-create-view.svg" >}}

Field | Use
------|-----
_view&lowbar;name_ | The view you want to use. You can find available view names through [`SHOW VIEWS`](../show-views).

## Examples

```sql
SHOW CREATE VIEW my_view;
```
```nofmt
            View            |                                           Create View
----------------------------+--------------------------------------------------------------------------------------------------
 materialize.public.my_view | CREATE VIEW "materialize"."public"."my_view" AS SELECT * FROM "materialize"."public"."my_source"
```

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`CREATE VIEW`](../create-view)
