---
title: "SHOW CREATE VIEW"
description: "`SHOW CREATE VIEW` returns the `SELECT` statement used to create the view."
menu:
  main:
    parent: commands
---

`SHOW CREATE VIEW` returns the [`SELECT`](../select) statement used to create the view.

## Syntax

```sql
SHOW CREATE VIEW <view_name>
```

For available view names, see [`SHOW VIEWS`](/sql/show-views).

## Examples

```mzsql
SHOW CREATE VIEW my_view;
```
```nofmt
            name            |                                            create_sql
----------------------------+--------------------------------------------------------------------------------------------------
 materialize.public.my_view | CREATE VIEW "materialize"."public"."my_view" AS SELECT * FROM "materialize"."public"."my_source"
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the view.

## Related pages

- [`SHOW VIEWS`](../show-views)
- [`CREATE VIEW`](../create-view)
