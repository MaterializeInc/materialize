---
title: "SHOW CREATE MATERIALIZED VIEW"
description: "`SHOW CREATE MATERIALIZED VIEW` returns the `SELECT` statement used to create the materialized view."
menu:
  main:
    parent: commands
---

`SHOW CREATE MATERIALIZED VIEW` returns the [`SELECT`](../select) statement used to create the materialized view.

## Syntax

{{< diagram "show-create-materialized-view.svg" >}}

Field | Use
------|-----
_view&lowbar;name_ | The materialized view you want to use. You can find available materialized view names through [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).

## Examples

```sql
SHOW CREATE MATERIALIZED VIEW my_materialized_view;
```
```nofmt
            Materialized View            |        Create Materialized View
-----------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.my_materialized_view | CREATE MATERIALIZED VIEW "materialize"."public"."my_materialized_view" IN CLUSTER "default" AS SELECT * FROM "materialize"."public"."my_source"
```

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
