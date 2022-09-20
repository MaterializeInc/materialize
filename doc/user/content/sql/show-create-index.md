---
title: "SHOW CREATE INDEX"
description: "`SHOW CREATE INDEX` returns the statement used to create the index."
menu:
  main:
    parent: commands
---

`SHOW CREATE INDEX` returns the DDL statement used to create the index.

## Syntax

{{< diagram "show-create-index.svg" >}}

Field | Use
------|-----
_index&lowbar;name_ | The index you want use. You can find available index names through [`SHOW INDEXES`](../show-indexes).

## Examples

```sql
SHOW INDEXES FROM my_view;
```

```nofmt
     name    | on  | cluster | key
-------------+-----+---------+--------------------------------------------
 my_view_idx | t   | default | {a, b}
```

```sql
SHOW CREATE INDEX my_view_idx;
```

```nofmt
              name              |                                           create_sql
--------------------------------+------------------------------------------------------------------------------------------------
 materialize.public.my_view_idx | CREATE INDEX "my_view_idx" IN CLUSTER "default" ON "materialize"."public"."my_view" ("a", "b")
```

## Related pages

- [`SHOW INDEXES`](../show-indexes)
- [`CREATE INDEX`](../create-index)
