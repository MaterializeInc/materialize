---
title: "SHOW CREATE INDEX"
description: "`SHOW CREATE INDEX` returns the statement used to create the index."
menu:
  main:
    parent: commands
---

`SHOW CREATE INDEX` returns the DDL statement used to create the index.

## Syntax

```sql
SHOW CREATE INDEX <index_name>
```

For available index names, see [`SHOW INDEXES`](/sql/show-indexes).

## Examples

```mzsql
SHOW INDEXES FROM my_view;
```

```nofmt
     name    | on  | cluster    | key
-------------+-----+------------+--------------------------------------------
 my_view_idx | t   | quickstart | {a, b}
```

```mzsql
SHOW CREATE INDEX my_view_idx;
```

```nofmt
              name              |                                           create_sql
--------------------------------+------------------------------------------------------------------------------------------------
 materialize.public.my_view_idx | CREATE INDEX "my_view_idx" IN CLUSTER "default" ON "materialize"."public"."my_view" ("a", "b")
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the index.

## Related pages

- [`SHOW INDEXES`](../show-indexes)
- [`CREATE INDEX`](../create-index)
