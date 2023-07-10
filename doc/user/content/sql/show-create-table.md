---
title: "SHOW CREATE TABLE"
description: "`SHOW CREATE TABLE` returns the SQL used to create the table."
menu:
  main:
    parent: commands
---

`SHOW CREATE TABLE` returns the SQL used to create the table.

## Syntax

{{< diagram "show-create-table.svg" >}}

Field | Use
------|-----
_table&lowbar;name_ | The table you want use. You can find available table names through [`SHOW TABLES`](../show-tables).

## Examples

```sql
CREATE TABLE t (a int, b text NOT NULL);
```

```sql
SHOW CREATE TABLE t;
```
```nofmt
         name         |                                             create_sql
----------------------+-----------------------------------------------------------------------------------------------------
 materialize.public.t | CREATE TABLE "materialize"."public"."t" ("a" "pg_catalog"."int4", "b" "pg_catalog"."text" NOT NULL)
```

## Privileges

{{< alpha />}}

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the table.

## Related pages

- [`SHOW TABLES`](../show-tables)
- [`CREATE TABLE`](../create-table)
