---
title: "SHOW CREATE TYPE"
description: "`SHOW CREATE TYPE` returns the DDL statement used to custom create the type. Returns <builtin> for builtin types"
menu:
  main:
    parent: commands
---

`SHOW CREATE TYPE` returns the DDL statement used to create the custom type.

## Syntax

```sql
SHOW CREATE TYPE <type_name>
```

For available type names names, see [`SHOW TYPES`](/sql/show-types).

## Examples

```sql
SHOW CREATE TYPE point;

```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 point            | CREATE TYPE "materialize"."public"."point" AS ("x" [s20 AS "pg_catalog"."int4"], "y" [s20 AS "pg_catalog"."int4"])
```

## Privileges

{{< include-md
file="shared-content/sql-command-privileges/show-create-type.md" >}}

## Related pages

- [`SHOW TYPES`](../show-types)
- [`CREATE TYPE`](../create-type)
