---
title: "SHOW CREATE TYPE"
description: "`SHOW CREATE TYPE` returns the DDL statement used to custom create the type."
menu:
  main:
    parent: commands
---

`SHOW CREATE TYPE` returns the DDL statement used to create the custom type.

## Syntax

```sql
SHOW [REDACTED] CREATE TYPE <type_name>;
```

{{< yaml-table data="show_create_redacted_option" >}}

For available type names names, see [`SHOW TYPES`](/sql/show-types).

## Examples

```sql
SHOW CREATE TYPE point;

```

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 point            | CREATE TYPE materialize.public.point AS (x pg_catalog.int4, y pg_catalog.int4);
```

## Privileges

{{< include-md
file="shared-content/sql-command-privileges/show-create-type.md" >}}

## Related pages

- [`SHOW TYPES`](../show-types)
- [`CREATE TYPE`](../create-type)
