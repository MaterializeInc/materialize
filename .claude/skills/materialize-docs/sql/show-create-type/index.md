---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-type/
complexity: intermediate
description: '`SHOW CREATE TYPE` returns the DDL statement used to custom create the
  type.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE TYPE
- SHOW CREATE
product_area: Indexes
status: stable
title: SHOW CREATE TYPE
---

# SHOW CREATE TYPE

## Purpose
`SHOW CREATE TYPE` returns the DDL statement used to custom create the type.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE TYPE` returns the DDL statement used to custom create the type.



`SHOW CREATE TYPE` returns the DDL statement used to create the custom type.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE TYPE <type_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available type names names, see [`SHOW TYPES`](/sql/show-types).

## Examples

This section covers examples.

```sql
SHOW CREATE TYPE point;

```text

```nofmt
    name          |    create_sql
------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 point            | CREATE TYPE materialize.public.point AS (x pg_catalog.int4, y pg_catalog.int4);
```

## Privileges

- `USAGE` privileges on the schema containing the table.


## Related pages

- [`SHOW TYPES`](../show-types)
- [`CREATE TYPE`](../create-type)

