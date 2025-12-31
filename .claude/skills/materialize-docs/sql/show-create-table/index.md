---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-table/
complexity: intermediate
description: '`SHOW CREATE TABLE` returns the SQL used to create the table.'
doc_type: reference
keywords:
- SHOW CREATE TABLE
- CREATE THE
- SHOW CREATE
product_area: Indexes
status: stable
title: SHOW CREATE TABLE
---

# SHOW CREATE TABLE

## Purpose
`SHOW CREATE TABLE` returns the SQL used to create the table.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE TABLE` returns the SQL used to create the table.



`SHOW CREATE TABLE` returns the SQL used to create the table.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE TABLE <table_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available table names, see [`SHOW TABLES`](/sql/show-tables).

## Examples

This section covers examples.

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
```text

```mzsql
SHOW CREATE TABLE t;
```text
```nofmt
         name         |                                             create_sql
----------------------+-----------------------------------------------------------------------------------------------------
 materialize.public.t | CREATE TABLE "materialize"."public"."t" ("a" "pg_catalog"."int4", "b" "pg_catalog"."text" NOT NULL)
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the table.


## Related pages

- [`SHOW TABLES`](../show-tables)
- [`CREATE TABLE`](../create-table)

