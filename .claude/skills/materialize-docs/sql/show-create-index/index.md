---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-index/
complexity: beginner
description: '`SHOW CREATE INDEX` returns the statement used to create the index.'
doc_type: reference
keywords:
- SHOW CREATE INDEX
- CREATE THE
- SHOW CREATE
product_area: Indexes
status: stable
title: SHOW CREATE INDEX
---

# SHOW CREATE INDEX

## Purpose
`SHOW CREATE INDEX` returns the statement used to create the index.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE INDEX` returns the statement used to create the index.



`SHOW CREATE INDEX` returns the DDL statement used to create the index.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE INDEX <index_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available index names, see [`SHOW INDEXES`](/sql/show-indexes).

## Examples

This section covers examples.

```mzsql
SHOW INDEXES FROM my_view;
```text

```nofmt
     name    | on  | cluster    | key
-------------+-----+------------+--------------------------------------------
 my_view_idx | t   | quickstart | {a, b}
```text

```mzsql
SHOW CREATE INDEX my_view_idx;
```text

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

