---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-view/
complexity: intermediate
description: '`SHOW CREATE VIEW` returns the `SELECT` statement used to create the
  view.'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE VIEW
- SHOW CREATE
product_area: Views
status: stable
title: SHOW CREATE VIEW
---

# SHOW CREATE VIEW

## Purpose
`SHOW CREATE VIEW` returns the `SELECT` statement used to create the view.

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE VIEW` returns the `SELECT` statement used to create the view.



`SHOW CREATE VIEW` returns the [`SELECT`](../select) statement used to create the view.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE VIEW <view_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available view names, see [`SHOW VIEWS`](/sql/show-views).

## Examples

This section covers examples.

```mzsql
SHOW CREATE VIEW my_view;
```text
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

