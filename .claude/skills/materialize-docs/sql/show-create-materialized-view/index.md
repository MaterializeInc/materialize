---
audience: developer
canonical_url: https://materialize.com/docs/sql/show-create-materialized-view/
complexity: beginner
description: '`SHOW CREATE MATERIALIZED VIEW` returns the statement used to create
  the materialized view'
doc_type: reference
keywords:
- CREATE THE
- SHOW CREATE MATERIALIZED VIEW
- SHOW CREATE
product_area: Views
status: stable
title: SHOW CREATE MATERIALIZED VIEW
---

# SHOW CREATE MATERIALIZED VIEW

## Purpose
`SHOW CREATE MATERIALIZED VIEW` returns the statement used to create the materialized view

If you need to understand the syntax and options for this command, you're in the right place.


`SHOW CREATE MATERIALIZED VIEW` returns the statement used to create the materialized view



`SHOW CREATE MATERIALIZED VIEW` returns the DDL statement used to create the materialized view.

## Syntax

This section covers syntax.

```sql
SHOW [REDACTED] CREATE MATERIALIZED VIEW <view_name>;
```text

<!-- Dynamic table: show_create_redacted_option - see original docs -->

For available materialized view names, see [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views).

## Examples

This section covers examples.

```mzsql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```text
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "quickstart" AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the materialized view.


## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)

