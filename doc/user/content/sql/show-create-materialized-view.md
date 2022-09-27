---
title: "SHOW CREATE MATERIALIZED VIEW"
description: "`SHOW CREATE MATERIALIZED VIEW` returns the statement used to create the materialized view"
menu:
  main:
    parent: commands
---

`SHOW CREATE MATERIALIZED VIEW` returns the DDL statement used to create the materialized view.

## Syntax

{{< diagram "show-create-materialized-view.svg" >}}

Field | Use
------|-----
_view&lowbar;name_ | The materialized view you want to use. You can find available materialized view names through [`SHOW MATERIALIZED VIEWS`](../show-materialized-views).

## Examples

```sql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "default" AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
