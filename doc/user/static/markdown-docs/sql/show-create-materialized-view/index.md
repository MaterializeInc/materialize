# SHOW CREATE MATERIALIZED VIEW

`SHOW CREATE MATERIALIZED VIEW` returns the statement used to create the materialized view



`SHOW CREATE MATERIALIZED VIEW` returns the DDL statement used to create the materialized view.

## Syntax

```sql
SHOW [REDACTED] CREATE MATERIALIZED VIEW <view_name>;
```


| Syntax element | Description |
| --- | --- |
| <strong>REDACTED</strong> | If specified, literals will be redacted. |


For available materialized view names, see [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views).

## Examples

```mzsql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "quickstart" AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

## Privileges

The privileges required to execute this statement are:

<ul>
<li><code>USAGE</code> privileges on the schema containing the materialized view.</li>
</ul>


## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`CREATE MATERIALIZED VIEW`](../create-materialized-view)
