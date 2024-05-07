---
title: "Time travel queries"
description: "How to use history retention to enable time travel queries in Materialize"
menu:
  main:
    parent: 'sql-patterns'
    name: "Time travel queries"
---

{{< private-preview />}}

By default, all user-defined sources, tables, materialized views, and indexes
keep track of the most recent version of their underlying data. Some use cases
require accessing past versions of this data at specific points in time
(i.e. time traveling). To allow time travel queries, you can configure these
objects to **retain history**.

When set to a value different than the default (i.e. no history retention), the
history retention period of an object determines the duration for which
historical versions of its underlying data are available for querying.

The common use cases for time traveling in Materialize are:

* Lossless, **durable subscriptions** to your changing results. See the
  [`SUBSCRIBE` documentation](/sql/subscribe/#durable-subscriptions)
  for examples of how to create this type of subscription.

* Accessing a past version of results at a specific point in time.

## History retention period

You can change the history retention period for [sources](/sql/create-source/),
[tables](/sql/create-table/), [materialized views](/sql/create-materialized-view/),
and [indexes](/sql/create-index/). To configure the history retention period for
an object, use the `RETAIN HISTORY` option in the `CREATE` statement. This
value can also be adjusted at any time using the object-specific `ALTER`
statement.

### Configuration

#### Increasing the history retention period

When you increase the history retention period for an object, both the existing
historical data and any subsequently produced historical data are retained for
the specified time period.

* **For sources, tables and materialized views:** increasing the history retention
  period will not restore older historical data that was already outside the
  previous history retention period before the change.

* **For indexes:** if all of the underlying source, table, and materialized view
  historical data is available for the increased history retention period, the
  index can use that data to backfill as far back as the underlying historical
  data is available.

#### Decreasing the history retention period

When you decrease the history retention period for an object:

* Newly produced historical data is retained for the new, shorter history
  retention period.

* Historical data outside the new, shorter history retention period is no longer
  retained. If you subsequently increase the history retention period again,
  the older historical data may already be unavailable.

### Resource utilization

It's important to note that increasing the history retention period for an
object will lead to increased resource utilization in Materialize.

* **For sources, tables and materialized views:** increasing the history
    retention period for these objects increases the amount of historical data
    that is retained in the storage layer. You can expect storage resource
    utilization to increase, which may incur additional costs.

* **For indexes:** increasing the history retention period for an index
    increases the amount of historical data that is retained in memory in the
    cluster maintaining the index. You can expect memory resource utilization
    for that cluster to go up, which might require you to size up your cluster
    and consume additional compute credits.

### Clean-up

The history retention period represents the minimum amount of historical data
guaranteed to be retained by Materialize. History clean-up is processed in the
background, so older history may be accessible for the period of time between
when it falls outside the retention period and when it is cleaned up.

### Best practices

Given the additional memory costs associated with increasing the history
retention period on indexes, if you don't need the performance benefits of an
index - in particular for the use case of [durable subscriptions](/sql/subscribe#durable-subscriptions)
- you should consider creating a materialized view for your subscription query
and configure the history retention period on that materialized view
instead.

Similarly, because of the increased storage costs and processing time for the
additional historical data on whichever object has an increased history
retention period, you should consider configuring history retention period on
the index or materialized view directly powering the subscription, rather than
all the way through the dependency chain from the source to the index or
materialized view.

## Examples

### Enabling history retention

To set a history retention period for an object, use the `RETAIN HISTORY`
option, which accepts positive [interval](/sql/types/interval/) values
(e.g. `'1hr'`):

```sql
CREATE MATERIALIZED VIEW winning_bids
WITH (RETAIN HISTORY FOR '1hr') AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

To adjust the history retention period for an object, use `ALTER`:

```sql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '2hr');
```

<!-- TODO(maddyblue): replace this paragraph with a mention of the catalog
     table/column once it's available -->
To see what history retention period has been configured for an object, run the
`SHOW CREATE ...` statement for the given object. See [`SHOW CREATE SOURCE`](/sql/show-create-source/)
for an example.

```sql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "quickstart" WITH (RETAIN HISTORY = FOR '2hr') AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

### Disabling history retention

To disable history retention, set the `RETAIN HISTORY` option to its original
default value (1 second):

```sql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '1s');
```
