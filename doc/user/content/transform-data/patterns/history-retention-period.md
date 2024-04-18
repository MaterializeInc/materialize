---
title: "Time travel and history retention periods"
description: "Understanding time travel and history retention periods"
menu:
  main:
    parent: 'sql-patterns'
    name: "Time travel and history retention periods"
---

{{< private-preview />}}

The history retention period of an object is the duration for which historical versions
of the object (also known as historical data or time-versioned data) are retained.

By default, all user-defined sources, tables, materialized views, and indexes have a history
retention period of one second. For the purpose of time travel queries, you can adjust the
history retention period on these objects.

The common use cases for adjusting the history retention period are:
* Lossless, continuous subscriptions to your changing results. See the
[`SUBSCRIBE` documentation](/sql/subscribe#durable-lossless-subscriptions) for examples of
how to create this type of subscription.
* Accessing a past version of results at a specific point in time.

## Configuring the history retention period for an object

You can change the history retention period for [sources](/sql/create-source/),
[tables](/sql/create-table/), [materialized views](/sql/create-materialized-view/),
and [indexes](/sql/create-index/). To configure a history retention period for
an object that is different to the default (1 second), use the `RETAIN HISTORY`
option in the respective `CREATE` statement. This value can be adjusted at any
time using the respective `ALTER` statement.

<!-- TODO(mjibson): replace this paragraph with a mention of the catalog table/column
     once it's available -->
To see what history retention period has been configured for an object, run the
`SHOW CREATE ...` statement for the given object. See [`SHOW CREATE SOURCE`](/sql/show-create-source/)
for an example.

[//]: # "TODO(morsapaes) Include example."

### Increasing the history retention period

Increasing the history retention period for an object causes both the retained
historical data and subsequently produced historical data to be retained for
the specified time period.

* **For sources, tables and materialized views:** increasing the history retention
  period will not restore older historical data that was already outside the
  previous history retention period before the change.

* **For indexes:** if all of the underlying source, table, and materialized view
  historical data is available for the increased history retention period, the
  index can use that data to backfill as far back as the underlying historical
  data is available.

### Decreasing the history retention period

Decreasing the history retention period for an object causes:

* Newly produced historical data to be retained for the new, shorter history
  retention period.

* Historical data outside the new, shorter history retention period to no longer
  be retained. If you subsequently increase the history retention period again,
  the older historical data may already be unavailable.


## Considerations

### Resource usage

Changing the history retention period for an object can have resource usage
implications in Materialize.

* **For sources, tables and materialized views:** increasing the history
    retention period for these objects increases the amount of historical data
    that is retained in the storage layer. You can expect storage resource
    utilization to increase, which may incur additional costs.

* **For indexes:** increasing the history retention period for an index
    increases the amount of historical data that is retained in memory in the
    cluster maintaining the index. You can expect memory resource utilization
    for that cluster to go up, which might require you to size up your cluster
    and consume additional compute credits.

#### Best practices
Given the additional memory costs associated with increasing the history retention period
on indexes, if you don't need the performance benefits of an index - in particular for 
the use case of lossless, continuous subscribes - it's better to create a
materialized view for your subscription query and configure the history retention period
on that materialized view.

Similarly, because of the increased storage costs and processing time for the additional
historical data on whichever object has an increasde history retention period, it's better
to configure the retention the history retention period on
the index or materialized view directly powering the subscribe, rather than all the
way through the dependency chain from the source to the index or materialized view.

### History removal

The history retention period represents the minimum amount of historical data
guaranteed to be retained by Materialize. History clean up is processed in the
background, so older history may be accessible for the period of time between when
it falls outside the retention period and when it is cleaned up.


## Examples
### Creating a materialized view with retain history configured
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

### Modifying retain history
```sql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '1hr');
```

### Observing retain history
```sql
SHOW CREATE MATERIALIZED VIEW winning_bids;
```
```nofmt
              name               |                                                                                                                       create_sql
---------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 materialize.public.winning_bids | CREATE MATERIALIZED VIEW "materialize"."public"."winning_bids" IN CLUSTER "quickstart" WITH (RETAIN HISTORY = FOR '1hr') AS SELECT * FROM "materialize"."public"."highest_bid_per_auction" WHERE "end_time" < "mz_catalog"."mz_now"()
```

### Subscribe as of a timestamp in the past
```sql
SUBSCRIBE TO winning_bids WITH (PROGRESS, SNAPSHOT=false) AS OF 1713467627903;
