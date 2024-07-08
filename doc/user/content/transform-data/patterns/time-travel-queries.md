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
  [durable subscriptions section below](#durable-subscriptions)
  for details on how to create this type of subscription.

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
index—in particular for the use case of durable subscriptions-you should
consider creating a materialized view for your subscription query and configure
the history retention period on that materialized view instead.

Similarly, because of the increased storage costs and processing time for the
additional historical data on whichever object has an increased history
retention period, you should consider configuring history retention period on
the index or materialized view directly powering the subscription, rather than
all the way through the dependency chain from the source to the index or
materialized view.

## Durable subscriptions

Because `SUBSCRIBE` requests happen over the network, these connections might
get disrupted for both expected and unexpected reasons. You can adjust the
[history retention period](#history-retention-period) for
the objects a subscription depends on, and then use [`AS OF`](/sql/subscribe#as-of)
to pick up where you left off on connection drops—this ensures that no data is lost
in the subscription process, and avoids the need for re-snapshotting the data.

### Implementing durable subscriptions in your application

To set up a durable subscription in your application:

1. In Materialize, configure the history retention period for the object
(s) queried in the `SUBSCRIBE`. Choose a duration you expect will allow you to
recover in case of connection drops. One hour (`1h`) is a good place to start,
though you should be mindful of the [resource utilization impact](/transform-data/patterns/time-travel-queries/#resource-utilization)
of increasing an object's history retention period.

1. In order to restart your application without losing or re-snapshotting data
after a connection drop, you need to store the latest timestamp processed for
the subscription (either in Materialize, or elsewhere in your application
state). This will allow you to resume using the retained history upstream.

1. The first time you start the subscription, run the following
continuous query against Materialize in your application code:

   ```mzsql
   SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT true);
   ```

   If you do not need a full snapshot to bootstrap your application,  change
   this to `SNAPSHOT false`.

1. As results come in continuously, buffer the latest results in memory until you receive
a [progress](/sql/subscribe#progress) message. At that point, the data up until the progress message
is complete, so you can:

   1. Process all the buffered data in your application.
   1. Persist the `mz_timestamp` of the progress message.

1. To resume the subscription in subsequent restarts,
use the following continuous query against Materialize in your application code:

   ```mzsql
   SUBSCRIBE (<your query>) WITH (PROGRESS, SNAPSHOT false) AS OF <last_progress_mz_timestamp>;
   ```

   In a similar way, as results come in continuously, buffer the latest results
   in memory until you receive a [progress](/sql/subscribe#progress) message. At that point,
   the data up until the progress message is complete, so you can:

   1. Process all the buffered data in your application.
   1. Persist the `mz_timestamp` of the progress message.

   You can tweak the flush interval at which you durably record the latest
   progress timestamp, if every progress message is too frequent.

<!--TODO(chaas): add section on DECLARE + FETCH as a way to get around continuous connection -->

### Idempotency
The guide above recommends you buffer data in memory until receiving a progress message, then persisting the data and progress message `mz_timestamp` at the same time. This is to ensure data is processed exactly once.

In the case that your application crashes and you need to resume your subscription using the
persisted progress message `mz_timestamp`:
* If you were processing data in your application before persisting the subsequent progress message's `mz_timestamp`: you may end up processing duplicate data.
* If you were persisting the progress message's `mz_timestamp` before processing all the
buffered data from before that progress message: you may end up dropping some data.

As a result, the way to guarantee exactly once data processing in the case of your application
crashes is to write the progress message `mz_timestamp` and all buffered data together in a single transaction.

<!--TODO(chaas): add top-level section on point-in-time queries, similar to Durable
subscriptions section-->

## Examples

### Enabling history retention

To set a history retention period for an object, use the `RETAIN HISTORY`
option, which accepts positive [interval](/sql/types/interval/) values
(e.g. `'1hr'`):

```mzsql
CREATE MATERIALIZED VIEW winning_bids
WITH (RETAIN HISTORY FOR '1hr') AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

### Adjusting history retention configuration

To adjust the history retention period for an object, use `ALTER`:

```mzsql
ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '2hr');
```

### Viewing history retention configuration

To see what history retention period has been configured for an object,
look up the object in the [`mz_catalog_unstable.mz_history_retention_strategies`](/sql/system-catalog/mz_catalog_unstable/#mz_history_retention_strategies) catalog table.

```mzsql
SELECT
    d.name AS database_name,
    s.name AS schema_name,
    mv.name,
    hrs.strategy,
    hrs.value
FROM
    mz_catalog.mz_materialized_views AS mv
        LEFT JOIN mz_schemas AS s ON mv.schema_id = s.id
        LEFT JOIN mz_databases AS d ON s.database_id = d.id
        LEFT JOIN mz_catalog_unstable.mz_history_retention_strategies AS hrs ON mv.id = hrs.id
WHERE mv.name = 'winning_bids';
```
```nofmt

 database_name | schema_name |     name     | strategy |  value
---------------+-------------+--------------+----------+---------
 materialize   | public      | winning_bids | FOR      | 3600000
```

### Disabling history retention

To disable history retention, reset the `RETAIN HISTORY` option:

```mzsql
ALTER MATERIALIZED VIEW winning_bids RESET (RETAIN HISTORY);
```
