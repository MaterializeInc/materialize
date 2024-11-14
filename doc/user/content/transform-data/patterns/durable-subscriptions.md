---
title: "Durable subscriptions"
description: "How to enable lossless, durable subscriptions to your changing results in Materialize"
menu:
  main:
    parent: 'sql-patterns'
aliases:
  - /transform-data/patterns/time-travel-queries/
  - /transform-data/patterns/time-travel-queries/#history-retention-period
---

[//]: # "TODO: Move to Serve results section"

[Subscriptions](/sql/subscribe/) allow you to stream changing results from
Materialize to an external application programatically. Like any connection over
the network, subscriptions might get disrupted for both expected and unexpected
reasons. In such cases, it can be useful to have a mechanism to gracefully
recover data processing.

To avoid the need for re-processing data that was already sent to your external
application following a connection disruption, you can:

- Adjust the [history retention period](#history-retention-period) for the
  objects that a subscription depends on, and

- [Access past versions of this
  data](#enabling-durable-subscriptions-in-your-application) at specific points
  in time to pick up data processing where you left off.

## History retention period

{{< private-preview />}}

By default, all user-defined sources, tables, materialized views, and indexes
keep track of the most recent version of their underlying data. To gracefully
recover from connection disruptions and enable lossless, _durable
subscriptions_, you can configure the sources, tables, and materialized views
that the subscription depends on to **retain history**.

{{< important >}}

Configuring indexes to retain history is not recommended. Instead, consider
creating a materialized view for your subscription query and configuring the
history retention period on that view.

{{</ important >}}

To configure the history retention period for sources, tables and materialized
views, use the `RETAIN HISTORY` option in its `CREATE` statement. This value can
also be adjusted at any time using the object-specific `ALTER` statement.

### Semantics

#### Increasing the history retention period

When you increase the history retention period for an object, both the existing
historical data and any subsequently produced historical data are retained for
the specified time period.

**For sources, tables and materialized views:** increasing the history retention
  period will not restore older historical data that was already outside the
  previous history retention period before the change.

Configuring indexes to retain history is not recommended. Instead, consider
creating a materialized view for your subscription query and configuring the
history retention period on that view.

See also [Considerations](#considerations).

#### Decreasing the history retention period

When you decrease the history retention period for an object:

* Newly produced historical data is retained for the new, shorter history
  retention period.

* Historical data outside the new, shorter history retention period is no longer
  retained. If you subsequently increase the history retention period again,
  the older historical data may already be unavailable.

See also [Considerations](#considerations).

### Set history retention period

{{< important >}}

Setting the history retention period for an object will lead to increased
resource utilization. Moreover, for indexes, setting history retention period is
not recommended. Instead, consider creating a materialized view for your
subscription query and configuring the history retention period on that view.
See [Considerations](#considerations).

{{</ important >}}

To set the history retention period for [sources](/sql/create-source/),
[tables](/sql/create-table/), and [materialized
views](/sql/create-materialized-view/), you can either:

- Specify the `RETAIN HISTORY` option in the `CREATE` statement. The `RETAIN
   HISTORY` option accepts positive [interval](/sql/types/interval/)
   values (e.g., `'1hr'`). For example:

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

- Specify the `RETAIN HISTORY` option in the `ALTER` statement. The `RETAIN
  HISTORY` option accepts positive [interval](/sql/types/interval/) values
  (e.g., `'1hr'`). For example:

  ```mzsql
  ALTER MATERIALIZED VIEW winning_bids SET (RETAIN HISTORY FOR '2hr');
  ```

### View history retention period for an object

To see what history retention period has been configured for an object, look up
the object in the
[`mz_internal.mz_history_retention_strategies`](/sql/system-catalog/mz_internal/#mz_history_retention_strategies)
catalog table. For example:

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
        LEFT JOIN mz_internal.mz_history_retention_strategies AS hrs ON mv.id = hrs.id
WHERE mv.name = 'winning_bids';
```

If set, the returning result includes the value (in milliseconds) of the history
retention period:

```nofmt

 database_name | schema_name |     name     | strategy |  value
---------------+-------------+--------------+----------+---------
 materialize   | public      | winning_bids | FOR      | 7200000
```

### Unset/reset history retention period

To disable history retention, reset the history retention period; i.e., specify
the `RESET (RETAIN HISTORY)` option in the `ALTER` statement. For example:

```mzsql
ALTER MATERIALIZED VIEW winning_bids RESET (RETAIN HISTORY);
```

### Considerations

#### Resource utilization

Increasing the history retention period for an object will lead to increased
resource utilization in Materialize.

**For sources, tables and materialized views:**  Increasing the history
retention period for these objects increases the amount of historical data that
is retained in the storage layer. You can expect storage resource utilization to
increase, which may incur additional costs.

**For indexes:** Configuring indexes to retain history is not recommended.
Instead, consider creating a materialized view for your subscription query and
configuring the history retention period on that view.

#### Best practices

- Because of the increased storage costs and processing time for the additional
  historical data, consider configuring history retention period on the object
  directly powering the subscription, rather than all the way through the
  dependency chain from the source to the materialized view.

- Configuring indexes to retain history is not recommended. Instead, consider
  creating a materialized view for your subscription query and configuring the
  history retention period on that view.

#### Clean-up

The history retention period represents the minimum amount of historical data
guaranteed to be retained by Materialize. History clean-up is processed in the
background, so older history may be accessible for the period of time between
when it falls outside the retention period and when it is cleaned up.

## Enabling durable subscriptions in your application

1. In Materialize, configure the history retention period for the object(s)
queried in the `SUBSCRIBE`. Choose a duration you expect will allow you to
recover in case of connection drops. One hour (`1h`) is a good place to start,
though you should be mindful of the [impact](#considerations) of increasing an
object's history retention period.

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

1. As results come in continuously, buffer the latest results in memory until
you receive a [progress](/sql/subscribe#progress) message. At that point, the
data up until the progress message is complete, so you can:

   1. Process all the buffered data in your application.
   1. Persist the `mz_timestamp` of the progress message.

1. To resume the subscription in subsequent restarts, use the following
continuous query against Materialize in your application code:

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

### Note about idempotency

The guidance above recommends you buffer data in memory until receiving a
progress message, and then persist the data and progress message `mz_timestamp`
at the same time. This is to ensure data is processed **exactly once**.

In the case that your application crashes and you need to resume your subscription using the
persisted progress message `mz_timestamp`:
* If you were processing data in your application before persisting the subsequent progress message's `mz_timestamp`: you may end up processing duplicate data.
* If you were persisting the progress message's `mz_timestamp` before processing all the
buffered data from before that progress message: you may end up dropping some data.

As a result, to guarantee that the data processing occurs only once after your
application crashes, you must write the progress message `mz_timestamp` and all
buffered data **together in a single transaction**.
