---
title: "Time travel and history retention periods"
description: "Understanding time travel and history retention periods"
menu:
  main:
    parent: manage
    name: "Time travel and history data retention periods"
    weight: 13
---

{{< private-preview />}}

By default, all user-defined sources, tables, materialized views, and indexes have a history
retention period of one second. You can adjust the history
retention period on these objects for time travel queries.

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

### Observe the configured history retention period
<!-- TODO(mjibson): replace this section with a mention of the catalog table/column
    once it's available -->
To see what history retention period has been configured for an object, run the
`SHOW CREATE ...` statement for the given object. See [`SHOW CREATE SOURCE`](/sql/show-create-source/)
for an example.

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

### History removal

The history retention period represents the minimum amount of historical data
guaranteed to be retained by Materialize. History clean up is processed in the
background, so older history may be accessible for the period of time between when
it falls outside the retention period and when it is cleaned up.