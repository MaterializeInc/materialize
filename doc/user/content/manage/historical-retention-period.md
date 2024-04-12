---
title: "Time travel and historical data retention periods"
description: "Understanding time travel and historical data retention periods"
menu:
  main:
    parent: manage
    name: "Time travel and historical data retention periods"
    weight: 13
---

{{< private-preview />}}

By default, all user-defined sources, tables, materialized views, and indexes have a historical
data retention period of one second. Materialize provides the option to adjust the retention
period on these objects for time travel queries.

The common use cases for adjusting the historical data retention period are:
* Lossless, continuous subscriptions to your changing results. See the
[`SUBSCRIBE` documentation](/sql/subscribe#durable-lossless-subscriptions) for examples of
how to create this type of subscription.
* Accessing results as they were at a specific historical period in time.

## Configuring the retention period for an object
The retention period for an object can be configured at creation time within the `CREATE`
statement, by setting the `RETAIN HISTORY` `WITH` option. See the respective `CREATE`
reference documentation for each object type for syntax.

The retention period for an object can also be adjusted at any time, via the `ALTER`
statement. See the respective `ALTER` reference documentation for each object type for syntax.

The retention period is represented as an [interval](https://materialize.com/docs/sql/types/interval/)
value like `'1hr'`.

### Increasing retention
Increasing the retention period for an object causes the currently retained historical data and all
new data to be retained for the longer time period.
For sources, tables and materialized views: increasing the retention period will not restore
older data that was already outside the previous retention period before the change.
For indexes: if all of the underlying source, table, and materialized view data is available
for the increased retention period, the index can use that data to backfill as far back
as that underlying historical data is available.

### Decreasing retention
Decreasing the reteion period for an object causes:
* New data to be retained for the new, shorter retention period.
* Historical data outside the new, shorter retention period to no longer be retained. If you
subsequently increase the rention period again, the older data may already be unavailable.

### Observe the configured retention period
<!-- TODO(mjibson): replace this section with a mention of the catalog table/column
    once it's available -->
To see what retention period has been configured for an object, run the
`SHOW CREATE ...` statement for the given object. See [`SHOW CREATE SOURCE`](/sql/show-create-source/)
for an example.

## Considerations
### Resource usage
Changing the retention period for an object can have resource usage implications for your
system.

##### For sources, tables and materialized views:
Increasing the retention period for these objects increases the amount of historical
data that is retained in the Materialize durable storage. As such, your storage
resource usage may go up, incurring storage costs.

##### For indexes
Increasing the rentetion period for an index increases the amount of historical data that
is retained in memory within the cluster on which the index resides. As such, the memory
usage for that cluster may go up, and you may need to size up your cluster, which would
incur costs in the form of additional compute credits.

### Data removal
The retention period represents the minimum amount of historical data guaranteed to be
retained by Materialize. Data clean up is processed in the background, so older data
may be accessible for a period of time between when it falls outside the retention
period and when it is cleaned up.