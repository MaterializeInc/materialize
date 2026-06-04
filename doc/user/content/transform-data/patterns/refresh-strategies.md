---
title: "Refresh strategies and scheduled clusters"
description: "Configure materialized views to recompute on a fixed schedule, and host them on clusters that turn on only for refreshes."
robots: "noindex, nofollow"
build:
  render: always
  list: never
---

{{< private-preview />}}

Materialized views in Materialize are incrementally maintained by default, meaning their results are automatically updated as soon as new data arrives.
This guarantees that queries returns the most up-to-date information available with minimal delay and that results are always as [fresh](/concepts/reaction-time) as the input data itself.

In most cases, this default behavior is ideal.
However, in some very specific scenarios like reporting over slow changing historical data, it may be acceptable to relax freshness in order to reduce compute usage.
For these cases, Materialize supports refresh strategies, which allow you to configure a materialized view to recompute itself on a fixed schedule rather than maintaining them incrementally.

{{< note >}}

The use of refresh strategies is discouraged unless you have a clear and measurable need to reduce maintenance costs on stale or archival data. For most use cases, the default incremental maintenance model provides a better experience.

{{< /note >}}

## Refresh strategies

### Refresh on commit

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH ON COMMIT</code></p>

Materialized views in Materialize are incrementally updated by default. This means that as soon as new data arrives in the system, any dependent materialized views are automatically and continuously updated. This behavior, known as **refresh on commit**, ensures that the view's contents are always as fresh as the underlying data.

**`REFRESH ON COMMIT` is:**

* **Generally available**
* The **default behavior** for all materialized views
* **Implicit** and does not need to be manually specified
* **Strongly recommended** for the vast majority of use cases

With `REFRESH ON COMMIT`, Materialize provides low-latency, up-to-date results without requiring user-defined schedules or manual refreshes. This model is ideal for most workloads, including streaming analytics, live dashboards, customer-facing queries, and applications that rely on timely, accurate results.

Only in rare cases—such as batch-oriented processing or reporting over slowly changing historical datasets—might it make sense to trade off freshness for potential cost savings. In such cases, consider defining an explicit refresh strategy to control when recomputation occurs.

### Refresh at

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH AT</code> { <code>CREATION</code> | <i>timestamp</i> }</p>

This strategy allows configuring a materialized view to **refresh at a specific
time**. The refresh time can be specified as a timestamp, or using the `AT CREATION`
clause, which triggers a first refresh when the materialized view is created.

**Example**

To create a materialized view that is refreshed at creation, and then at the
specified times:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_at
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh at a user-specified (future) time
  REFRESH AT '2024-06-06 12:00:00',
  -- Refresh at another user-specified (future) time
  REFRESH AT '2024-06-08 22:00:00'
)
AS SELECT ... FROM ...;
```

You can specify multiple `REFRESH AT` strategies in the same `CREATE` statement,
and combine them with the [`REFRESH EVERY` strategy](#refresh-every).

### Refresh every

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH EVERY</code> <i>interval</i> [ <code>ALIGNED TO</code> <i>timestamp</i> ]</code></p>

This strategy allows configuring a materialized view to **refresh at regular
intervals**. The `ALIGNED TO` clause additionally allows specifying the _phase_
of the scheduled refreshes: for daily refreshes, it specifies the time of the
day when the refresh will happen; for weekly refreshes, it specifies the day of
the week and the time of the day when the refresh will happen. If `ALIGNED TO`
is not specified, it defaults to the time when the materialized view is
created.

**Example**

To create a materialized view that is refreshed at creation, and then once a day
at 10PM UTC:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-06 22:00:00'
) AS
SELECT ...;
```

You can specify multiple `REFRESH EVERY` strategies in the same `CREATE`
statement, and combine them with the [`REFRESH AT` strategy](#refresh-at). When
this strategy, we recommend **always** using the [`REFRESH AT CREATION`](#refresh-at)
clause, so the materialized view is available for querying ahead of the first
user-specified refresh time.

### Querying materialized views with refresh strategies

Materialized views configured with [`REFRESH EVERY` strategies](#refresh-every)
have a period of unavailability around the scheduled refresh times — during this
period, the view **will not return any results**. To avoid unavailability
during the refresh operation, you must host these views in
[**scheduled clusters**](#scheduled-clusters), which can be
configured to automatically [turn on ahead of the scheduled refresh time](#hydration-time-estimate).

**Example**

To create a scheduled cluster that turns on 1 hour ahead of any scheduled
refresh times:

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

You can then create a materialized view in this cluster, configured to refresh
at creation, then once a day at 12PM UTC:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 12PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-18 00:00:00'
) AS
SELECT ...;
```

Because the materialized view is hosted on a scheduled cluster that is
configured to **turn on ahead of any scheduled refreshes**, you can expect
`my_scheduled_cluster` to be provisioned at 11PM UTC — or, 1 hour ahead of the
scheduled refresh time for `mv_refresh_every`. This means that the cluster can
backfill the view with pre-existing data — a process known as [_hydration_](/transform-data/troubleshooting/#hydrating-upstream-objects)
— ahead of the refresh operation, which **reduces the total unavailability window
of the view** to just the duration of the refresh.

If the cluster is **not** configured to turn on ahead of scheduled refreshes
(i.e., using the `HYDRATION TIME ESTIMATE` option), the total unavailability
window of the view will be a combination of the hydration time for all objects
in the cluster (typically long) and the duration of the refresh for the
materialized view (typically short).

Depending on the actual time it takes to hydrate the view or set of views in the
cluster, you can later adjust the hydration time estimate value for the
cluster using [`ALTER CLUSTER`](#altering-a-clusters-schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '30 minutes'));
```

### Introspection

To check details about the (non-default) refresh strategies associated with any materialized
view in the system, you can query
the [`mz_internal.mz_materialized_view_refresh_strategies`](/reference/system-catalog/mz_internal/#mz_materialized_view_refresh_strategies)
and [`mz_internal.mz_materialized_view_refreshes`](/reference/system-catalog/mz_internal/#mz_materialized_view_refreshes)
system catalog tables:

```mzsql
SELECT mv.id AS materialized_view_id,
       mv.name AS materialized_view_name,
       rs.type AS refresh_strategy,
       rs.interval AS refresh_interval,
       rs.aligned_to AS refresh_interval_phase,
       rs.at AS refresh_time,
       r.last_completed_refresh,
       r.next_refresh
FROM mz_internal.mz_materialized_view_refresh_strategies rs
JOIN mz_internal.mz_materialized_view_refreshes r ON r.materialized_view_id = rs.materialized_view_id
JOIN mz_materialized_views mv ON rs.materialized_view_id = mv.id;
```

## Scheduled clusters

To support [scheduled refreshes in materialized views](#refresh-strategies),
you can configure a cluster to automatically turn on and off using the
`SCHEDULE...ON REFRESH` syntax.

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '800cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

Scheduled clusters should **only** contain materialized views configured with a
non-default [refresh strategy](#refresh-strategies)
(and any indexes built on these views). These clusters will automatically turn
on (i.e., be provisioned with compute resources) based on the configured
refresh strategies, and **only** consume credits for the duration of the
refreshes.

It's not possible to manually turn on a cluster with `ON REFRESH` scheduling. If
you need to turn on a cluster outside its schedule, you can temporarily disable
scheduling and provision compute resources using [`ALTER CLUSTER`](#altering-a-clusters-schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster SET (SCHEDULE = MANUAL, REPLICATION FACTOR = 1);
```

To re-enable scheduling:

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

The `SCHEDULE` option accepts the following values:

Value        | Description
-------------|---------------------------------------------------------------
`MANUAL`     | The cluster is provisioned and turned off manually. This is the default.
`ON REFRESH` | The cluster automatically turns on to perform scheduled refreshes of the materialized views it hosts, and turns off otherwise.

### Hydration time estimate

<p style="font-size:14px"><b>Syntax:</b> <code>HYDRATION TIME ESTIMATE</code> <i>interval</i></p>

By default, scheduled clusters will turn on at the scheduled refresh time. To
avoid [unavailability of the objects scheduled for refresh](#querying-materialized-views-with-refresh-strategies) during the refresh
operation, we recommend turning the cluster on ahead of the scheduled time to
allow hydration to complete. This can be controlled using the `HYDRATION
TIME ESTIMATE` clause.

### Altering a cluster's schedule

For use cases that require using scheduled clusters, you can set or change the
originally configured schedule and related options using the [`ALTER
CLUSTER`](/sql/alter-cluster/) command.

```mzsql
ALTER CLUSTER c1 SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour'));
```

### Scheduling strategy

To check the scheduling strategy associated with a cluster, you can query the
[`mz_internal.mz_cluster_schedules`](/reference/system-catalog/mz_internal/#mz_cluster_schedules)
system catalog table:

```mzsql
SELECT c.id AS cluster_id,
       c.name AS cluster_name,
       cs.type AS schedule_type,
       cs.refresh_hydration_time_estimate
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id
WHERE c.name = 'my_refresh_cluster';
```

To check if a scheduled cluster is turned on, you can query the
[`mz_catalog.mz_cluster_replicas`](/reference/system-catalog/mz_catalog/#mz_cluster_replicas) system catalog table:

```mzsql
SELECT cs.cluster_id,
       -- A cluster with scheduling is "on" when it has compute resources
       -- (i.e. a replica) attached.
       CASE WHEN cr.id IS NOT NULL THEN true
       ELSE false END AS is_on
FROM mz_internal.mz_cluster_schedules cs
JOIN mz_clusters c ON cs.cluster_id = c.id AND cs.type = 'on-refresh'
LEFT JOIN mz_cluster_replicas cr ON c.id = cr.cluster_id;
```

You can also use the [audit log](/reference/system-catalog/mz_catalog/#mz_audit_events)
to observe the commands that are automatically run when a scheduled cluster is
turned on and off for materialized view refreshes:

```mzsql
SELECT *
FROM mz_audit_events
WHERE object_type = 'cluster-replica'
ORDER BY occurred_at DESC;
```

Any commands attributed to scheduled refreshes will be marked with
`"reason":"schedule"` under the `details` column.
