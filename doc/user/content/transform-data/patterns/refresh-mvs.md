---
title: "Materialized view refresh interval"
description: "Make materialized views refresh less frequently, to reduce cost."
aliases:
  - /guides/refresh-mvs/
  - /sql/patterns/refresh-mvs/
menu:
  main:
    parent: 'sql-patterns'
---

{{< private-preview />}}

By default, Materialize refreshes materialized views (MVs) on every change of their inputs (`REFRESH ON COMMIT`). This means that results in an MV are always fresh and consistent with the data in their inputs. In the majority of cases, this is the recommended way to use Materialize. However, some use cases don't require up-to-the-second data freshness, in which case you can reduce costs by making an MV refresh less often, e.g., `REFRESH EVERY '1 day'`. The cost savings of less frequent refreshes are realized by not having to keep a cluster running all the time, but only when a refresh is needed.

Note that for the cost savings to be significant, the refresh interval should be at least a few hours. This is because when the cluster of the MV is shut down between refreshes, it loses its internal state that it is normally keeping for its incremental computations. This means that an MV with a `REFRESH EVERY ...` option typically recomputes its results from scratch for every refresh. In other words, the `REFRESH EVERY` feature is not intended for refreshing every few minutes.

## `REFRESH` options

To create an MV that refreshes once per day, you can use the following syntax:
```SQL
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  REFRESH EVERY '1 day' ALIGNED TO '2024-04-17 22:00:00',
  REFRESH AT CREATION
)
AS SELECT ... FROM ...;
```

Here, the `REFRESH EVERY '1 day'` specifies that the MV will refresh once a day. The `ALIGNED TO` clause, which is an optional part of the `REFRESH EVERY` clause, specifies the "phase" of the periodic refreshes. For daily refreshes, the phase simply means the time of day when the refreshes will happen, which is 22:00 in the above example. The date component of the `ALIGNED TO` doesn't matter for a refresh interval of 1 day; it is ok for it to be either in the past or the future. (More technically, refreshes would be at times `A + i*E`, where `A` is the `ALIGNED TO` timestamp, `E` is the `REFRESH EVERY` interval, and `i = -∞ .. ∞`. From among these times, actual refreshes will happen only at such times for which the inputs have data available, which is typically from the current time onwards.) If `ALIGNED TO` is not specified, it defaults to the time when the MV is created.

The `REFRESH AT CREATION` specifies that the MV will be refreshed immediately when it is created. Without this, queries that refer to the MV would block until the first of the daily refreshes. We recommend specifying this option additionally when using `REFRESH EVERY`.

`REFRESH AT` also allows you to explicitly specify a time when the MV will refresh, e.g., `REFRESH AT '2025-06-10 15:43:26'` (not shown in the above example).

Note that you can specify `REFRESH` options multiple times, and can specify any combination of `REFRESH` options (except for `REFRESH ON COMMIT`, which is the default, and excludes any other `REFRESH` options). For example, you can create an MV that refreshes every Tuesday and Thursday at noon as follows:
```sql
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-04 12:00:00',
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-06 12:00:00',
  REFRESH AT CREATION
)
AS SELECT ... FROM ...;
```

The specified refresh times are exact logical times: Even if a refresh physically completes a few seconds (or more) later than the specified time, the results will be consistent with the state of the inputs as they were exactly at the specified logical time.

## Cluster scheduling

As mentioned above, in order to realize the cost savings from an MV that refreshes less often than the default, you need to place the MV on a cluster that is off most of the time. (A cluster consumes credits only when it is on, i.e., it has a replica.) You can manage this externally by your own scheduling mechanism by issuing `ALTER CLUSTER` commands around refreshes, turning the `REPLICATION FACTOR` up to 1 before refreshes, and down to 0 after refreshes. An easier way is to rely on Materialize's automatic cluster scheduling mechanism:

```SQL
CREATE CLUSTER my_refresh_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (REHYDRATION TIME ESTIMATE = '1 hour')
);
```

This command creates a cluster which the system will automatically turn on/off by creating/dropping a replica whenever any REFRESH MVs on the cluster need a refresh. (The `SCHEDULE =` syntax also works in `ALTER CLUSTER` statements.)

The `REHYDRATION TIME ESTIMATE` (of which the default is 0) controls how much earlier to automatically turn on the cluster before a refresh time. This is to allow the cluster to complete rehydration already before the refresh time, so that the refresh can be performed (almost) instantaneously. This way, we avoid unavailability of the MV around refreshes: If the rehydration completes before the refresh time, then querying the MV during the rehydration will simply yield the pre-refresh contents of the MV. On the other hand, if the rehydration doesn't complete before the refresh time, then there will be a period where the MV is mostly not queryable, because queries would need to serve the post-refresh MV contents, but we haven't finished computing it yet. (It's actually still queryable in `SERIALIZABLE` transaction isolation mode on its own, because in that case it will serve the pre-refresh contents. However, it's not queryable even in `SERIALIZABLE` mode if you query it together with other objects, e.g., in a join or a union, because other objects' contents usually won't be available at a time that is before the refresh, unless [`RETAIN HISTORY ...`](https://materialize.com/docs/transform-data/patterns/time-travel-queries/) is specified on them.)

Note that the system assumes that if you specify `SCHEDULE = ON REFRESH`, then refreshing MVs with (non-default) `REFRESH` options is the only purpose of that cluster, and any other objects (e.g., indexes) are there only to serve `REFRESH` MVs. In particular, this means that the cluster will be off if there are no MVs with (non-default) `REFRESH` options on it at all. Other objects (e.g. MVs without `REFRESH` options or indexes), won't cause the cluster to turn on.

Also note that you can't manually specify or alter the `REPLICATION FACTOR` of a `SCHEDULE = ON REFRESH` cluster. If you need to turn on a `SCHEDULE = ON REFRESH` cluster outside its schedule (e.g. to test something), you can temporarily switch off the automatic scheduling by `ALTER CLUSTER c SET (SCHEDULE = MANUAL)`. In this case, don't forget to switch it back to `ON REFRESH ...` with another `ALTER CLUSTER` command later.

## Introspection

There are several internal introspection relations reflecting the state of MVs with `REFRESH` options and auto-scheduled clusters:
- [`mz_catalog.mz_cluster_replicas`](/sql/system-catalog/mz_catalog/#mz_cluster_replicas) shows whether a cluster is turned on, because the auto-scheduling turns clusters on/off by just creating/dropping a replica.
- [`mz_internal.mz_materialized_view_refresh_strategies`](/sql/system-catalog/mz_internal/#mz_materialized_view_refresh_strategies) shows the `REFRESH` options specified for each of your MVs. Alternatively, you can look at the definition of an MV by using `SHOW CREATE MATERIALIZED VIEW my_mv`.
- [`mz_internal.mz_materialized_view_refreshes`](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_materialized_view_refreshes) shows the time of the last successfully completed refresh and the time of the next scheduled refresh for each materialized view that has a non-default refresh option.
- [`mz_internal.mz_cluster_schedules`](/sql/system-catalog/mz_internal/#mz_cluster_schedules) shows the `SCHEDULE` option of each cluster.
- [`mz_catalog.mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events)'s `details` column shows the reason why the auto-scheduling created a replica, i.e., which MVs necessitated turning on a cluster.
