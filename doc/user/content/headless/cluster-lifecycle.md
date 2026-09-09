---
headless: true
---
## Lifecycle of a cluster

Whenever a cluster starts running a workload (after you create it, resize it,
or one of its replicas restarts), its replicas move through a sequence of states
before results are fully up to date. Knowing which state a cluster is in tells
you whether it is making progress or is stuck.

The queries below monitor a cluster named `lifecycle_demo` that hosts the
materialized view `bids_by_auction` and its index `bids_by_auction_idx`, both
built on a continuously-updating `AUCTION` load-generator source. Substitute
your own cluster and object names.

### Provisioning

Replicas are scheduled and brought online. A cluster with a [replication
factor](/fundamentals/concepts/clusters/#cluster-replicas) of `0` has no
compute and never leaves this state. To monitor progress, check that replicas
report `online` in
[`mz_cluster_replica_statuses`](/sql/system-catalog/mz_internal/#mz_cluster_replica_statuses),
and confirm the cluster has replicas via
[`mz_clusters`](/sql/system-catalog/mz_catalog/#mz_clusters).

```mzsql
SELECT c.name AS cluster, r.name AS replica, r.size, st.status, st.reason
FROM mz_internal.mz_cluster_replica_statuses st
JOIN mz_catalog.mz_cluster_replicas r ON r.id = st.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'lifecycle_demo'
ORDER BY r.name;
```

```none
    cluster     | replica | size | status | reason
----------------+---------+------+--------+--------
 lifecycle_demo | r1      | 25cc | online |
(1 row)
```

The `reason` column is empty while the replica is `online`, and reports why a
replica is unavailable otherwise.

### Hydrating

Each replica reconstructs its in-memory state by reading from Materialize's
storage layer (see [hydration](/fundamentals/concepts/hydration/)). While an object is
hydrating, its `hydrated` flag reads `f` and its lag is reported as `NULL`. To
monitor progress, check the `hydrated` flag per object in
[`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses),
where the `replica_id` stays blank until the object attaches to a replica. For
indexes and materialized views,
[`mz_compute_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_compute_hydration_statuses)
also reports how long hydration took.

```mzsql
SELECT o.name AS object, o.type, r.name AS replica, ch.hydrated, ch.hydration_time
FROM mz_internal.mz_compute_hydration_statuses ch
JOIN mz_objects o ON o.id = ch.object_id
JOIN mz_catalog.mz_cluster_replicas r ON r.id = ch.replica_id
WHERE o.name IN ('bids_by_auction', 'bids_by_auction_idx', 'bids_load')
ORDER BY o.name;
```

```none
       object        |       type        | replica | hydrated | hydration_time
---------------------+-------------------+---------+----------+-----------------
 bids_by_auction     | materialized-view | r1      | t        | 00:00:00.000074
 bids_by_auction_idx | index             | r1      | t        | 00:00:00.000019
 bids_load           | materialized-view | r1      | t        | 00:00:05.6032
(3 rows)
```

The light view and index hydrate in microseconds, while the larger `bids_load`
view takes about 5.6 seconds. A larger object with more state to reconstruct
shows a longer, more visible hydration window.

Both relations report only the current state, so they are wiped when a replica
or Materialize restarts. To compare this hydration against earlier ones, read
the durable [hydration
history](/clusters/sizing/#read-what-the-last-hydration-needed) instead.

### Catching up

Once hydrated, the cluster processes the backlog of input updates that
accumulated while it was unavailable, so its total lag starts high and comes
down. To monitor progress, watch `lag` decrease in
[`mz_wallclock_global_lag_recent_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history),
or break the lag down by input with
[`mz_materialization_lag`](/sql/system-catalog/mz_internal/#mz_materialization_lag).

```mzsql
SELECT o.name AS object, l.local_lag, l.global_lag,
       si.name AS slowest_local_input, sg.name AS slowest_global_input
FROM mz_internal.mz_materialization_lag l
JOIN mz_objects o ON o.id = l.object_id
LEFT JOIN mz_objects si ON si.id = l.slowest_local_input_id
LEFT JOIN mz_objects sg ON sg.id = l.slowest_global_input_id
WHERE o.name IN ('bids_by_auction', 'bids_load')
ORDER BY o.name;
```

```none
     object      |    local_lag     |    global_lag    | slowest_local_input | slowest_global_input
-----------------+------------------+------------------+---------------------+----------------------
 bids_by_auction | 00:00:34.001     | 00:00:34.001     | bids                | bids
 bids_load        | 00:00:41.001     | 00:00:41.001     | bids                | bids
```

Both objects trail their slowest input, the `bids` source, by tens of seconds.
As the cluster works through the backlog, these lags fall.

### Steady state

The cluster has caught up and its lag holds low and roughly constant, typically
a few seconds. Re-running the lag query confirms the objects have caught up to
their input.

```mzsql
SELECT o.name AS object, l.local_lag, l.global_lag,
       si.name AS slowest_local_input, sg.name AS slowest_global_input
FROM mz_internal.mz_materialization_lag l
JOIN mz_objects o ON o.id = l.object_id
LEFT JOIN mz_objects si ON si.id = l.slowest_local_input_id
LEFT JOIN mz_objects sg ON sg.id = l.slowest_global_input_id
WHERE o.name = 'bids_by_auction';
```

```none
     object      | local_lag | global_lag | slowest_local_input | slowest_global_input
-----------------+-----------+------------+---------------------+----------------------
 bids_by_auction | 00:00:00  | 00:00:00   | bids                | bids
(1 row)
```

Wallclock lag in
[`mz_wallclock_global_lag_recent_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history)
holds near-constant at a few seconds. A lag that instead climbs steadily, at
about one minute per minute, means the cluster has stopped making progress.

{{< note >}}
Sources go through an additional
[snapshotting](/fundamentals/concepts/snapshotting/) step the first time they run, reading the
initial state of the upstream system before the states above apply. See
[Troubleshooting](/transform-data/freshness-troubleshooting/) for how to
diagnose a cluster that is not progressing through these states.
{{< /note >}}
