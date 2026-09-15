---
title: "Understand the lifecycle of a cluster"
description: "The stages a cluster moves through before its results are up to date, how to monitor each one, and the states that mean it is stuck."
menu:
  main:
    parent: "clusters"
    name: "Understand the lifecycle of a cluster"
    identifier: cluster-lifecycle
    weight: 6
---

Whenever a cluster starts running a workload, it moves through a sequence of
stages before its results are fully up to date. Knowing which stage a cluster
is in tells you whether it is making progress or is stuck.

A cluster enters this sequence when you create it,
[resize](/sql/alter-cluster/#resizing) it, or raise its [replication
factor](/sql/alter-cluster/#replication-factor), and whenever a replica
restarts, including during Materialize Cloud's routine maintenance and after an
out-of-memory event.

## Stages

| Stage                         | What is happening                                                | Where to look                            |
|-------------------------------|------------------------------------------------------------------|------------------------------------------|
| [Provisioning](#provisioning) | Replicas are scheduled and brought online.                        | `mz_cluster_replica_statuses`            |
| [Hydrating](#hydrating)       | Each replica rebuilds its in-memory state from the storage layer. | `mz_hydration_statuses`                  |
| [Catching up](#catching-up)   | The cluster works through the backlog of updates, so lag falls.   | `mz_wallclock_global_lag_recent_history` |
| [Steady state](#steady-state) | The cluster keeps up with its inputs and lag holds low.           | `mz_wallclock_global_lag_recent_history` |

No single column reports these stages. A replica's `status` is only `online` or
`offline`, so it answers the provisioning question and nothing else. Hydration
and lag are separate signals, and you need both: a hydrated object has
processed the snapshot of its inputs, but not the updates that arrived while it
was doing so, and a low lag on an unhydrated object does not mean its results
are available.

Two states interrupt the sequence: a cluster with [no replicas](#no-compute)
never leaves provisioning, and a replica that goes [offline](#offline-replicas)
restarts and re-enters the sequence from the top.

The queries below monitor this cluster. Substitute your own object names.

```mzsql
CREATE CLUSTER lifecycle_demo SIZE '25cc';

CREATE SOURCE auction_load IN CLUSTER lifecycle_demo
  FROM LOAD GENERATOR AUCTION (TICK INTERVAL '1s') FOR ALL TABLES;

CREATE MATERIALIZED VIEW bids_by_auction IN CLUSTER lifecycle_demo AS
  SELECT auction_id, count(*) AS bids, max(amount) AS max_bid
  FROM bids
  GROUP BY auction_id;

CREATE INDEX bids_by_auction_idx IN CLUSTER lifecycle_demo
  ON bids_by_auction (auction_id);
```

## Provisioning

Replicas are scheduled and brought online. To monitor progress, check that each
replica reports `online` in
[`mz_cluster_replica_statuses`](/sql/system-catalog/mz_internal/#mz_cluster_replica_statuses):

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

`status` is `online` or `offline`. `reason` is `NULL` while a replica is
online, and otherwise reports `initializing` or `oom-killed`.

A [resize](/sql/alter-cluster/#resizing) passes through this stage without
downtime: Materialize provisions replicas at the target size and hydrates them
before retiring the old ones, so this query reports replicas at both sizes
until the cutover. The new replicas are new objects with new IDs and names, so
a `r1` that becomes `r2` is a resize, not a restart. See [Monitoring a
resize](/sql/alter-cluster/#monitoring-a-resize).

### No compute

A cluster with a [replication factor](/sql/alter-cluster/#replication-factor)
of `0` has no replicas, so it never leaves provisioning. The query above
returns no rows at all rather than an `offline` status, which is worth knowing
if you alert on it. Queries routed to the cluster fail immediately:

```none
ERROR:  CLUSTER "lifecycle_demo" has no replicas available to service request
HINT:  Use ALTER CLUSTER to adjust the replication factor of the cluster.
```

A query run from a *different* cluster against an object maintained by this one
does not error. It blocks until the object's frontier advances, which it never
will.

To find clusters in this state:

```mzsql
SELECT name, replication_factor
FROM mz_catalog.mz_clusters
WHERE replication_factor = 0;
```

Scaling a cluster to zero between scheduled runs saves compute, but it is not
free: its objects hold back compaction of their inputs, so the cluster faces
more work, and a higher memory peak, when it comes back. Prefer dropping and
recreating such clusters over parking them at zero for long stretches.

## Hydrating

Each replica reconstructs its in-memory state by reading from Materialize's
storage layer (see [hydration](/fundamentals/concepts/hydration/)).
[`mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)
reports the flag for every object a cluster maintains, sources and sinks
included:

```mzsql
SELECT o.name AS object, o.type, h.hydrated
FROM mz_internal.mz_hydration_statuses h
JOIN mz_objects o ON o.id = h.object_id
JOIN mz_catalog.mz_cluster_replicas r ON r.id = h.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'lifecycle_demo' AND o.id LIKE 'u%'
ORDER BY o.name;
```

```none
       object        |       type        | hydrated
---------------------+-------------------+----------
 accounts            | source            | t
 auction_load        | source            | t
 auctions            | source            | t
 bids                | source            | t
 bids_by_auction     | materialized-view | t
 bids_by_auction_idx | index             | t
 organizations       | source            | t
 users               | source            | t
(8 rows)
```

The `o.id LIKE 'u%'` filter excludes the built-in introspection indexes that
every replica carries. The join to `mz_cluster_replicas` is load-bearing:
`mz_hydration_statuses` keeps rows for replicas that no longer exist, and
without the join a retired replica's stale `hydrated` values contradict the
live ones.

Sources hydrate more slowly than indexes and materialized views, often by a
minute or more on a fresh replica, so an all-object table like the one above
reads mixed for a while. To narrow the view to indexes and materialized views,
and to see how long each took,
[`mz_compute_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_compute_hydration_statuses)
adds a `hydration_time` column:

```mzsql
SELECT o.name AS object, o.type, r.name AS replica, ch.hydrated, ch.hydration_time
FROM mz_internal.mz_compute_hydration_statuses ch
JOIN mz_objects o ON o.id = ch.object_id
JOIN mz_catalog.mz_cluster_replicas r ON r.id = ch.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'lifecycle_demo' AND o.id LIKE 'u%'
ORDER BY o.name;
```

```none
       object        |       type        | replica | hydrated | hydration_time
---------------------+-------------------+---------+----------+-----------------
 bids_by_auction     | materialized-view | r1      | t        | 00:00:00.00011
 bids_by_auction_idx | index             | r1      | t        | 00:00:00.000019
(2 rows)
```

Judge a replica by its objects collectively rather than one at a time. An
object that finishes early reports a freshness number while its neighbours are
still hydrating and competing for CPU, so that number looks bad and alerting on
it produces false positives. Treat a replica as ready only once every object on
it is hydrated:

```mzsql
SELECT r.name AS replica, bool_and(h.hydrated) AS replica_hydrated
FROM mz_internal.mz_hydration_statuses h
JOIN mz_objects o ON o.id = h.object_id
JOIN mz_catalog.mz_cluster_replicas r ON r.id = h.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE c.name = 'lifecycle_demo' AND o.id LIKE 'u%'
GROUP BY r.name;
```

A cluster with no replicas returns no rows here, not `false`.

Hydration is per replica, so adding a replica or resizing a cluster hydrates
only the new replicas while the existing ones keep serving. It is also the
memory peak of a cluster's life, and that peak is higher than steady-state
usage, so a cluster that runs comfortably for weeks can still fail to come back
after a restart. Size clusters against the peak rather than the steady state,
and consider [autoscaling](/clusters/autoscaling/) to a larger size for the
duration.

## Catching up

Once hydrated, the cluster processes the backlog of updates that accumulated
while it was unavailable, so its lag starts high and comes down.
[`mz_wallclock_global_lag_recent_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history)
records how far each object trails wallclock time, binned by minute over the
last 24 hours:

```mzsql
SELECT o.name AS object, w.occurred_at, w.lag
FROM mz_internal.mz_wallclock_global_lag_recent_history w
JOIN mz_objects o ON o.id = w.object_id
WHERE o.name = 'bids_by_auction'
ORDER BY w.occurred_at DESC
LIMIT 8;
```

```none
     object      |      occurred_at       |   lag
-----------------+------------------------+----------
 bids_by_auction | 2026-09-15 17:33:00+00 | 00:00:01
 bids_by_auction | 2026-09-15 17:32:00+00 | 00:00:01
 bids_by_auction | 2026-09-15 17:31:00+00 | 00:00:01
 bids_by_auction | 2026-09-15 17:30:00+00 | 00:00:01
 bids_by_auction | 2026-09-15 17:29:00+00 | 00:00:01
 bids_by_auction | 2026-09-15 17:28:00+00 | 00:01:42
 bids_by_auction | 2026-09-15 17:27:00+00 | 00:00:42
 bids_by_auction | 2026-09-15 17:26:00+00 | 00:00:01
(8 rows)
```

The cluster lost its replica just after 17:26. Lag climbs to 42 seconds, then
to 1 minute 42 seconds, then drops back to a second once the replacement
replica hydrates and works through the backlog. That shape, a climb followed by
a return to baseline, is what catching up looks like.

## Steady state

The cluster has caught up and its lag holds low and roughly constant, typically
a few seconds, as in the rows from 17:29 onward above. A lag that instead
climbs steadily, at about one minute per minute, means the cluster has stopped
making progress.

To attribute lag to a specific input,
[`mz_materialization_lag`](/sql/system-catalog/mz_internal/#mz_materialization_lag)
reports each object's distance from its direct inputs and from the sources and
tables at the root of its dependency graph:

```mzsql
SELECT o.name AS object, l.local_lag, l.global_lag,
       si.name AS slowest_local_input, sg.name AS slowest_global_input
FROM mz_internal.mz_materialization_lag l
JOIN mz_objects o ON o.id = l.object_id
JOIN mz_objects si ON si.id = l.slowest_local_input_id
JOIN mz_objects sg ON sg.id = l.slowest_global_input_id
WHERE o.name IN ('bids_by_auction', 'bids_by_auction_idx')
ORDER BY o.name;
```

```none
       object        | local_lag | global_lag | slowest_local_input | slowest_global_input
---------------------+-----------+------------+---------------------+----------------------
 bids_by_auction     | 00:00:00  | 00:00:00   | bids                | bids
 bids_by_auction_idx | 00:00:00  | 00:00:00   | bids_by_auction     | bids
(2 rows)
```

The index trails its direct input, the materialized view, and the view trails
the `bids` source, which is the root input for both.

{{< note >}}
These lags are measured against inputs, not against wallclock time. When a
whole cluster stalls, the objects on it stall together and this query keeps
reporting `00:00:00`. Use it to find *which* input a lagging object is waiting
on, and wallclock lag to decide whether the object is lagging at all.
{{< /note >}}

See [Troubleshooting freshness](/transform-data/freshness-troubleshooting/) to
diagnose a cluster that is not progressing, and [Monitor
freshness](/transform-data/monitor-freshness/) to track lag over time.

## Offline replicas

A replica that becomes unavailable reports `offline`, then restarts and
re-enters the lifecycle at [provisioning](#provisioning), rehydrating
everything it hosts. The cluster's lag grows until it catches up again.

A point-in-time status misses a replica that died and recovered between two
readings, so read
[`mz_cluster_replica_status_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_status_history)
rather than polling the current status:

```mzsql
SELECT h.replica_id, rh.replica_name, h.status, h.reason, h.occurred_at
FROM mz_internal.mz_cluster_replica_status_history h
JOIN mz_internal.mz_cluster_replica_history rh ON rh.replica_id = h.replica_id
WHERE rh.cluster_name = 'lifecycle_demo'
ORDER BY h.occurred_at DESC
LIMIT 20;
```

Resolve replica names through
[`mz_cluster_replica_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_history),
not `mz_cluster_replicas`. A replica that was retired or replaced is gone from
`mz_cluster_replicas`, so joining against it drops exactly the events a history
query is for.

Reading the history:

- A single `offline` followed by `online` on the same `replica_id` is a
  restart, and is routine.
- The same pair on a *new* `replica_id` is a replica being created. A resize or
  a blue/green swap changes which replicas back a cluster, so key on
  `replica_id`, not on the cluster or replica name.
- A repeating `offline` with reason `oom-killed` is a crash loop: the replica
  is too small for its workload, and each restart triggers a rehydration that
  exhausts memory again. See [Check for OOM crash
  loops](/transform-data/freshness-troubleshooting/#check-for-oom-crash-loops).
- `reason` is best-effort and is sometimes empty even for an out-of-memory
  kill, so treat repeated restarts as the signal rather than the reason string.

{{< note >}}
Sources on the cluster go through an additional
[snapshotting](/fundamentals/concepts/snapshotting/) step the first time they
run, reading the initial state of the upstream system before the stages above
apply. See [Understand the lifecycle of a
source](/ingest-data/lifecycle-of-a-source/).
{{< /note >}}

## Related pages

- [Clusters](/fundamentals/concepts/clusters/)
- [Hydration](/fundamentals/concepts/hydration/)
- [Autoscaling](/clusters/autoscaling/)
- [Troubleshooting freshness](/transform-data/freshness-troubleshooting/)
- [Understand the lifecycle of a source](/ingest-data/lifecycle-of-a-source/)
