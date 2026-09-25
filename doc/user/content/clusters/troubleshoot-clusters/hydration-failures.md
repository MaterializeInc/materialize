---
title: "Troubleshoot hydration failures"
description: "Diagnose an object that never finishes hydrating, and the conditions that make hydration cost more than it used to."
menu:
  main:
    name: "Hydration failures"
    identifier: cluster-hydration-troubleshooting
    parent: "troubleshoot-clusters"
    weight: 30
---

[Hydration](/fundamentals/concepts/hydration/) rebuilds an object's in-memory
state by reading from Materialize's storage layer. Queries against an object
that is still hydrating block until it completes, so an object that never
finishes hydrating looks like a query that never returns, and a blue/green
deployment that waits on it never cuts over.

This guide covers objects that do not reach a hydrated state. For a hydration
that completes but spikes memory, see [Memory
spikes](/clusters/troubleshoot-clusters/memory-spike/). To size a cluster from
a hydration that did complete, see [Optimize cluster
size](/clusters/sizing/).

## Step 1: Find what has not hydrated

[`mz_internal.mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses)
reports one row per object per replica. Filtering on `id LIKE 'u%'` restricts
the result to user objects, leaving out Materialize's own introspection
indexes:

```mzsql
SELECT
    c.name AS cluster_name,
    o.name AS object_name,
    o.type,
    h.replica_id,
    h.hydrated
FROM mz_internal.mz_hydration_statuses AS h
JOIN mz_catalog.mz_objects AS o ON o.id = h.object_id
JOIN mz_catalog.mz_clusters AS c ON c.id = o.cluster_id
WHERE h.hydrated IS NOT TRUE
  AND o.id LIKE 'u%'
ORDER BY c.name, o.name;
```

```nofmt
 cluster_name | object_name |       type        | replica_id | hydrated
--------------+-------------+-------------------+------------+----------
 batch_jobs   | orders_idx  | index             |            | f
 batch_jobs   | orders_mv   | materialized-view |            | f
(2 rows)
```

An empty result means every object is hydrated. Otherwise, `replica_id` tells
you which case you are in:

- **Empty**, as in the output above: the cluster has no replicas, so nothing
  is hydrating the object. Give it one with [`ALTER CLUSTER ... SET
  (REPLICATION FACTOR = <int>)`](/sql/alter-cluster/). A cluster left at `0`
  also holds its inputs' history back, so read [Step
  3](#step-3-check-whether-an-inputs-history-is-pinned) before leaving it
  that way.
- **Set**: a replica is hydrating the object, or failing to. Continue to
  [Step 2](#step-2-check-for-a-rehydration-loop).

## Step 2: Check for a rehydration loop

Hydration is memory-intensive, and a replica that runs out of memory while
hydrating is killed and restarted, which begins hydration again from the
start. If the workload no longer fits the replica's size, every restart
repeats the same out-of-memory kill and the cluster never reaches a hydrated
state.

Hydration peaks well above steady state, because a dataflow holds both the
state it is rebuilding and the state it is reading. Steady-state memory is
therefore a poor guide to what the next restart will need, and a cluster sized
against it can stop fitting as data grows, with no change to the objects on
it.

```mzsql
SELECT
    rh.cluster_name,
    rh.replica_name,
    rh.size,
    count(*) FILTER (WHERE h.status = 'offline') AS offline_events,
    max(h.occurred_at) FILTER (WHERE h.reason = 'oom-killed') AS last_oom
FROM mz_internal.mz_cluster_replica_status_history AS h
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
WHERE h.occurred_at > now() - INTERVAL '1 day'
GROUP BY rh.cluster_name, rh.replica_name, rh.size
HAVING count(*) FILTER (WHERE h.status = 'offline') > 0
ORDER BY offline_events DESC;
```

```nofmt
 cluster_name | replica_name | size  | offline_events |        last_oom
--------------+--------------+-------+----------------+------------------------
 analytics    | r1           | 25cc  |             12 | 2026-09-23 15:53:41+00
(1 row)
```

Repeated `offline_events` over a short window is a rehydration loop. A
`last_oom` confirms memory as the cause. It stays `NULL` when the
orchestrator could not attribute the restart, so treat the restart count as
the primary signal.

{{< note >}}
Query the history rather than
[`mz_internal.mz_cluster_replica_statuses`](/sql/system-catalog/mz_internal/#mz_cluster_replica_statuses),
which reports only the current status and misses a restart that happens
between polls. Join to
[`mz_internal.mz_cluster_replica_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_history)
rather than `mz_catalog.mz_cluster_replicas`, so that replicas dropped by a
resize still resolve to a name.
{{< /note >}}

**Resolution**: size the cluster up with [`ALTER CLUSTER ... SET (SIZE =
'<new_size>')`](/sql/alter-cluster/) *before* the next restart or upgrade,
rather than during one. To choose a size from what the last successful
hydration actually required, see [Optimize cluster
size](/clusters/sizing/). To lower the peak instead of raising the
size, see [Optimize hydration
requirements](/clusters/optimize-hydration-requirements/).

## Step 3: Check whether an input's history is pinned

A new object starts reading data at the most recent time at which all of its
upstream inputs are readable. 

Materialize compacts historical data whenever possible, to reduce resource consumption. Under normal circumstances, this means that about 1 second of history is available, and so a new object will have to read just 1 second of upstream history to hydrate. However, if compaction did not succeed, the new object will need to replay more upstream history. This can take longer, and require more memory.

To determine if the input history is pinned, compare each object's "read frontier", against its "write frontier", using
[`mz_internal.mz_frontiers`](/sql/system-catalog/mz_internal/#mz_frontiers):

```mzsql
SELECT
    o.name AS input,
    o.type,
    f.read_frontier::timestamp AS readable_from,
    f.write_frontier::timestamp - f.read_frontier::timestamp AS retained_history
FROM mz_internal.mz_frontiers AS f
JOIN mz_catalog.mz_objects AS o ON o.id = f.object_id
WHERE o.id LIKE 'u%'
  AND f.read_frontier IS NOT NULL
  AND f.write_frontier IS NOT NULL
ORDER BY retained_history DESC
LIMIT 5;
```

```nofmt
   input      |       type        |      readable_from      | retained_history
--------------+-------------------+-------------------------+------------------
 orders       | table             | 2026-09-15 15:31:02.511 | 192:59:38.363
 shipments    | table             | 2026-09-23 18:30:40     | 00:00:01.874
 shipments_mv | materialized-view | 2026-09-23 18:30:40     | 00:00:01.874
(3 rows)
```

A healthy object retains about a second of history. Hours or days mean
something is holding its compaction back, and anything hydrating from it pays
for that history.


### Find what is holding compaction back

An object holds a read hold on its inputs at a time no greater than its own
write frontier, and a cluster with no replicas never advances that frontier.
Materialized views and sinks on such a cluster therefore pin their inputs for
as long as they exist, however idle the cluster itself is. List what a
zero-replica cluster still carries:

```mzsql
SELECT
    dep.name AS input,
    o.name AS object_name,
    o.type,
    c.name AS cluster_name
FROM mz_internal.mz_compute_dependencies AS d
JOIN mz_catalog.mz_objects AS dep ON dep.id = d.dependency_id
JOIN mz_catalog.mz_objects AS o ON o.id = d.object_id
JOIN mz_catalog.mz_clusters AS c ON c.id = o.cluster_id
WHERE c.replication_factor = 0
ORDER BY c.name, o.name;
```

```nofmt
 input  | object_name |       type        | cluster_name
--------+-------------+-------------------+--------------
 orders | orders_idx  | index             | batch_jobs
 orders | orders_mv   | materialized-view | batch_jobs
(2 rows)
```

Cross-check each input against `retained_history` before acting. Materialized
views and sinks are the ones that pin reliably. An index on a cluster with no
replica can instead be fast-forwarded past its stalled write frontier, because
no replica can serve reads from it anyway.

A replica that cannot make progress has the same effect on every object type:
the rehydration loop in [Step 2](#step-2-check-for-a-rehydration-loop) freezes
the write frontier, so the loop makes each successive restart more expensive
than the last.

**Resolution**: drop the objects whose inputs show a large
`retained_history`, or drop the cluster that carries them. Setting a
cluster's replication factor to `0` does not release its read holds, because
the objects remain. Only dropping them does. Capture the object definitions
with [`SHOW CREATE MATERIALIZED
VIEW`](/sql/show-create-materialized-view/) or [`SHOW CREATE
INDEX`](/sql/show-create-index/) before dropping anything you intend to
recreate.

{{< note >}}
Compaction is not scheduled. It happens as a side effect of other work, so
`retained_history` does not shrink the moment you drop the holders. The
immediate benefit is that objects created afterwards start from a current
time rather than the pinned one.
{{< /note >}}

Two cases do not hold compaction back, and need no action:

- Materialized views with a [`REFRESH
  EVERY`](/transform-data/patterns/refresh-strategies/) strategy, whose write
  frontiers advance to the next refresh time.
- Collections with an explicit `RETAIN HISTORY` window, which hold history back
  deliberately. Check for one in
  [`mz_internal.mz_history_retention_strategies`](/sql/system-catalog/mz_internal/#mz_history_retention_strategies)
  before treating a large `retained_history` as a fault.

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Optimize cluster size](/clusters/sizing/)
- [Optimize hydration requirements](/clusters/optimize-hydration-requirements/)
- [Memory spikes](/clusters/troubleshoot-clusters/memory-spike/)
- [Freshness troubleshooting](/transform-data/freshness-troubleshooting/)
