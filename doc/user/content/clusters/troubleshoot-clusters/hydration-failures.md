---
title: "Troubleshoot hydration failures"
description: "Diagnose a cluster that never finishes hydrating, and the conditions that make hydration cost more than it used to."
menu:
  main:
    name: "Hydration failures"
    identifier: cluster-hydration-troubleshooting
    parent: "troubleshoot-clusters"
    weight: 30
---

[Hydration](/fundamentals/concepts/hydration/) rebuilds the in-memory state of
the objects on a cluster by reading from Materialize's storage layer. A cluster
is hydrated once every object on it is. A query served by an index that is
still hydrating blocks until that index is ready, so a cluster that never
finishes hydrating looks like a query that never returns, and a blue/green
deployment that waits on it never cuts over. Reading a materialized view
without an index goes to its persist shard instead, and waits only when the
shard has not yet reached the query's timestamp.

This guide helps you diagnose a cluster that never reaches a hydrated state.
For a hydration that completes but spikes memory, see [Memory
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

- **`NULL`**, as in the output above: no replica has reported on the object.
  Usually the cluster has no replicas, so nothing is hydrating it. Give it one
  with [`ALTER CLUSTER ... SET (REPLICATION FACTOR =
  <int>)`](/sql/alter-cluster/). A replica that has only just started also
  reports `NULL` until it reports in, so re-run the query before acting on a
  cluster that does have replicas. A cluster left at `0` additionally pins the
  history of its inputs, so read [Step
  3](#step-3-check-whether-an-inputs-history-is-pinned) before leaving it that
  way.
- **Set**: that replica is hydrating the object, or failing to. Continue to
  [Step 2](#step-2-check-for-a-rehydration-loop).

Rows are per object per replica, so on a cluster with more than one replica an
object appears here while another replica may already be serving it.

## Step 2: Check for a rehydration loop

Hydration is memory-intensive, and a replica that runs out of memory while
hydrating is killed and restarted, which begins hydration again from the
start. If the workload no longer fits the replica's size, every restart
repeats the same out-of-memory kill and the cluster never reaches a hydrated
state.

Hydration peaks well above steady state. Steady-state memory is therefore a
poor guide to what the next restart will need, and a cluster sized against it
can stop fitting as data grows, with no change to the objects on it.

```mzsql
SELECT
    rh.cluster_name,
    rh.replica_name,
    rh.size,
    count(DISTINCT h.occurred_at) FILTER (WHERE h.status = 'offline') AS offline_events,
    max(h.occurred_at) FILTER (WHERE h.reason = 'oom-killed') AS last_oom
FROM mz_internal.mz_cluster_replica_status_history AS h
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
WHERE h.occurred_at > now() - INTERVAL '1 day'
GROUP BY rh.cluster_name, rh.replica_name, rh.size
HAVING count(DISTINCT h.occurred_at) FILTER (WHERE h.status = 'offline') > 0
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
resize still resolve to a name. A multi-process replica records one row per
process per restart, which is why the count is over distinct `occurred_at`
rather than over rows.
{{< /note >}}

**Resolution**: size the cluster up with [`ALTER CLUSTER ... SET (SIZE =
'<new_size>')`](/sql/alter-cluster/). To choose a size from what the last
successful hydration actually required, see [Optimize cluster
size](/clusters/sizing/). To lower the peak instead of raising the size, see
[Optimize hydration
requirements](/clusters/optimize-hydration-requirements/).

## Step 3: Check whether an input's history is pinned

A new object starts reading at an `as_of` equal to the latest of its inputs'
read frontiers, which is the earliest time at which all of them are readable
at once.

Materialize compacts historical data whenever possible, to reduce resource
consumption. Under normal circumstances, this means that about 1 second of
history is available, and so a new object will have to replay only about 1
second of upstream history on top of the current snapshot. However, if
compaction has been held back, the new object will need to replay more
upstream history. This can take longer, and require more memory.

One pinned input is not enough on its own to cause that. Because the `as_of`
takes the *latest* of the read frontiers, a single current input keeps it from
being dragged back. An object starts far in the past only when **every** one of
its inputs is pinned. A pinned input still retains more unconsolidated updates,
which can make reading it more expensive, but it no longer sets where the
object starts.

To determine if the input history is pinned, compare each object's "read
frontier" against its "write frontier", using
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

A healthy object retains about a second of history. Hours or days mean its
compaction is being held back.

A single row here is not a diagnosis. By the `as_of` rule above, pinned inputs
only slow an object down when *all* of its inputs are pinned, so check the
whole input set of the object that will not hydrate rather than acting on the
worst row.
[`mz_internal.mz_compute_dependencies`](/sql/system-catalog/mz_internal/#mz_compute_dependencies)
lists an object's inputs.

An explicit `RETAIN HISTORY` window is an expected reason for hours or days of
retained history, not a fault. Check
[`mz_internal.mz_history_retention_strategies`](/sql/system-catalog/mz_internal/#mz_history_retention_strategies)
before treating a large `retained_history` as a problem.

### Find what is holding compaction back

An object holds a read hold on its inputs at a time no greater than its own
write frontier, so anything whose write frontier has stopped advancing pins the
history of everything it reads. Indexes on a cluster with no replicas are the
exception, covered below.

#### Possible cause: an object cannot finish hydrating

The rehydration loop in [Step 2](#step-2-check-for-a-rehydration-loop) freezes
that object's write frontier, so it pins its own inputs for as long as the loop
runs, and each restart has more history to replay than the last. Resolve it as
described in [Step 2](#step-2-check-for-a-rehydration-loop).

#### Possible cause: a cluster has a replication factor of `0`

A cluster with `REPLICATION FACTOR` set to `0` never advances the write
frontiers of the objects on it. Materialized views and sinks there go on
pinning the history of their inputs for as long as they exist.

An index on such a cluster does not pin its inputs by itself: with no replica
to serve reads from it, the controller advances its read hold anyway, and the
holds it places on its own inputs follow. An index only pins when something
else holds its since back, such as a materialized view or sink on the same
cluster that reads from it.

List what such a cluster still carries:

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
 orders | orders_mv   | materialized-view | batch_jobs
(1 row)
```

[`mz_internal.mz_compute_dependencies`](/sql/system-catalog/mz_internal/#mz_compute_dependencies)
covers indexes and materialized views but not sinks, so a sink on the cluster
pins its inputs without appearing here. Index rows are worth reading against
the rule above: an index listed here is pinning only if a materialized view or
sink on the same cluster reads from it.

**Resolution**: drop the materialized views and sinks the cluster carries,
along with any index one of them reads from. The read holds belong to the
objects rather than to the cluster, so dropping the cluster works only because
it takes its objects with it. Setting a cluster's replication factor to `0` is
usually what caused the problem, and never fixes it. An index the cluster
carries on its own is not holding anything back and does not need dropping.

Dropping is destructive. Dropping a materialized view discards its persisted
output, so recreating it hydrates it again from its inputs. One with dependents
needs [`DROP MATERIALIZED VIEW ... CASCADE`](/sql/drop-materialized-view/),
which drops those dependents too and leaves you to recreate them. Capture the
definitions with [`SHOW CREATE MATERIALIZED
VIEW`](/sql/show-create-materialized-view/) or [`SHOW CREATE
INDEX`](/sql/show-create-index/) first.

Once the objects are gone, the read frontiers of their inputs advance again and
a new deployment can follow within seconds.

Do not restore a replica to a cluster that has sat at `0` for days while it
still carries a materialized view or sink. The replica rehydrates that object,
and any index it reads from, through the whole retained backlog, which is the
out-of-memory case in [Step
2](#step-2-check-for-a-rehydration-loop). Drop those objects and recreate them
instead. A cluster carrying only indexes does not have this problem, because
their read holds were forwarded while it was paused and a new replica starts
them near the present.

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Optimize cluster size](/clusters/sizing/)
- [Optimize hydration requirements](/clusters/optimize-hydration-requirements/)
- [Memory spikes](/clusters/troubleshoot-clusters/memory-spike/)
- [Freshness troubleshooting](/transform-data/freshness-troubleshooting/)
