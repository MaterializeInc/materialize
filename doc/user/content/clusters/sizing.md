---
title: "Cluster sizing"
description: "Pick a cluster size by measuring what hydration actually needed, then sizing down."
menu:
  main:
    parent: "clusters"
    weight: 5
    name: "Cluster sizing"
    identifier: "cluster-sizing"
---

A cluster's [size](/sql/create-cluster/#available-sizes) fixes the CPU, memory,
and scratch disk available to every replica of that cluster, and on Materialize
Cloud it fixes the [cost](/materialize-cloud/billing/#compute). The size you
need is set by the most expensive thing the cluster does, and for most clusters
that is [hydration](/fundamentals/concepts/hydration/) rather than steady state.

Hydration is also the part you cannot predict from the query text. How much
memory a join or an aggregation needs depends on the data: key distribution,
skew, and how much history the inputs carry. So rather than estimate, start at a
size that hydrates comfortably, measure what hydration needed, and then size
down.

{{% include-headless "/headless/cluster-lifecycle" %}}

## Why hydration sets the size

Steady state is the cheap part of a cluster's life. Once a dataflow is hydrated
it holds its arrangements and applies incoming updates, and its memory tracks
the size of the state it maintains. Hydration is different: the replica rebuilds
that state from the storage layer, which means reading the inputs and building
the intermediate arrangements that produce it. Peak memory during hydration is
therefore higher than steady-state memory, often around twice as high, and the
same holds for the time it takes.

That gap decides two things at once:

- **A cluster that cannot hydrate serves nothing.** A replica that exceeds its
  memory allocation is restarted, and it then attempts the same hydration again.
  An undersized cluster does not degrade gracefully into a slow cluster: it
  restarts in a loop and never reaches the point where it can answer queries.

- **A cluster sized for steady state may not survive a restart.** The size that
  holds a hydrated dataflow can be too small to rebuild it. Restarts are not
  exceptional (a resize, a version upgrade, or a new index all trigger
  hydration), so the size has to cover the rebuild, not just the result.

Both point the same way: choose a size that hydrates, then reduce it with
evidence.

## Start large, then size down

The procedure below oversizes the cluster deliberately for one hydration, reads
what that hydration needed from the catalog, and uses those numbers to pick a
steady-state size.

Steps 3 through 5 read the durable hydration history relations:

{{< warn-if-unreleased v26.41 >}}

### 1. Create the cluster at a generous size

Pick a size you are confident can hydrate the workload, even if it is clearly
more than steady state needs. Oversizing costs money for as long as the cluster
runs at that size. Undersizing costs a hydration that never completes.

```mzsql
CREATE CLUSTER analytics (SIZE = '400cc');
```

Then create the cluster's indexes and materialized views as usual.

### 2. Wait for the cluster to hydrate

Every object has to finish hydrating before the numbers describe the whole
workload. Check that nothing is still hydrating:

```mzsql
SELECT o.name AS object, h.replica_id, h.hydrated
FROM mz_internal.mz_hydration_statuses AS h
JOIN mz_catalog.mz_objects AS o ON o.id = h.object_id
JOIN mz_catalog.mz_clusters AS c ON c.id = o.cluster_id
WHERE c.name = 'analytics' AND h.hydrated IS NOT TRUE;
```

An empty result means every object on the cluster is hydrated. A row with a
`NULL` `replica_id` is an object that has not attached to a replica yet, which
`IS NOT TRUE` catches along with `hydrated = false`. See [Lifecycle of a
cluster](#lifecycle-of-a-cluster) for the states that follow.

<a name="read-what-the-last-hydration-needed"></a>

### 3. Read what the last hydration needed

Materialize records completed hydration episodes durably, so the numbers survive
the replica restart or resize that produced them.
[`mz_internal.mz_replica_hydration_history`](/sql/system-catalog/mz_internal/#mz_replica_hydration_history)
holds one row per replica-wide episode, with the resource high-water marks
observed for it:

```mzsql
SELECT
    rh.replica_name AS replica,
    rh.size,
    h.started_at,
    h.finished_at - h.started_at AS hydration_time,
    h.object_count,
    pg_size_pretty(h.peak_memory_bytes) AS peak_memory,
    pg_size_pretty(h.peak_disk_bytes) AS peak_disk
FROM mz_internal.mz_replica_hydration_history AS h
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
WHERE rh.cluster_name = 'analytics'
ORDER BY h.started_at DESC;
```

```none
 replica | size  |          started_at           | hydration_time | object_count | peak_memory | peak_disk
---------+-------+-------------------------------+----------------+--------------+-------------+-----------
 r1      | 400cc | 2026-09-08 09:12:04.117841+00 | 00:04:11.83    |           41 | 11 GB       | 2438 MB
(1 row)
```

The join to
[`mz_internal.mz_cluster_replica_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_history)
is what makes the numbers usable for sizing: it supplies the size the episode
ran at, and it keeps that row after the replica is gone. Hydration history
itself stores only the replica ID, and a resize replaces the replica, so joining
[`mz_cluster_replicas`](/sql/system-catalog/mz_catalog/#mz_cluster_replicas)
instead would drop exactly the episodes you want to compare against.

Two columns need reading with care:

- `object_count` counts every maintained dataflow in the episode, which includes
  the system introspection dataflows each replica runs. It is normally a few
  dozen higher than the number of objects you created, and it is not the number
  of rows the per-object table holds for that replica.

- `peak_memory_bytes` and `peak_disk_bytes` are the largest values reported by
  any single process of the replica, not the sum across processes. Memory and
  disk limits apply per process, so the maximum is what answers whether any
  process came close to its limit. See [Reading the recorded
  numbers](#reading-the-recorded-numbers) for what the peaks do and do not
  cover.

To find which object dominated the episode, read the per-object table,
[`mz_internal.mz_object_hydration_history`](/sql/system-catalog/mz_internal/#mz_object_hydration_history).
Its `object_id` is the ID of the object's dataflow, so reach the catalog item
through
[`mz_internal.mz_object_global_ids`](/sql/system-catalog/mz_internal/#mz_object_global_ids):

```mzsql
SELECT
    rh.replica_name AS replica,
    o.name AS object,
    o.type,
    h.hydrated_at - h.installed_at AS hydration_time
FROM mz_internal.mz_object_hydration_history AS h
JOIN mz_internal.mz_object_global_ids AS g ON g.global_id = h.object_id
JOIN mz_catalog.mz_objects AS o ON o.id = g.id
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
WHERE rh.cluster_name = 'analytics'
ORDER BY hydration_time DESC
LIMIT 5;
```

```none
 replica |       object        |       type        | hydration_time
---------+---------------------+-------------------+-----------------
 r1      | auction_summary     | materialized-view | 00:04:11.83
 r1      | bids_by_auction     | materialized-view | 00:01:47.21
 r1      | bids_by_auction_idx | index             | 00:00:22.04
 r1      | auction_summary_idx | index             | 00:00:19.88
(4 rows)
```

Every replica records its own rows, so a cluster with a replication factor
above one, or one that has been resized, returns a row per object per replica.
The per-object table carries no resource columns, because peaks are measured per
process and a process runs many dataflows at once. Use it to find the object
whose hydration dominates the episode, then attribute the episode's peak to that
object's cluster placement. If one object accounts for most of the episode, [move
it to its own
cluster](/fundamentals/concepts/hydration/#hydration-strategies) so its
hydration peak stops dictating the size of everything else.

### 4. Choose a steady-state size

The episode's peak memory is what the smaller size has to fit, with headroom for
data growth. The following query reports, per cluster, the largest peak still in
the history and the smallest size whose per-process memory keeps that peak under
75%:

```mzsql
WITH observed AS (
    SELECT
        rh.cluster_name AS cluster,
        max(h.peak_memory_bytes) AS peak_memory_bytes
    FROM mz_internal.mz_replica_hydration_history AS h
    JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
    WHERE h.peak_memory_bytes IS NOT NULL
    GROUP BY rh.cluster_name
)
SELECT
    o.cluster,
    pg_size_pretty(o.peak_memory_bytes) AS peak_hydration_memory,
    (
        SELECT s.size
        FROM mz_catalog.mz_cluster_replica_sizes AS s
        WHERE o.peak_memory_bytes <= s.memory_bytes * 0.75
        ORDER BY s.memory_bytes
        LIMIT 1
    ) AS smallest_size_with_headroom
FROM observed AS o
ORDER BY o.cluster;
```

```none
  cluster  | peak_hydration_memory | smallest_size_with_headroom
-----------+-----------------------+-----------------------------
 analytics | 11 GB                 | 100cc
(1 row)
```

The 75% in that query is a starting point, not a guarantee. Raise the headroom
when the inputs are growing, when the workload is seasonal, or when the cluster
also serves ad-hoc `SELECT` queries, since those compete for the same memory and
are not part of a hydration episode.

Treat the result as the next size to try rather than the final answer. Sizing
down changes the thing you measured: fewer workers per replica changes how the
work is distributed, so the peak at `100cc` is not the peak at `400cc` divided
by four. Step down one size at a time and re-measure after each step.

{{< tip >}}
If a cluster's peak is dominated by hydration and its steady state is much
cheaper, you can keep it small and let it borrow capacity only while it
hydrates. An [`AUTO SCALING STRATEGY (ON
HYDRATION)`](/sql/alter-cluster/#speed-up-hydration-by-autoscaling-to-a-larger-size)
provisions an extra burst replica at a larger size whenever the cluster has
un-hydrated objects, including after a restart or an upgrade, and removes it once
a steady-size replica catches up. You then pay the hydration size only for the
duration of hydration.
{{< /tip >}}

### 5. Size down and confirm

A resize is graceful by default: Materialize hydrates replicas at the new size
alongside the current ones before retiring them, and rolls the resize back if
they do not hydrate within the reconfiguration timeout. See [resizing
process](/sql/alter-cluster/#resizing-process) for the details and how to change
that behavior.

```mzsql
ALTER CLUSTER analytics SET (SIZE = '100cc');
```

That rollback is what makes stepping down safe to try: a size that cannot
rebuild the state leaves the cluster where it was rather than serving nothing.

The resize also hydrates the whole workload again, which produces exactly the
measurement you need to confirm the new size. Re-run the query from [step
3](#read-what-the-last-hydration-needed) once the new replica is hydrated:

```none
 replica | size  |          started_at           | hydration_time | object_count | peak_memory | peak_disk
---------+-------+-------------------------------+----------------+--------------+-------------+-----------
 r2      | 100cc | 2026-09-08 10:41:22.913044+00 | 00:12:37.42    |           41 | 12 GB       | 4310 MB
 r1      | 400cc | 2026-09-08 09:12:04.117841+00 | 00:04:11.83    |           41 | 11 GB       | 2438 MB
(2 rows)
```

This is the outcome to look for, and it is also the point of measuring rather
than estimating. Peak memory barely moved, so `100cc` holds the workload with
the headroom the previous step asked for. Hydration got three times slower, which
is the cost of the smaller size, and whether that matters depends on how long
you can tolerate a restart taking.

If the new size is too small, no completed episode is recorded for the new
replica at all. Only successful hydration is recorded, so an out-of-memory
restart loop shows up as a missing row plus repeated restarts in
[`mz_internal.mz_cluster_replica_status_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_status_history):

```mzsql
SELECT sh.occurred_at, sh.process_id, sh.status, sh.reason
FROM mz_internal.mz_cluster_replica_status_history AS sh
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = sh.replica_id
WHERE rh.cluster_name = 'analytics'
ORDER BY sh.occurred_at DESC
LIMIT 10;
```

Repeated `offline` rows with an out-of-memory `reason`, and no new episode in
hydration history, mean the size cannot rebuild the state. Go back to the size
that hydrated, and take a smaller step, or reduce the peak itself with one of
the [hydration
strategies](/fundamentals/concepts/hydration/#hydration-strategies).

## Reading the recorded numbers

Hydration history is a best-effort record, not an audit log. Where it is
approximate, it is approximate in ways that matter for sizing:

- **Only successful episodes are recorded.** There is no row for a hydration
  that was killed, canceled, or is still running, and `status` is currently
  always `hydrated`. A missing row is a signal in its own right, as in step 5,
  but it is never a measurement of a failure.

- **Short-lived objects can be missed entirely.** Recording works by sampling
  each replica in a rotation, so an object that is dropped before its replica's
  turn leaves no trace. Nothing incorrect is recorded, the episode is simply
  absent.

- **The peaks are upper bounds on the episode.** They come from operating-system
  high-water marks that cover each process's whole lifetime up to the moment the
  episode is recorded, so post-hydration work can raise them, and a later
  episode can inherit an earlier episode's mark. For sizing this errs the safe
  way: the recorded value is never below the true hydration peak.

- **A peak can be `NULL`.** The values depend on what the platform exposes
  (a cgroup memory peak, and a scratch filesystem or swap peak), so they are
  absent rather than zero when a deployment does not report them.

- **Timestamps can carry clock skew.** On a multi-process replica the endpoints
  of an interval come from different process clocks, so a recorded duration
  includes their skew. This is not usually visible at the minute scale that
  matters for sizing.

- **Rows outlive what they name.** `replica_id`, `cluster_id`, and `object_id`
  may all name objects that no longer exist, which is what makes the history
  useful across a resize. Join `mz_cluster_replica_history` for replica and
  cluster names, and expect the join through `mz_object_global_ids` to drop
  objects that have since been dropped.

- **Rows are retained for 30 days by default.** Sizing decisions should come
  from the recent history rather than the earliest episode still stored.

Both tables live in the [`mz_internal`](/sql/system-catalog/mz_internal/)
schema, which is not part of Materialize's stable interface.

## If hydration history is empty

Recording is controlled by the `hydration_history_collection_interval` system
parameter, which sets how often Materialize samples replicas for completed
episodes. A value of zero disables recording, and the tables then stay as they
are: rows already collected remain, and no new ones are added.
`hydration_history_retention_period` bounds how long rows live, and defaults to
30 days.

On Materialize Cloud, these parameters are managed for you. If both tables are
empty for a cluster that has certainly hydrated, contact
[support](/support/).

On Materialize Self-Managed, set them as the `mz_system` user, or through the
[system parameters
ConfigMap](/self-managed-deployments/configuration-system-parameters/):

```mzsql
ALTER SYSTEM SET hydration_history_collection_interval = '60s';
```

A shorter interval records episodes sooner, at the cost of installing a dataflow
on a replica more often. Recording visits one replica per interval, so an
environment with many replicas revisits each one proportionally less often.

Until history is available, the current-state relations still answer the
narrower question of what is happening now:

| Relation | What it gives you |
|----------|-------------------|
| [`mz_internal.mz_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_hydration_statuses) | Per-object, per-replica hydration flag, for every object type. |
| [`mz_internal.mz_compute_hydration_statuses`](/sql/system-catalog/mz_internal/#mz_compute_hydration_statuses) | The same flag plus how long hydration took, for indexes and materialized views. |
| [`mz_internal.mz_cluster_replica_metrics_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics_history) | CPU, memory, and disk sampled about once a minute, retained for 30 days. |

The two hydration relations report only the current state and are reset by a
replica or Materialize restart. The metrics history survives restarts, but at
roughly one sample a minute it can miss a hydration spike entirely, and it does
not tell you which episode a sample belonged to. That is why these are a
fallback rather than the basis for a sizing decision.

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Clusters](/fundamentals/concepts/clusters/)
- [Operational guidelines](/clusters/operational-guidelines/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [Usage & billing](/materialize-cloud/billing/)
