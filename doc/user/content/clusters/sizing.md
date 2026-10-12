---
title: "Size clusters for hydration"
description: "Measure the resources your cluster requires to hydrate, and optimize your cluster size accordingly"
menu:
  main:
    parent: "clusters"
    weight: 5
    name: "Optimize cluster size"
    identifier: "cluster-sizing"
---

A cluster's [size](/sql/create-cluster/#available-sizes) defines the CPU,
memory, and disk available to every replica. On Materialize Cloud, this
determines the [cost](/materialize-cloud/billing/#compute) of the cluster.
Clusters should be provisioned for peak resource usage, to ensure that they can
handle the load placed on them. For most clusters, peak resource usage happens
during [hydration](/fundamentals/concepts/hydration/).

This guide will walk you through how to estimate resources required for
hydration. Before reading this guide, make sure you understand the [lifecycle of
a cluster](/fundamentals/concepts/clusters/#lifecycle-of-a-cluster).

{{< note >}}
Hydration rebuilds a dataflow's in-memory state from the storage layer, which
can take more memory than maintaining that state afterwards. The size that holds a
hydrated cluster is therefore not always the size that can rebuild it, and a
replica that runs out of memory while hydrating restarts and tries again rather
than running slower.
{{< /note >}}

## Determine the right size by starting large, and then size down

This guide assumes you are running Materialize v26.42 or later. v26.42 added
improvements to allow you to track peak resource usage during hydration.

{{< note >}}
**Multi-process replicas.** The recorded peaks are a single process's
high-water marks, not the replica's. On a multi-process size they do not capture
the replica's true peak, and no way to combine the per-process marks into one is
established. This guide assumes a single-process size.
{{< /note >}}

### 1. Create the cluster at a generous size

Pick a size you are confident can hydrate the workload, even if it is clearly
more than steady state needs.

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
`IS NOT TRUE` catches along with `hydrated = false`.

<a name="read-what-the-last-hydration-needed"></a>

### 3. Read what the last hydration required

Materialize records completed hydration episodes. [`mz_internal.mz_replica_hydration_history`](/sql/system-catalog/mz_internal/#mz_replica_hydration_history)
holds one row per replica-wide hydration episode:

```mzsql
SELECT
    rh.replica_name AS replica,
    rh.size,
    h.started_at,
    h.finished_at - h.started_at AS hydration_time,
    h.object_count,
    pg_size_pretty(h.peak_memory_bytes + coalesce(h.peak_disk_bytes, 0)) AS peak_heap,
    pg_size_pretty(s.memory_bytes + coalesce(s.disk_bytes, 0)) AS heap_limit
FROM mz_internal.mz_replica_hydration_history AS h
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
JOIN mz_catalog.mz_clusters AS c ON c.id = rh.cluster_id
JOIN mz_catalog.mz_cluster_replica_sizes AS s ON s.size = rh.size
WHERE c.name = 'analytics'
ORDER BY h.started_at DESC;
```

```none
 replica | size  |          started_at           | hydration_time | object_count | peak_heap | heap_limit
---------+-------+-------------------------------+----------------+--------------+-----------+------------
 r1      | 400cc | 2026-09-08 09:12:04.117841+00 | 00:04:11.83    |           41 | 13 GB     | 152 GB
(1 row)
```

`peak_heap` adds a process's memory and disk high-water marks
(`peak_memory_bytes` and `peak_disk_bytes`). Both marks cover the process's
whole life up to the moment the episode was recorded. A page moved back from
disk to memory also counts in both marks. As a result, `peak_heap` can run
higher than the hydration itself needed, which errs on the safe side for
sizing. `heap_limit` is the memory plus disk the size provides, from
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes).
Both figures are per process. Compare them to determine the ideal cluster size.

On Materialize Self-Managed, sizes with swap enabled, the default, report a
`disk_bytes` of 0, because Materialize does not limit their swap. There, this
`heap_limit` shows memory only. Compare `peak_heap` with the `heap_limit` of a
running replica of that size in
[`mz_internal.mz_cluster_replica_metrics`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics)
instead, which includes the swap available to the replica.

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
JOIN mz_catalog.mz_clusters AS c ON c.id = rh.cluster_id
WHERE c.name = 'analytics'
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
If one object accounts for most of the episode, [move it to its own
cluster](/clusters/optimize-hydration-requirements/) so its
hydration peak stops dictating the size of everything else.

### 4. Size down

Once you have identified the appropriate size, you can downsize by altering the
cluster:

```mzsql
ALTER CLUSTER analytics SET (SIZE = '100cc');
```

`ALTER CLUSTER` operations are graceful. This means the smaller cluster will
hydrate in parallel, and Materialize will cut over to the smaller cluster when
it is ready.

The resize also hydrates the whole workload again, which produces exactly the
measurement you need to confirm the new size. Re-run the query from [step
3](#read-what-the-last-hydration-needed) once the new replica is hydrated:

```none
 replica | size  |          started_at           | hydration_time | object_count | peak_heap | heap_limit
---------+-------+-------------------------------+----------------+--------------+-----------+------------
 r2      | 100cc | 2026-09-08 10:41:22.913044+00 | 00:12:37.42    |           41 | 16 GB     | 38 GB
 r1      | 400cc | 2026-09-08 09:12:04.117841+00 | 00:04:11.83    |           41 | 13 GB     | 152 GB
(2 rows)
```

If the new size is too small, no completed episode is recorded for the new
replica at all. Only successful hydration is recorded, so an out-of-memory
restart loop shows up as a missing row plus repeated restarts in
[`mz_internal.mz_cluster_replica_status_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_status_history):

```mzsql
SELECT sh.occurred_at, sh.process_id, sh.status, sh.reason
FROM mz_internal.mz_cluster_replica_status_history AS sh
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = sh.replica_id
JOIN mz_catalog.mz_clusters AS c ON c.id = rh.cluster_id
WHERE c.name = 'analytics'
ORDER BY sh.occurred_at DESC
LIMIT 10;
```

If you see repeated `offline` rows with an out-of-memory `reason`, that means
the new size is too small. Size up, or consider [optimizing hydration
requirements](/clusters/optimize-hydration-requirements/) to reduce the memory
required for hydration.

## How should I interpret the hydration metrics?

Hydration history is a best-effort record, not an audit log. Where it is
approximate, it is approximate in ways that matter for sizing:

- **Only successful episodes are recorded.** There is no row for a hydration
  that was killed, canceled, or is still running, and `status` is currently
  always `hydrated`. A missing row is a signal in its own right, as in step 4,
  but it is never a measurement of a failure.

- **Only indexes and materialized views are tracked per object.** Sources,
  including upsert sources, contribute no rows to object history and do not hold
  a replica episode open. The replica peaks measure whole processes, so they
  include a source's memory and disk only for the work it had finished by the
  moment the episode was recorded. An episode closes on the compute dataflows,
  and [snapshotting](/fundamentals/concepts/snapshotting/) an upsert source
  often runs well past that, so a cluster whose peak is driven by snapshotting
  is not sized by these numbers. Read
  [`mz_internal.mz_cluster_replica_metrics_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics_history)
  for that instead.

- **Short-lived objects can be missed entirely.** Recording works by sampling
  each replica in a rotation, so an object that is dropped before its replica's
  turn leaves no trace. Nothing incorrect is recorded, the episode is simply
  absent.

- **`peak_heap` is an upper bound on the episode where disk is swap.** On
  Materialize Cloud and with the Self-Managed defaults, disk is provided as
  swap, and both marks come from the kernel, covering each process's whole
  lifetime up to the moment the episode is recorded. Post-hydration work can
  raise them, and a later episode can inherit an earlier episode's mark. For
  sizing, this errs the safe way. On a replica with a scratch disk, Materialize
  samples `peak_disk_bytes` periodically instead, so it can miss spikes: leave
  more headroom on disk there.

- **Timestamps can carry clock skew.** On a multi-process replica the endpoints
  of an interval come from different process clocks, so a recorded duration
  includes their skew. This is not usually visible at the minute scale that
  matters for sizing.

- **Rows outlive what they name.** `replica_id`, `cluster_id`, and `object_id`
  may all name objects that no longer exist, which is what makes the history
  useful across a resize. Join `mz_cluster_replica_history` for replica names,
  and `mz_clusters` for the cluster's current name: `mz_cluster_replica_history`
  keeps the cluster name from when each replica was created, so it goes stale
  after a rename or swap. Expect the join through `mz_object_global_ids` to drop
  objects that have since been dropped.

- **Replica episodes are kept for 120 days and per-object rows for 30 days by
  default.** Sizing decisions should come from the recent history rather than
  the earliest episode still stored.

## What should I do if hydration history is empty?

Recording is controlled by the `hydration_history_collection_interval` system
parameter, which sets how often Materialize samples replicas for completed
episodes. A value of zero disables recording, and the tables then stay as they
are: rows already collected remain, and no new ones are added.
`replica_hydration_history_retention_period` and
`hydration_history_retention_period` bound how long replica episodes and
per-object rows live, and default to 120 and 30 days.

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

## How do I speed up hydration?

Hydration speed scales with cluster size, so a cluster can borrow capacity for
hydration alone rather than running at the larger size permanently. See
[autoscaling for hydration](/clusters/autoscaling/), which provisions an extra
burst replica at a larger size whenever the cluster has un-hydrated objects and
removes it once a steady-size replica catches up.

To reduce the work hydration has to do in the first place, see [Optimize
hydration requirements](/clusters/optimize-hydration-requirements/).

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Optimize hydration requirements](/clusters/optimize-hydration-requirements/)
- [Clusters](/fundamentals/concepts/clusters/)
- [Operational guidelines](/clusters/operational-guidelines/)
- [Autoscaling for hydration](/clusters/autoscaling/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [Usage & billing](/materialize-cloud/billing/)
