---
title: "Optimize cluster sizes for hydration"
description: "Optimize your cluster size by observing the resources it requires to hydrate."
menu:
  main:
    parent: "clusters"
    weight: 5
    name: "Optimize cluster size"
    identifier: "cluster-sizing"
---

A cluster's [size](/sql/create-cluster/#available-sizes) defines the CPU,
memory, and scratch disk available to every replica. On Materialize Cloud, this
determines the [cost](/materialize-cloud/billing/#compute) of the cluster.
Clusters should be provisioned for peak resource usage, to ensure that they can
handle the load placed on them. For most clusters, peak resource usage happens
during [hydration](/fundamentals/concepts/hydration/).

This guide will walk you through how to estimate resources required for
hydration. Before reading this guide, make sure you understand the [lifecycle of
a cluster](/fundamentals/concepts/clusters/#lifecycle-of-a-cluster).

{{< note >}}
Hydration rebuilds a dataflow's in-memory state from the storage layer, which
takes more memory than maintaining that state afterwards. The size that holds a
hydrated cluster is therefore not always the size that can rebuild it, and a
replica that runs out of memory while hydrating restarts and tries again rather
than running slower.
{{< /note >}}

## Start large, then size down

This guide assumes you are running Materialize v26.42 or later. v26.42 included
improvements to allow you to track peak resource usage during hydration.

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

Compare `peak_memory` against the memory the candidate size provides, which
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
reports per process, and leave headroom for the inputs to grow.

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
If one object accounts for most of the episode, [move it to its own
cluster](/fundamentals/concepts/hydration/#hydration-strategies) so its
hydration peak stops dictating the size of everything else.

### 4. Size down

Once you have found the appropriate size, you can downsize by altering the
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
 replica | size  |          started_at           | hydration_time | object_count | peak_memory | peak_disk
---------+-------+-------------------------------+----------------+--------------+-------------+-----------
 r2      | 100cc | 2026-09-08 10:41:22.913044+00 | 00:12:37.42    |           41 | 12 GB       | 4310 MB
 r1      | 400cc | 2026-09-08 09:12:04.117841+00 | 00:04:11.83    |           41 | 11 GB       | 2438 MB
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
WHERE rh.cluster_name = 'analytics'
ORDER BY sh.occurred_at DESC
LIMIT 10;
```

Repeated `offline` rows with an out-of-memory `reason`, and no new episode in
hydration history, mean the size cannot rebuild the state. Go back to the size
that hydrated, and take a smaller step, or reduce the peak itself with one of
the [hydration
strategies](/fundamentals/concepts/hydration/#hydration-strategies).

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

- **`peak_memory_bytes` is an upper bound on the episode.** It comes from the
  kernel's own high-water mark, covering each process's whole lifetime up to the
  moment the episode is recorded, so post-hydration work can raise it and a
  later episode can inherit an earlier episode's mark. For sizing memory this
  errs the safe way: the recorded value is never below the true hydration peak.

- **`peak_disk_bytes` is a lower bound.** Where a scratch filesystem is in use
  there is no kernel high-water mark to read, so the value is a maximum over
  samples and can miss a spike between two of them. Leave more headroom on disk
  than the number by itself implies.

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

## What should I do if hydration history is empty?

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

## How do I speed up hydration?

Hydration speed scales with cluster size, so a cluster can borrow capacity for
hydration alone rather than running at the larger size permanently. An [`AUTO
SCALING STRATEGY (ON
HYDRATION)`](/sql/alter-cluster/#speed-up-hydration-by-autoscaling-to-a-larger-size)
provisions an extra burst replica at a larger size whenever the cluster has
un-hydrated objects, and removes it once a steady-size replica catches up.

To reduce the work hydration has to do in the first place, see [hydration
strategies](/fundamentals/concepts/hydration/#hydration-strategies).

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Clusters](/fundamentals/concepts/clusters/)
- [Operational guidelines](/clusters/operational-guidelines/)
- [`CREATE CLUSTER`](/sql/create-cluster/)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
- [Usage & billing](/materialize-cloud/billing/)
