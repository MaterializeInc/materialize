---
title: "Clusters"
description: "Learn about clusters in Materialize."
menu:
  main:
    parent: 'concepts'
    weight: 70
    identifier: 'concepts-clusters'
aliases:
  - /get-started/key-concepts/#clusters
  - /self-managed/v25.2/concepts/clusters/
  - /concepts/clusters/
---

## Overview

Clusters are pools of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

## Resource isolation

Clusters provide **resource isolation.** Each cluster provisions dedicated
compute resources and can fail independently from other clusters. All workloads
on a given cluster compete for access to that cluster's compute resources.

Workloads on different clusters are strictly isolated from one another. That is,
a given workload has access only to the CPU, memory, and scratch disk of the
cluster it runs on.

Resource isolation lets you place workloads on separate clusters to prevent
them from competing for compute resources.

See also [three-tier architecture](#three-tier-architecture-in-production).

## Clusters and workloads

The following operations require a cluster in Materialize:

- Maintaining [sources](/fundamentals/concepts/sources/), [tables (or
  subsources)](/fundamentals/concepts/sources/#tables-and-subsources) created from a
  source, and [sinks](/fundamentals/concepts/sinks/).
- Maintaining [indexes](/fundamentals/concepts/indexes/) and [materialized
  views](/fundamentals/concepts/views/#materialized-views).
- Executing [`SELECT`] and [`SUBSCRIBE`] statements.

Each session has an **active cluster**, which you can change with [`SET
CLUSTER`](/sql/set/#set-active-cluster).

```mzsql
SET CLUSTER = 'my_transform_cluster';
```

[`SELECT`] and [`SUBSCRIBE`] statements run in the session's active cluster.

Objects that require compute (e.g., indexes, materialized views, sources) are
associated with a cluster when they are created. The associated cluster is
either:

- the session's active cluster by default, or

- the cluster specified by the `IN CLUSTER <cluster>` clause in the `CREATE`
  statement.

### Cross-cluster objects

{{% include-from-yaml data="cluster_details" name="cross-cluster-accessibility" %}}

### Cluster-local objects

{{% include-from-yaml data="index_details" name="index-cluster-local" %}}

For more on indexes and clusters, see [Indexes](/fundamentals/concepts/indexes/).

## Cluster replicas

The [replication factor](/sql/create-cluster/#replication-factor) of a cluster
determines the number of replicas provisioned for the cluster.

{{% include-from-yaml data="cluster_details" name="replica-definition" %}}

Materialize automatically assigns names to replicas (e.g., `r1`, `r2`). You can
view information about individual replicas in the Materialize console and the
system catalog.

### Fault tolerance

Provisioning more than one replica for a cluster improves **fault tolerance**.
Clusters with multiple replicas can tolerate failures of the underlying
hardware that cause a replica to become unreachable. As long as one replica of
the cluster remains available, the cluster can continue to maintain dataflows
and serve queries.

{{< note >}}

{{% include-headless "/headless/cluster-replica-cost-capacity-notes" %}}

{{< /note >}}

### Availability guarantees

When provisioning replicas,

{{% include-headless "/headless/multi-replica-az" %}}

See also [Hydration considerations](#hydration-considerations).

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
factor](#cluster-replicas) of `0` has no compute and never leaves this state. To
monitor progress, check that replicas report `online` in
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

<a name="sizing-your-clusters"></a>

## Cluster sizing

When creating a cluster, you must choose its
[size](/sql/create-cluster/#available-sizes) (e.g., `25cc`, `50cc`, `100cc`),
which determines its resource allocation (CPU, memory, and scratch disk space)
and [cost (for Cloud)](/materialize-cloud/billing/#compute). The appropriate size
for a cluster depends on the resource requirements of your workload. Larger
clusters have more compute resources available and can therefore process data
faster and handle larger data volumes.

To gauge the performance and utilization of your clusters, use the
[**Environment Overview** page in the Materialize
Console](/developer-tools/console/monitoring/).

As your workload changes, you can [resize a cluster](/sql/alter-cluster/). A
resize triggers [hydration](#hydration-considerations). During hydration, the
cluster keeps serving since Materialize provisions new replicas at the
target size and hydrates them before retiring the old ones.

### Size a cluster for hydration

The resources required to hydrate a workload depend on its data volume, data
distribution, and dataflows. You cannot reliably predict these requirements
before the workload hydrates for the first time. Start with a cluster size that
has ample capacity, observe a successful hydration, and then resize down in
steps. Starting too small can cause an out-of-memory restart and rehydration
loop.

To establish a cluster size:

1. Create the cluster at a conservatively large size with a replication factor
   of `0`. This lets you deploy the complete workload before starting compute,
   so the first hydration episode represents the workload as a whole. Replace
   `<large-size>` with the size you want to evaluate.

   ```mzsql
   CREATE CLUSTER my_cluster (
       SIZE = '<large-size>',
       REPLICATION FACTOR = 0
   );
   ```

   Deploy every source, index, materialized view, and sink that the cluster will
   maintain. Then start compute by setting the intended replication factor.

   ```mzsql
   ALTER CLUSTER my_cluster SET (REPLICATION FACTOR = 1);
   ```

1. Monitor the cluster through the [provisioning, hydrating, catching up, and
   steady-state phases](#lifecycle-of-a-cluster). Wait until every object has
   hydrated and the cluster has caught up before evaluating the size.

1. Inspect the latest completed hydration episode for each replica in
   [`mz_replica_hydration_history`](/sql/system-catalog/mz_internal/#mz_replica_hydration_history).
   Replace `my_cluster` with your cluster name. Completed episodes are collected
   asynchronously, so retry the query if the cluster only recently hydrated.

   ```mzsql
   WITH ranked_history AS (
       SELECT h.*,
              row_number() OVER (
                  PARTITION BY h.replica_id
                  ORDER BY h.finished_at DESC
              ) AS recency
       FROM mz_internal.mz_replica_hydration_history AS h
   )
   SELECT c.name AS cluster,
          r.name AS replica,
          r.size,
          h.finished_at - h.started_at AS hydration_time,
          h.object_count,
          round(h.peak_memory_bytes::numeric / 1073741824, 2)
              AS peak_memory_gib,
          round(h.peak_disk_bytes::numeric / 1073741824, 2)
              AS peak_disk_gib
   FROM ranked_history AS h
   JOIN mz_catalog.mz_clusters AS c ON c.id = h.cluster_id
   JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
   WHERE c.name = 'my_cluster'
     AND h.recency = 1
   ORDER BY r.name;
   ```

   Confirm that `object_count` is consistent across sizing trials. This count
   includes maintained compute dataflows, including system dataflows, and does
   not correspond directly to the number of user objects. Then use
   `hydration_time` to evaluate the hydration portion of your recovery
   objective. Use the resource peaks as evidence when deciding whether to try a
   smaller size. Leave headroom for data growth and variation between hydration
   episodes.

   {{< note >}}

   Hydration history is best effort and currently records only successful
   hydration. A missing row does not mean that hydration did not occur. The
   resource values are the largest process-lifetime high-water marks observed
   when the episode was collected, so work before collection can contribute to
   them. Disk peaks are sampled lower bounds. A newly provisioned replica that
   is inspected soon after its first hydration provides the clearest signal.

   {{< /note >}}

1. If you need to identify objects that took the longest to hydrate, inspect
   the per-object history in
   [`mz_object_hydration_history`](/sql/system-catalog/mz_internal/#mz_object_hydration_history).

   ```mzsql
   WITH ranked_replica_history AS (
       SELECT h.*,
              row_number() OVER (
                  PARTITION BY h.replica_id
                  ORDER BY h.finished_at DESC
              ) AS recency
       FROM mz_internal.mz_replica_hydration_history AS h
   ),
   latest_episodes AS (
       SELECT h.*
       FROM ranked_replica_history AS h
       JOIN mz_catalog.mz_clusters AS c ON c.id = h.cluster_id
       JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
       WHERE c.name = 'my_cluster'
         AND h.recency = 1
   )
   SELECT coalesce(o.name, h.object_id) AS object,
          o.type,
          coalesce(r.name, h.replica_id) AS replica,
          h.hydrated_at - coalesce(h.started_at, h.installed_at)
              AS hydration_time
   FROM latest_episodes AS e
   JOIN mz_internal.mz_object_hydration_history AS h
       ON h.cluster_id = e.cluster_id
      AND h.replica_id = e.replica_id
      AND h.installed_at >= e.started_at
      AND h.hydrated_at <= e.finished_at
   LEFT JOIN mz_internal.mz_object_global_ids AS g
       ON g.global_id = h.object_id
   LEFT JOIN mz_catalog.mz_objects AS o ON o.id = g.id
   LEFT JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
   ORDER BY hydration_time DESC;
   ```

   Historical object IDs might not resolve to names after an object is dropped.
   In that case, the query displays the dataflow ID from the history table.

1. [Resize the cluster](/sql/alter-cluster/#resizing) down one step. The resize
   creates new replicas at the target size and hydrates them while the existing
   replicas continue serving. Repeat the lifecycle and history checks before
   trying another smaller size. If hydration exceeds your recovery objective or
   a replica runs out of memory, return to the last successful size.

After you determine the steady-state size, you can configure an [autoscaling
strategy](/sql/alter-cluster/#speed-up-hydration-by-autoscaling-to-a-larger-size)
to provision a larger burst replica during future hydration. Use a size that
you have observed successfully hydrate the workload as the hydration size.

## Hydration considerations

{{% include-from-yaml data="hydration-details" name="definition" %}}

{{% include-from-yaml data="hydration-details" name="triggers" %}}

{{% include-from-yaml data="hydration-details" name="per-replica" %}}

{{< tip >}}
Hydration primarily impacts memory usage, and its speed scales with cluster
size. To handle the temporary compute increases during hydration, you can
configure an [autoscaling
strategy](/sql/alter-cluster/#speed-up-hydration-by-autoscaling-to-a-larger-size)
that provisions an extra burst replica at a larger size while the cluster has
un-hydrated objects.
{{< /tip >}}

For more information, including hydration strategies and the memory usage of
hydrating objects, see [Hydration](/fundamentals/concepts/hydration/).

## Best practices

The following provides some general guidelines for clusters. See also
[Operational guidelines](/clusters/operational-guidelines/).

### Three-tier architecture in production

{{% include-from-yaml data="best_practices_details" name="architecture-three-tier" %}}

See also [Operational guidelines](/clusters/operational-guidelines/).

#### Alternatives

Alternatively, if a three-tier architecture is not feasible or unnecessary due
to low volume or a non-production setup, a two cluster or a single cluster
architecture may suffice.

See [Appendix: Alternative cluster
architectures](/clusters/operational-guidelines/appendix-alternative-cluster-architectures/) for details.

### Use production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`ALTER CLUSTER`](/sql/alter-cluster)
- [Hydration](/fundamentals/concepts/hydration/)
- [`mz_object_hydration_history`](/sql/system-catalog/mz_internal/#mz_object_hydration_history)
- [`mz_replica_hydration_history`](/sql/system-catalog/mz_internal/#mz_replica_hydration_history)
- [System clusters](/sql/system-clusters)
- [Usage & billing](/materialize-cloud/billing/)
- [Operational guidelines](/clusters/operational-guidelines/)

[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
