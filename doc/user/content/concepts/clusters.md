---
title: "Clusters"
description: "Learn about clusters in Materialize."
menu:
  main:
    parent: 'concepts'
    weight: 5
    identifier: 'concepts-clusters'
aliases:
  - /get-started/key-concepts/#clusters
  - /self-managed/v25.2/concepts/clusters/
---

## Overview

Clusters are pools of compute resources (CPU, memory, and scratch disk space)
for running your workloads.

## Clusters and workloads

The following operations require a cluster in Materialize:

- Maintaining [sources](/concepts/sources/), [tables (or
  subsources)](/concepts/sources/#tables-and-subsources) created from a
  source, and [sinks](/concepts/sinks/).
- Maintaining [indexes](/concepts/indexes/) and [materialized
  views](/concepts/views/#materialized-views).
- Executing [`SELECT`] and [`SUBSCRIBE`] statements.

Each session has an **active cluster**, which you can change with [`SET
CLUSTER`](/sql/set/#set-active-cluster).

```mzsql
SET CLUSTER = 'my_transform_cluster';
```

[`SELECT`] and [`SUBSCRIBE`] statements run in the session's active cluster.

Objects that require compute (e.g., indexes, materialized views, sources) are
associated with a cluster when they are created, either:

- the session's active cluster by default, or

- the cluster specified by the `IN CLUSTER <cluster>` clause in the `CREATE`
  statement.

### Cross-cluster objects

{{% include-from-yaml data="cluster_details" name="cross-cluster-accessibility" %}}

### Cluster-local objects

{{% include-from-yaml data="index_details" name="index-cluster-local" %}}

For more on indexes and clusters, see [Indexes](/concepts/indexes/).

## Resource isolation

Clusters provide **resource isolation.** Each cluster provisions dedicated
compute resources and can fail independently from other clusters. All workloads
on a given cluster compete for access to that cluster's compute resources.

Workloads on different clusters are strictly isolated from one another. That is,
a given workload has access only to the CPU, memory, and scratch disk of the
cluster it runs on.

Resource isolation lets you place workloads on separate clusters to prevent
them from competing for compute resources: for example, sources in one
cluster, materialized views in a second, and indexes that serve queries in a
third, as in the recommended [three-tier
architecture](#three-tier-architecture-in-production).

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

- **Provisioning.** Replicas are scheduled and brought online. A cluster with a
  [replication factor](#cluster-replicas) of `0` has no compute and never leaves
  this state. To monitor progress, check that replicas report `online` in
  [`mz_cluster_replica_statuses`](/reference/system-catalog/mz_internal/#mz_cluster_replica_statuses),
  and confirm the cluster has replicas via
  [`mz_clusters`](/reference/system-catalog/mz_catalog/#mz_clusters).

- **Hydrating.** Each replica reconstructs its in-memory state by reading from
  Materialize's storage layer (see [hydration](/concepts/hydration/)). While an
  object is hydrating, its lag is reported as `NULL`. To monitor progress, check
  the `hydrated` flag per object in
  [`mz_hydration_statuses`](/reference/system-catalog/mz_internal/#mz_hydration_statuses).
  For indexes and materialized views,
  [`mz_compute_hydration_statuses`](/reference/system-catalog/mz_internal/#mz_compute_hydration_statuses)
  also reports how long hydration took.

- **Catching up.** Once hydrated, the cluster processes the backlog of input
  updates that accumulated while it was unavailable, so its total lag starts high
  and comes down. To monitor progress, watch `lag` decrease in
  [`mz_wallclock_global_lag_recent_history`](/reference/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history),
  or break the lag down by input with
  [`mz_materialization_lag`](/reference/system-catalog/mz_internal/#mz_materialization_lag).

- **Steady state.** The cluster has caught up and its lag holds low and roughly
  constant, typically a few seconds. To confirm it stays healthy, keep watching
  the same
  [`mz_wallclock_global_lag_recent_history`](/reference/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history)
  lag. A lag that climbs steadily, at about one minute per minute, means the
  cluster has stopped making progress.

{{< note >}}
Sources go through an additional
[snapshotting](/concepts/snapshotting/) step the first time they run, reading the
initial state of the upstream system before the states above apply. See
[Troubleshooting](/transform-data/freshness-troubleshooting/) for how to
diagnose a cluster that is not progressing through these states.
{{< /note >}}

<a name="sizing-your-clusters"></a>

## Cluster sizing

When creating a cluster, you must choose its
[size](/sql/create-cluster/#available-sizes) (e.g., `25cc`, `50cc`, `100cc`),
which determines its resource allocation (CPU, memory, and scratch disk space)
and [cost (for Cloud)](/administration/billing/#compute). The appropriate size
for a cluster depends on the resource requirements of your workload. Larger
clusters have more compute resources available and can therefore process data
faster and handle larger data volumes.

To gauge the performance and utilization of your clusters, use the
[**Environment Overview** page in the Materialize
Console](/console/monitoring/).

As your workload changes, you can [resize a cluster](/sql/alter-cluster/). A
resize triggers [hydration](#hydration-considerations). During hydration, the
cluster keeps serving since Materialize provisions new replicas at the
target size and hydrates them before retiring the old ones.

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
hydrating objects, see [Hydration](/concepts/hydration/).

## Best practices

The following provides some general guidelines for clusters. See also
[Operational guidelines](/manage/operational-guidelines/).

### Three-tier architecture in production

{{% include-from-yaml data="best_practices_details" name="architecture-three-tier" %}}

See also [Operational guidelines](/manage/operational-guidelines/).

#### Alternatives

Alternatively, if a three-tier architecture is not feasible or unnecessary due
to low volume or a non-production setup, a two cluster or a single cluster
architecture may suffice.

See [Appendix: Alternative cluster
architectures](/manage/appendix-alternative-cluster-architectures/) for details.

### Use production clusters for production workloads only

Use production cluster(s) for production workloads only. That is, avoid using
production cluster(s) to run development workloads or non-production tasks.

## Related pages

- [`CREATE CLUSTER`](/sql/create-cluster)
- [`ALTER CLUSTER`](/sql/alter-cluster)
- [Hydration](/concepts/hydration/)
- [System clusters](/sql/system-clusters)
- [Usage & billing](/administration/billing/)
- [Operational guidelines](/manage/operational-guidelines/)

[`SELECT`]: /sql/select/
[`SUBSCRIBE`]: /sql/subscribe/
