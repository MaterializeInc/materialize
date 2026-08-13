---
title: Hydration
description: "Learn about hydration in Materialize: reconstructing an object's in-memory state by reading from the storage layer."
menu:
  main:
    parent: concepts
    weight: 31
    identifier: 'concepts-hydration'
---

{{% include-from-yaml data="hydration-details" name="definition" %}}

## When hydration occurs

{{% include-from-yaml data="hydration-details" name="triggers" %}}

For when hydration occurs for each object type, see [Objects and
hydration](#objects-and-hydration).

## Objects and hydration

Hydration is per cluster replica. A [cluster](/concepts/clusters/) is a
collection of replicas, and the replicas, not the cluster, have the lifecycle:
a replica starts, hydrates, serves, and can crash and restart. A replica's
properties are also immutable, which is why a resize provisions new replicas
instead of changing existing ones. When a trigger above occurs, the objects on
the affected replicas hydrate as described below. When a replica restarts,
every object on it re-hydrates. A resize or an added replica hydrates only the
new replicas, where every object hydrates just as it would after a restart.

{{% yaml-table data="hydration-objects-table" %}}

## Hydration strategies

Hydration primarily impacts memory usage, and its speed scales with cluster
size. Some hydration-related strategies you may want to consider:

- Add an [`AUTO SCALING STRATEGY (ON HYDRATION)`](/sql/alter-cluster/) to your
  cluster. With this strategy, Materialize automatically provisions an extra,
  larger replica (a burst replica) while the cluster has un-hydrated objects,
  then removes it once a steady-size replica catches up. You pay for the burst
  replica while it is provisioned, but not at steady state.

  - If a steady-size replica runs out of memory during hydration, resize the
    cluster. During the resizing, the cluster continues to serve from the burst replica.

- Distribute materialized views and indexes across multiple clusters. Each
  cluster's replicas hydrate their objects independently, which distributes the
  memory required for hydration, lets objects on different clusters hydrate in
  parallel, and limits how much must re-hydrate when any one replica restarts.

- When changing a materialized view or index, or forcing dependents to re-plan
  (for example, after dropping an index and recreating the dependents), build
  the new version to the side to avoid downtime:

  - A [blue/green deployment](/manage/dbt/blue-green-deployments/) hydrates the
    new version alongside the old and cuts over when hydrated, with no serving
    gap. Note that blue/green requires sources and sinks to live on dedicated
    clusters that are excluded from the swap. For more information, see
    [blue/green deployment](/manage/dbt/blue-green-deployments/).

  - For a single materialized view, creating and hydrating a [replacement
    materialized view (public preview) and replacing the existing view in
    place](/transform-data/updating-materialized-views/replace-materialized-view/)
    may be simpler, but briefly reduces freshness. The replacement materialized
    view can be either on the same or different cluster.

{{< note >}}

The burst-replica and blue/green strategies run extra replicas alongside the
existing ones, as do a resize or a zero-downtime upgrade. During the overlap,
the cluster temporarily uses additional resources. Account for the additional
cost and, on self-managed deployments, the additional capacity required.

{{< /note >}}

In addition, consider the following strategies. These strategies trade off peak
hydration memory against added operational complexity, extra objects, and
potentially longer total hydration time. You can use them when peak hydration
memory is the bottleneck rather than as a default modeling pattern.

- <a name="index-order"></a>If multiple objects in the **same** cluster consume
  the same view, add an [index](/concepts/indexes/) to that view **before**
  creating the consumers. Consumers in that cluster can reuse the indexed
  arrangement instead of each building equivalent in-memory state, which can
  reduce both memory usage during hydration and steady-state memory. Note:
  - Index reuse is limited to the cluster the index is on, and the index must
    exist **before** its consumers are created for the optimizer to reuse it.
  - For a view with only one consumer, an index generally adds memory instead of
    saving it.

- For a very large materialized view, consider splitting it into several smaller
  materialized views, for example by a partition key such as customer, region,
  or date range. Smaller materialized views can hydrate as separate dataflows,
  which can bound peak memory compared with hydrating one very large
  materialized view.
  - This helps most when a cluster runs only a few large materialized views,
    where a single view's hydration spike can dictate the cluster size. A
    cluster with many materialized views already hydrates them as separate
    dataflows and gets this benefit naturally.
  - A re-plan or replacement of one split view affects only that portion of
    the data. A replica restart still re-hydrates all views on the replica,
    though in smaller units.
  - If the split views share expensive computation, put that computation in a
    [common indexed view first](#index-order), creating the index **before**
    creating the split views. Otherwise, each split view may rebuild its own
    copy of the shared work, increasing total memory.
  - Queries must target or combine the split views.

## Related pages

- [Snapshotting](/concepts/snapshotting/)
- [Clusters](/concepts/clusters/)
- [Sources](/concepts/sources/)
- [Troubleshooting](/transform-data/troubleshooting/#hydrating-objects)
- [Updating materialized views](/transform-data/updating-materialized-views/)
