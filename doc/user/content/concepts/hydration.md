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

Hydration is per replica. When a trigger above occurs, the objects on the
affected replicas hydrate as described below. A restart re-hydrates a cluster's
existing replicas. A resize or an added replica hydrates only the new replicas
it provisions. On those new replicas, every object hydrates just as it would
after a restart.


{{% yaml-table data="hydration-objects-table" %}}

## Hydration strategies

Hydration primarily impacts memory usage, and its speed scales with cluster
size. Some hydration-related strategies you may want to consider:

- Add an [`AUTO SCALING STRATEGY (ON HYDRATION)`](/sql/alter-cluster/) to your
  cluster. With this strategy, Materialize automatically provisions an extra,
  larger replica (a burst replica) while the cluster has un-hydrated objects,
  then removes it once a steady-size replica catches up. You pay for the burst
  replica while it is provisioned, but not at steady state.

- Split materialized views and indexes across multiple clusters. Each cluster
  hydrates its own objects independently, which distributes the memory required
  for hydration, lets objects on different clusters hydrate in parallel, and
  limits how much must re-hydrate when a single cluster restarts.

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
    may be simpler, but briefly reduces freshness.

{{< note >}}

The burst-replica and blue/green strategies run extra replicas alongside the
existing ones, as do a resize or a zero-downtime upgrade. During the overlap,
the cluster temporarily uses additional resources, up to roughly double during a
resize or upgrade. Account for the additional cost and, on self-managed
deployments, the additional capacity required.

{{< /note >}}

## Related pages

- [Snapshotting](/concepts/snapshotting/)
- [Clusters](/concepts/clusters/)
- [Sources](/concepts/sources/)
- [Troubleshooting](/transform-data/troubleshooting/#hydrating-objects)
- [Updating materialized views](/transform-data/updating-materialized-views/)
