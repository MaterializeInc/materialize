---
title: "Optimize hydration requirements"
description: "Strategies to reduce the memory Materialize needs to hydrate objects, and to speed up or avoid hydration altogether."
menu:
  main:
    parent: "clusters"
    weight: 6
    identifier: "optimize-hydration-requirements"
---

[Hydration](/fundamentals/concepts/hydration/) primarily impacts memory usage,
and its speed scales with cluster size. This guide covers strategies to reduce
the memory a cluster needs during hydration, speed hydration up, or avoid
triggering it in the first place.

## Isolate hydration-heavy objects

- Use a dedicated cluster for [sources](/fundamentals/concepts/sources/).

- In addition, use a dedicated cluster for upsert sources; i.e., do not
  co-locate with append-only Kafka sources or CDC database sources.

  - Keeping append-only Kafka sources and CDC database sources (PostgreSQL,
    MySQL, and SQL Server sources) on a separate cluster isolates ingestion
    from possible OOM loops caused by memory-heavy objects such as Kafka
    upsert sources.

  - Note: PostgreSQL, MySQL, and SQL Server sources run on a single replica,
    the oldest, and remain there until that replica is removed. As such, the
    use of a burst replica (through [`AUTO SCALING STRATEGY (ON
    HYDRATION)`](#provision-extra-capacity-while-hydrating)) has no impact on
    these single-replica sources.

- Distribute materialized views and indexes across multiple clusters. Each
  cluster's replicas hydrate their objects independently, which distributes
  the memory required for hydration, lets objects on different clusters
  hydrate in parallel, and limits how much must re-hydrate when any one
  replica restarts.

## Limit concurrent hydration

By default, a replica hydrates up to 4 dataflows at a time. Lowering this
limit spreads out the memory spikes from hydrating many objects at once, at
the cost of a longer total hydration time; raising it can shorten total
hydration time at the cost of a higher peak. [Contact our team](/support/) if
you'd like to tune this for your cluster.

## Reuse arrangements across consumers

If multiple objects in the **same** cluster consume the same view, add an
[index](/fundamentals/concepts/indexes/) to that view **before** creating the
consumers. Consumers in that cluster can reuse the indexed arrangement instead
of each building equivalent in-memory state, which can reduce both memory
usage during hydration and steady-state memory.

- Index reuse is limited to the cluster the index is on, and the index must
  exist **before** its consumers are created for the optimizer to reuse it.
- For a view with only one consumer, an index generally adds memory instead of
  saving it.

## Split large materialized views

For a very large materialized view, consider splitting it into several
smaller materialized views, for example by a partition key such as customer,
region, or date range. Smaller materialized views can hydrate as separate
dataflows, which can bound peak memory compared with hydrating one very large
materialized view.

- This helps most when a cluster runs only a few large materialized views,
  where a single view's hydration spike can dictate the cluster size. A
  cluster with many materialized views already hydrates them as separate
  dataflows and gets this benefit naturally.
- A re-plan or replacement of one split view affects only that portion of the
  data. A replica restart still re-hydrates all views on the replica, though
  in smaller units.
- If the split views share expensive computation, put that computation in a
  [common indexed view first](#reuse-arrangements-across-consumers), creating
  the index **before** creating the split views. Otherwise, each split view
  may rebuild its own copy of the shared work, increasing total memory.
- Queries must target or combine the split views.

## Reduce arrangement memory

- For multi-way joins (more than two inputs), a [delta
  join](/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins)
  keeps no intermediate results in memory, unlike a differential join. Index
  all the join keys so the optimizer can choose a delta join.

- If your workload has columns with a small set of often-repeated values (for
  example, status strings or enum-like labels), [dictionary
  compression](/transform-data/dictionary-compression/) can reduce the memory
  those arrangements use, at the cost of CPU and slower hydration.

- Avoid setting [`RETAIN HISTORY`](/serve-results/durable-subscriptions/) on
  indexes: an index's arrangement holds its full retained history in memory,
  not just the storage layer. Configure history retention on a materialized
  view instead.

## Provision extra capacity while hydrating

Add an [`AUTO SCALING STRATEGY (ON
HYDRATION)`](/sql/alter-cluster/#speed-up-hydration-by-autoscaling-to-a-larger-size)
to your cluster with memory-heavy objects. With this strategy, Materialize
automatically provisions an extra, larger replica (a burst replica) while the
cluster has unhydrated objects, then removes it once a steady-size replica
catches up. You pay for the burst replica while it is provisioned, but not at
steady state.

If a steady-size replica runs out of memory during hydration, resize the
cluster. During the resize, the cluster continues to serve from the burst
replica.

## Avoid unnecessary full-cluster hydration

When changing a materialized view or index, or forcing dependents to re-plan
(for example, after dropping an index and recreating the dependents), build
the new version to the side to avoid downtime:

- A [blue/green deployment](/developer-tools/dbt/blue-green-deployments/)
  hydrates the new version alongside the old and cuts over when hydrated, with
  no serving gap. Note that blue/green requires sources and sinks to live on
  dedicated clusters that are excluded from the swap. For more information,
  see [blue/green deployment](/developer-tools/dbt/blue-green-deployments/).

- For a single materialized view, creating and hydrating a [replacement
  materialized view (public preview) and replacing the existing view in
  place](/transform-data/updating-materialized-views/replace-materialized-view/)
  may be simpler, but briefly reduces freshness. The replacement materialized
  view can be either on the same or different cluster.

{{< note >}}
A [slim deployment](/developer-tools/dbt/slim-deployments/) that redeploys
only changed objects onto a copy of a cluster only exercises those objects'
hydration spike, not the full cluster's combined spike. A cluster that
passes a slim deployment can still fail to hydrate on the next full restart
or upgrade. Use a [blue/green
deployment](/developer-tools/dbt/blue-green-deployments/) in production to
validate hydration for the whole cluster.
{{< /note >}}

{{< note >}}

The burst-replica and blue/green strategies run extra replicas alongside the
existing ones, as do a resize or a zero-downtime upgrade. During the overlap,
the cluster temporarily uses additional resources. Account for the additional
cost and, on self-managed deployments, the additional capacity required.

{{< /note >}}

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Operational guidelines](/clusters/operational-guidelines/)
- [Query optimization](/transform-data/optimization/)
- [Dictionary compression](/transform-data/dictionary-compression/)
- [Durable subscriptions](/serve-results/durable-subscriptions/)
- [Snapshotting](/fundamentals/concepts/snapshotting/)
- [Clusters](/fundamentals/concepts/clusters/)
- [Troubleshooting](/serve-results/troubleshooting/#hydrating-objects)
