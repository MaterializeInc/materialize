---
title: "Usage & billing"
description: "Understand the billing model of Materialize, and learn best practices for cost control."
menu:
  main:
    parent: "administration"
    weight: 10
---

From the [Materialize console](https://console.materialize.com/) (`Admin` > `Usage &
Billing`), administrators can access their invoice. The invoice provides
Compute and Storage usage and cost information.

## Cost factors

Various factors can affect your costs, including (but not limited to):

- Each [replica for a cluster](/sql/create-cluster/#credit-usage). To see the
  number of replicas for your clusters, click on `Clusters` in the console,

- Creating a [materialized view](/concepts/views/#materialized-views) as
  materialized view creation executes the underlying query and stores the
  results to durable storage, and materialized views incrementally updates
  results as inputs change. To see the views in a cluster, in the console's
  `Clusters` view, select the cluster to inspect, and click on the `Materialized
  Views` tab.

  {{< note >}}

  For more information on how incremental computations can lower the cost of
  freshness, see [How Materialized can lower the cost of freshness for data
  teams](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation).

  {{< /note >}}

- Creating [indexes on a view](/concepts/indexes/) as index creation executes
  the underlying query, and indexes maintain and, as inputs change,
  incrementally update view results in memory.

  {{< note >}}

  For more information on how incremental computations can lower the cost of
  freshness, see [How Materialized can lower the cost of freshness for data
  team](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation).

  {{< /note >}}

  To see the indexes in a cluster, in the console's `Clusters` view, select the
  cluster to inspect, and click on the `Indexes` tab.

- Maintaining [sources](/concepts/sources/). To see the sources for a cluster,
  in the console's `Clusters` view, select the cluster to inspect, and click on
  the `Sources` tab.

- Maintaining [sinks](/concepts/sinks/). To see the sinks for a cluster, in the
  console's `Clusters` view, select the cluster to inspect, and click on the
  `Sinks` tab.

- Executing [`SELECT`](/sql/select/) and [`SUBSCRIBE`](/sql/subscribe/)
  statements.  To see your query history, in the console, under `Monitoring`,
  select `Query History`.

## Cost reductions

Some considerations for lowering costs:

- Create a view and an index rather than a materialized view.  See
  [Views](/concepts/views/).

- Avoid creating unnecessary indexes. That is, create indexes appropriate
  for your use cases. See [Optimization](/transform-data/optimization/).
