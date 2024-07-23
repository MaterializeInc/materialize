---
title: "Usage & Billing"
description: "Provides an overview of various factors that can affect cost."
menu:
  main:
    parent: "administration"
    weight: 10
---

From the [console](https://console.materialize.com/) (`Admin` > `Usage &
Billing`), administrators can access their invoice. The invoice provides
Compute and Storage usage and cost information.

## Cost Factors

Various factors can affect your costs, including (but not limited to):

- Each [replica for a cluster](/sql/create-cluster/#credit-usage). To see the
  number of replicas for your clusters, click on `Clusters` in the console,

- Creating a [materialized view](/concepts/views/#materialized-views) as
  materialized view creation executes the underlying query and stores the
  results to durable storage, and materialized views incrementally updates
  results as inputs change.

  {{< note >}}

  For more information on how incremental computations can lower the cost of
  freshness, see [How Materialized can lower the cost of freshness for data
  team](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation).

  {{< /note >}}

- Creating [indexes on a view](/concepts/indexes/) as index creation executes
  the underlying query, and indexes maintain and, as inputs change,
  incrementally update view results in memory.

  {{< note >}}

  For more information on how incremental computations can lower the cost of
  freshness, see [How Materialized can lower the cost of freshness for data
  team](https://materialize.com/promotions/cost-of-freshness/?utm_campaign=General&utm_source=documentation).

  {{< /note >}}

- Maintaining [sources](/concepts/sources/) and [sinks](/concepts/sinks/).

- Executing [`SELECT`] and [`SUBSCRIBE`] statements.

## Cost Reductions

Some considerations for lowering costs:

- Create a view and an index rather than a materialized view.  See
  [Views](/concepts/views/).

- Avoid creating random indexes. That is, create indexes appropriate
  for your use cases. See [Optimization](/transform-data/optimization/).
