---
title: "Updating materialized views"
description: "Strategies for updating materialized views in production."
disable_list: true
menu:
  main:
    parent: transform-data
    weight: 45
    identifier: updating-materialized-views
---

As your application and workload evolves, you might need to update materialized view definitions. Materialize offers multiple strategies to update your materialized views, each with different tradeoffs for complexity, resource usage, and impact on freshness.

## Choosing an update strategy

| Strategy | When to use | Tradeoffs |
|----------|-------------|-----------|
| [**Blue/green deployments**](/manage/dbt/blue-green-deployments/) | Complex changes across multiple objects, or when using dbt for deployment orchestration. | Ensures no impact to data freshness during cutover, but temporarily doubles resource usage and requires team coordination. |
| [**Replace materialized view**](replace-materialized-view/) | Simple changes to a single materialized view's query definition. | Simpler to deploy with no additional tooling, but may impact freshness on the materialized view and all downstream objects. |

## Blue/green deployments

Blue/green deployments allow you to deploy changes to a separate environment
("green") that mirrors your production environment ("blue"). After the green environment has hydrated, you atomically swap the
environments.

This strategy is ideal when:

- You're making changes across multiple materialized views, indexes, or clusters
- You're using dbt to manage your Materialize objects
- You need to ensure zero impact to data freshness during the cutover
- You have the resources to temporarily run two environments in parallel

For detailed instructions, see the [Blue/green deployment guide](/manage/dbt/blue-green-deployments/).

## Replace materialized view

The `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` command allows you to update
a single materialized view's definition while preserving its name, downstream
dependencies, and indexes. Materialize calculates the *diff* between the original
and replacement views, then propagates the changes to all dependent objects.

This strategy is ideal when:

- You're modifying a single materialized view
- You want a simple, SQL-native approach without additional tooling
- You can tolerate a brief reduction in freshness on the materialized view, and all downstream objects

For detailed instructions, see the [Replace materialized view guide](replace-materialized-view/).
