---
title: Transform Data
description: Learn how to efficiently transform data using Materialize SQL
doc_type: overview
product_area: SQL
audience: developer
status: stable
complexity: intermediate
keywords:
  - transform data
  - SQL
  - views
  - materialized views
  - patterns
  - optimization
canonical_url: https://materialize.com/docs/transform-data/
---

# Transform Data

## Purpose
This section covers how to efficiently transform data using Materialize SQL. Learn about views, materialized views, and SQL patterns optimized for streaming data.

If you're building data transformations in Materialize, start here.

## When to use
- Building views and materialized views
- Understanding Materialize SQL patterns
- Optimizing query performance
- Troubleshooting dataflow issues

## Core Concepts

In Materialize, you transform data by creating views and materialized views:

- **Views**: Named queries executed on read. Use for infrequently accessed data.
- **Materialized Views**: Incrementally maintained query results. Use for frequently accessed data or complex transformations.
- **Indexes**: Enable fast point lookups on views.

See [Views concept](../concepts/views/index.md) and [Indexes concept](../concepts/indexes/index.md) for details.

## SQL Reference

For SQL syntax, see:
- [SELECT](../sql/select/index.md) — Query data
- [CREATE VIEW](../sql/create-view/index.md) — Create views
- [CREATE MATERIALIZED VIEW](../sql/create-materialized-view/index.md) — Create materialized views
- [CREATE INDEX](../sql/create-index/index.md) — Create indexes
- [Functions](../sql/functions/index.md) — Built-in functions
- [Data Types](../sql/types/index.md) — Supported types

## Idiomatic Materialize SQL

Learn SQL patterns optimized for streaming:

- [Idiomatic Materialize SQL Overview](idiomatic-materialize-sql/index.md)
- [Using mz_now()](idiomatic-materialize-sql/mz_now/index.md) — Filter by processing time
- [Top-K patterns](idiomatic-materialize-sql/top-k/index.md) — Efficient ranking queries
- [LEAD function](idiomatic-materialize-sql/lead/index.md)
- [LAG function](idiomatic-materialize-sql/lag/index.md)
- [FIRST_VALUE function](idiomatic-materialize-sql/first-value/index.md)
- [LAST_VALUE function](idiomatic-materialize-sql/last-value/index.md)
- [ANY / SOME patterns](idiomatic-materialize-sql/any/index.md)

### Appendices
- [Idiomatic SQL Chart](idiomatic-materialize-sql/appendix/idiomatic-sql-chart/index.md)
- [Window Function Mappings](idiomatic-materialize-sql/appendix/window-function-to-materialize/index.md)

## Common Patterns

- [Patterns Overview](patterns/index.md)
- [Temporal Filters](patterns/temporal-filters/index.md) — Time-based filtering
- [Percentiles](patterns/percentiles/index.md) — Percentile calculations
- [Partition By](patterns/partition-by/index.md) — Grouped aggregations
- [Top-K](idiomatic-materialize-sql/top-k/index.md) — Ranking patterns
- [Durable Subscriptions](patterns/durable-subscriptions/index.md)
- [Rules Engine](patterns/rules-engine/index.md)

## Optimization

- [Optimization Guide](optimization/index.md) — Query performance tuning
- [FAQ: Indexes](faq/index.md) — Common index questions

## Troubleshooting

- [Dataflow Troubleshooting](dataflow-troubleshooting/index.md) — Debug dataflow issues
- [Troubleshooting](troubleshooting/index.md) — Common problems and solutions

## Key Takeaways

- Use materialized views for frequently queried results
- Use regular views for infrequently accessed data
- Create indexes on views to enable fast point lookups
- Use `mz_now()` for time-based filtering in streaming contexts
- Avoid patterns that prevent incremental maintenance (e.g., non-monotonic operations)
