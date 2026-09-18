---
title: "Diagnose ad hoc queries that create dataflows"
description: "How to tell whether an ad hoc query built a new dataflow, why that can be slow or memory-intensive, and how to avoid it."
menu:
  main:
    parent: serve-results
    weight: 85
    identifier: ad-hoc-query-dataflows
---

An [ad hoc query](/sql/select/#ad-hoc-queries) either reads straight out of an
existing index (the **fast path**) or spins up a temporary dataflow to compute
its result (the **slow path**). Fast-path queries return from memory with low,
predictable latency. Slow-path queries do real work at query time: they read
and arrange the underlying data, which takes CPU and memory proportional to
the size and complexity of the query, for as long as the query takes to run.
An ad hoc query that regularly takes the slow path over large or unindexed
data can show up as unexpectedly high query latency, or as a spike in cluster
memory, even though its dataflow is torn down once the query completes.

This guide covers how to tell which path a query took, how to spot this
pattern using statement logging, and how to avoid it.

## Tell whether a query takes the fast path

Before running a query, use [`EXPLAIN`](/sql/explain-plan/) and check for the
`Explained Query (fast path):` heading:

```mzsql
EXPLAIN SELECT * FROM t WHERE x = 42;
```

Its presence means Materialize will serve the query from an existing index. Its
absence means Materialize will build a dataflow to compute the result. See
[Fast path queries](/sql/explain-plan/#fast-path-queries) for details, and
[Optimization](/transform-data/optimization/#use-explain-to-verify-index-usage)
for how index design affects which path a query takes.

## Find dataflow-creating queries after the fact

To check which of your *already-executed* queries took the slow path, query
[`mz_recent_activity_log`](/sql/system-catalog/mz_internal/#mz_recent_activity_log):
its `transient_index_id` column holds the ID of the dataflow Materialize built
for that query, and is `NULL` for fast-path queries.

```mzsql
SELECT sql, began_at, finished_at, transient_index_id
FROM mz_internal.mz_recent_activity_log
WHERE transient_index_id IS NOT NULL
ORDER BY began_at DESC;
```

This is also the data behind the **Query History** view in the
[Materialize console](/developer-tools/console/monitoring/). See
[Troubleshooting: How do I troubleshoot slow queries?](/serve-results/troubleshooting/#how-do-i-troubleshoot-slow-queries)
for more on reading query history, and
[Query History](/self-managed-deployments/query-history/) for how statement
logging is configured in self-managed deployments.

Keep two limits in mind when using this data to investigate a past incident:

- **Sampling.** Statements are sampled, not all captured, so a slow-path query
  can be missing from the log. See
  [Query History](/self-managed-deployments/query-history/) for the sample
  rate and how to raise it.
- **Retention.** `mz_recent_activity_log` only covers the last 24 hours.

## Avoid creating a dataflow for a repeated query

If the same query pattern keeps taking the slow path, that's a sign to stop
running it ad hoc:

- **Index the pattern.** [Create an index](/sql/create-index/) on the columns
  the query filters or joins on, so it becomes a fast-path point lookup. See
  [Optimization](/transform-data/optimization/#where-point-lookups) for how to
  pick index keys.
- **Materialize it.** If the query is read often relative to how often its
  inputs change, consider a [materialized view](/sql/create-materialized-view/)
  instead of repeating the ad hoc query.
- **Check for anti-patterns that force a dataflow even with the right
  indexes.** Some predicate shapes (`NOT IN` with a subquery, `= ANY(...)` in a
  join) compile to a cross join that can't use an index, regardless of query
  design. See [Idiomatic Materialize SQL](/transform-data/idiomatic-materialize-sql/)
  for rewrites.
