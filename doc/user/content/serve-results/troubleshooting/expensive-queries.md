---
title: "Troubleshooting: Expensive queries"
description: "How to find queries that use a lot of CPU or memory on a Materialize cluster, and how to reduce their cost."
menu:
  main:
    parent: "transform-troubleshooting"
    name: "Expensive queries"
    weight: 30
---

This guide helps you find queries that use a lot of CPU or memory on a
cluster, and how to reduce their cost.

A query that can't be answered from an existing index makes the cluster build a
temporary dataflow, compute the result, and then drop the dataflow. The cluster
does this work on every execution, alongside the work of maintaining its
indexes and materialized views. Expensive queries slow down other queries on
the same cluster, and can make the cluster run out of memory.

## Common causes

- **No usable index**: Frequent queries that join, aggregate, or filter on
  columns without an index build a dataflow on every execution.
- **Ad-hoc queries on a production cluster**: Exploratory queries, such as
  large joins or full scans, compete for CPU and memory with the indexes and
  materialized views that serve your application.
- **Large results**: Queries without filters or limits read and return much
  more data than needed.

## Diagnosing the issue

### Find the most expensive queries

Queries with the `standard` execution strategy built a temporary dataflow. To
find the ones that consumed the most time over the last day, grouped by query
text, query the [statement log](/serve-results/troubleshooting/#query-history):

```mzsql
SELECT
  left(sql, 60) AS sql,
  cluster_name,
  count(*) AS executions,
  round(sum(extract(epoch FROM finished_at - began_at)), 3) AS total_seconds,
  max(result_size) AS max_result_bytes
FROM mz_internal.mz_recent_activity_log
WHERE execution_strategy = 'standard'
  AND cluster_name NOT LIKE 'mz_%'
  AND began_at > now() - INTERVAL '1 day'
GROUP BY sql_hash, sql, cluster_name
ORDER BY total_seconds DESC
LIMIT 10;
```

```nofmt
                             sql                              | cluster_name | executions | total_seconds | max_result_bytes
--------------------------------------------------------------+--------------+------------+---------------+------------------
 SELECT o.customer_id, t.total FROM orders o JOIN order_total | quickstart   |          1 |         0.081 |              234
 SELECT count(*) FROM orders                                  | quickstart   |          1 |         0.026 |               20
 SELECT * FROM order_counts WHERE customer_id = 5             | quickstart   |          1 |         0.015 |               21
```

Look for two patterns:

- A query with many `executions` runs often enough that its total cost adds
  up, even if each execution is fast. Make it a [fast path
  query](#make-frequent-queries-fast-path).
- A query with a high `total_seconds` for few executions is an expensive
  ad-hoc query. [Isolate it](#isolate-ad-hoc-queries) or [reduce the data it
  reads](#return-less-data).

Canceled and failed statements have no `execution_strategy`, so this query
doesn't include them. To find long-running statements that were canceled or
failed, filter on `finished_status IN ('canceled', 'error')` instead.

### Find expensive queries that are running now

Dataflows for queries that are running are named `oneshot-select-<id>`. To
see how much CPU time each one has used, run the following on the cluster
that runs the queries:

```mzsql
SET cluster = quickstart;

SELECT
  mdo.name,
  mse.elapsed_ns / 1000 * '1 MICROSECONDS'::interval AS elapsed_time
FROM mz_introspection.mz_scheduling_elapsed AS mse,
  mz_introspection.mz_dataflow_operators AS mdo,
  mz_introspection.mz_dataflow_addresses AS mda
WHERE mse.id = mdo.id
  AND mdo.id = mda.id
  AND list_length(mda.address) = 1
  AND mdo.name LIKE 'Dataflow: oneshot-select-%'
ORDER BY mse.elapsed_ns DESC;
```

```nofmt
             name              |  elapsed_time
-------------------------------+-----------------
 Dataflow: oneshot-select-t176 | 00:00:35.980703
 Dataflow: oneshot-select-t190 | 00:00:14.433187
```

To find the query text and cancel a running query, see [Find running
queries](/serve-results/troubleshooting/unresponsive-queries/#find-running-queries).

### Compare with the cost of indexes and materialized views

To see how the CPU and memory of your indexes and materialized views compare,
run [`EXPLAIN ANALYZE CLUSTER`](/sql/explain-analyze/#explain-analyze-cluster-)
on the cluster:

```mzsql
EXPLAIN ANALYZE CLUSTER CPU, MEMORY;
```

If the indexes and materialized views account for most of the cluster's CPU and
memory, the queries are not the main cost. To see which operators of an index
or materialized view use the most resources, see [`EXPLAIN
ANALYZE`](/sql/explain-analyze/) and [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/).

### Check the query plan

Run [`EXPLAIN`](/sql/explain-plan/) on an expensive query to see what the
dataflow does:

```mzsql
EXPLAIN SELECT o.customer_id, t.total
FROM orders o
JOIN order_totals t USING (customer_id)
WHERE o.id < 10;
```

Look for operators that read full collections, such as joins without a
matching index, or `Read` operators on large sources or materialized views.

## Resolution

### Make frequent queries fast path

A query that reads from an index and applies only filters and projections
doesn't build a dataflow. `EXPLAIN` shows `Explained Query (fast path)` for
these queries.

- Create an [index](/fundamentals/concepts/indexes/) on the columns that the
  query filters on.
- Move joins and aggregations into a view, and index the view on the lookup
  key. For example:

  ```mzsql
  CREATE VIEW order_totals AS
    SELECT customer_id, sum(amount) AS total
    FROM orders
    GROUP BY customer_id;

  CREATE INDEX order_totals_idx ON order_totals (customer_id);

  -- Fast path lookup
  SELECT * FROM order_totals WHERE customer_id = 5;
  ```

- Run the query on the same cluster as the index. Indexes are local to a
  cluster.

For more techniques, see [Optimization](/transform-data/optimization/).

### Isolate ad-hoc queries

Run exploratory queries on a separate cluster, so they can't slow down or
crash the cluster that serves your application:

```mzsql
CREATE CLUSTER adhoc (SIZE = '25cc');
SET cluster = adhoc;
```

For guidance on how to split work across clusters, see [Operational
guidelines](/clusters/operational-guidelines/).

### Return less data

- Add filters, and use [temporal
  filters](/transform-data/patterns/temporal-filters/) on timestamp columns.
  Materialize can skip over old data in storage that doesn't match the filter.
- Add a `LIMIT` clause to exploratory queries.
- Select only the columns you need.

The `max_query_result_size` [configuration
parameter](/sql/set/#other-configuration-parameters) makes a query fail if its
result exceeds the limit. It doesn't limit the memory that the temporary
dataflow uses to compute the result.

### Size up the cluster

If the queries are already optimized, [size up the
cluster](/sql/alter-cluster/) to give it more CPU and memory.
