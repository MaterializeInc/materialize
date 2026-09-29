---
title: "Understand the query lifecycle"
description: "Understand how Materialize executes a `SELECT` query, and what latency to expect for each type of query."
menu:
  main:
    parent: serve-results
    weight: 7
---

When you issue a `SELECT`, Materialize answers it in one of three ways,
depending on what the query reads and which indexes exist on the active
[cluster](/fundamentals/concepts/clusters/). Knowing which one applies tells you
what latency to expect and what to change when a query is slow.

| Query type | Reads from | Typical latency |
|------------|------------|-----------------|
| [Fast path: index](#fast-path-index) | An existing index, in memory | Milliseconds |
| [Fast path: storage](#fast-path-storage) | A few rows from object storage | Tens to hundreds of milliseconds |
| [Slow path](#slow-path) | A temporary dataflow built for the query | Seconds or longer, grows with the data read |

The examples on this page use the following tables:

```mzsql
CREATE TABLE orders (id int, customer_id int, amount numeric);
CREATE TABLE customers (id int, name text);
```

## Stages of a query

Every `SELECT` goes through the same stages:

1. **Plan and optimize.** Materialize parses the query, resolves the objects it
   references, and picks one of the three query types.
1. **Select a timestamp.** Materialize chooses the timestamp at which to read,
   based on your [isolation level](/serve-results/isolation-level/).
1. **Wait for inputs.** Materialize waits until every input can be read at that
   timestamp. Inputs that are still [hydrating](/fundamentals/concepts/hydration/)
   or lagging delay the query. See [freshness
   troubleshooting](/transform-data/freshness-troubleshooting/).
1. **Execute.** The cluster computes the result. This is the stage where the
   three query types differ.
1. **Return results.** Materialize sends the rows to your client.

For fast path queries, the execute stage is short, so the total latency is
usually dominated by timestamp selection, waiting for inputs, and the network
round trip to your client.

## Fast path: index

The query reads directly from an [index](/fundamentals/concepts/indexes/) that
already holds the results in memory. No new dataflow is built.

A query takes this path when it reads a **single** indexed object in the active
cluster and only filters, projects, or applies scalar functions to it, optionally
followed by `ORDER BY`, `LIMIT`, or `OFFSET`. Equality filters on the index key
become [point lookups](/fundamentals/concepts/indexes/#point-lookups) and only
touch the matching rows.

```mzsql
CREATE VIEW order_totals AS
  SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id;
CREATE INDEX order_totals_idx ON order_totals (customer_id);

-- Fast path: point lookup on the index key
SELECT * FROM order_totals WHERE customer_id = 42;
```

**Expected performance:** milliseconds. Cost grows with the number of rows the
query scans, so a filter that is not on the index key scans the full index.

## Fast path: storage

The query reads a small number of rows directly from object storage, without
building a dataflow. This applies to a source, table, or materialized view that
has **no index** in the active cluster, when the query:

- Only projects columns or applies scalar functions, with no filter or
  `ORDER BY`.
- Has a `LIMIT` where `LIMIT` plus `OFFSET` is less than 25.

```mzsql
CREATE MATERIALIZED VIEW order_totals_mv AS
  SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id;

-- Fast path: reads 10 rows from storage
SELECT * FROM order_totals_mv LIMIT 10;
```

**Expected performance:** tens to hundreds of milliseconds, bound by object
storage reads. Use this for quick data exploration, not for serving.

## Slow path

When neither fast path applies, Materialize builds a temporary dataflow on the
active cluster, reads its inputs, computes the result, and drops the dataflow
once the result is returned. Common examples:

- Joining or aggregating across objects, even when the inputs are indexed.
- Filtering a materialized view or table that has no index in the cluster. The
  whole object is read from storage, minus any data skipped by [filter
  pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

```mzsql
-- Slow path: the join needs a new dataflow
SELECT c.name, t.total
FROM order_totals t JOIN customers c ON c.id = t.customer_id;

-- Slow path: order_totals_mv has no index, so it is read from storage
SELECT * FROM order_totals_mv WHERE customer_id = 42;
```

**Expected performance:** seconds or longer. The dataflow must read and process
all of its inputs from scratch, so latency grows with the amount of data read.
Slow path queries also use CPU and memory on the cluster, which can slow down
other work on the same cluster.

To move a frequent slow path query to the fast path, create a view for the
query and [index it](/fundamentals/concepts/indexes/#indexes-on-views) in the
cluster that serves the query.

## Identify the query type

Use [`EXPLAIN`](/sql/explain-plan/) to see which type a query uses before you
run it:

| Query type | `EXPLAIN` output |
|------------|------------------|
| Fast path: index | Starts with `Explained Query (fast path):` and contains `Index Lookup` or `Indexed` |
| Fast path: storage | Starts with `Explained Query (fast path):` and contains `ReadStorage` |
| Slow path | Starts with `Explained Query:` |

```mzsql
EXPLAIN SELECT * FROM order_totals WHERE customer_id = 42;
```

```nofmt
Explained Query (fast path):
  →Map/Filter/Project
    Project: #0, #1
    →Index Lookup on materialize.public.order_totals (using materialize.public.order_totals_idx)
      Lookup values: (42)

Used Indexes:
  - materialize.public.order_totals_idx (lookup)

Target cluster: quickstart
```

To find slow path queries that already ran, filter the [statement
log](/sql/system-catalog/mz_internal/#mz_recent_activity_log) on
`execution_strategy = 'standard'`:

```mzsql
SELECT sql, execution_strategy, finished_at - began_at AS duration
FROM mz_internal.mz_recent_activity_log
WHERE execution_strategy = 'standard'
ORDER BY began_at DESC
LIMIT 10;
```

## Related pages

- [`SELECT` and `SUBSCRIBE`](/serve-results/query-results/)
- [Indexes](/fundamentals/concepts/indexes/)
- [`EXPLAIN PLAN`](/sql/explain-plan/)
- [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp/)
- [Troubleshooting slow queries](/serve-results/troubleshooting/)
