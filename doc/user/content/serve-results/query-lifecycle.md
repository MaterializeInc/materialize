---
title: "Understand the query lifecycle"
description: "Understand how Materialize executes a `SELECT` query, and what latency to expect for each type of query."
menu:
  main:
    parent: serve-results
    weight: 7
---

When you issue a `SELECT`, Materialize answers it in one of two ways,
depending on what the query reads and which indexes exist on the active
[cluster](/fundamentals/concepts/clusters/). Knowing which one applies tells you
what latency to expect and what to change when a query is slow.

| Query type | Reads from |
|------------|------------|
| [Indexed reads](#indexed-reads) | An existing index, in memory |
| [Ad-hoc computation](#ad-hoc-computation) | A temporary dataflow built for the query |

Queries that reduce to a constant, such as `SELECT 1`, are answered without
reading any data or using a cluster.

The examples on this page use the following tables:

```mzsql
CREATE TABLE orders (id int, customer_id int, amount numeric);
CREATE TABLE customers (id int, name text);
```

## Stages of a query

Every `SELECT` goes through the same stages:

1. **Plan and optimize.** Materialize parses the query, resolves the objects it
   references, and picks how to run it.
1. **Select a timestamp.** Materialize chooses the timestamp at which to read,
   based on your [isolation level](/serve-results/isolation-level/). If an input
   has not yet caught up to that timestamp, for example because it is
   [hydrating](/fundamentals/concepts/hydration/) or lagging, the query cannot
   return until it does. Use [`EXPLAIN TIMESTAMP`](/sql/explain-timestamp/) to
   compare the chosen timestamp with each input's frontier. See [freshness
   troubleshooting](/transform-data/freshness-troubleshooting/).
1. **Execute.** Materialize sends the query to the cluster. The cluster returns
   the result once every input is readable at the chosen timestamp. If the
   cluster needs to wait for the chosen timestamp, the query keeps holding
   cluster resources. For example, joining a large input with a lagging one can
   use a lot of memory until the lagging input catches up.
1. **Return results.** Materialize sends the rows to your client.

For indexed reads, computing the result is quick, so the total latency usually
comes from how far the chosen timestamp is ahead of your inputs, plus the
network round trip to your client.

## Indexed reads

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

-- Indexed read: point lookup on the index key
SELECT * FROM order_totals WHERE customer_id = 42;
```

**Expected performance:** milliseconds. Cost grows with the number of rows the
query scans, so a filter that is not on the index key scans the full index.

## Ad-hoc computation

When a query can't be answered by an indexed read, Materialize builds a
temporary dataflow on the active cluster, reads its inputs, computes the
result, and drops the dataflow once the result is returned. Common examples:

- Joining or aggregating objects.
- Filtering a materialized view or table that has no index in the cluster. The
  whole object is read from storage, minus any data skipped by [filter
  pushdown](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

```mzsql
CREATE INDEX customers_idx ON customers (id);

-- Ad-hoc computation: the join needs a new dataflow, but reuses the indexes
-- on order_totals and customers
SELECT c.name, t.total
FROM order_totals t JOIN customers c ON c.id = t.customer_id;

CREATE MATERIALIZED VIEW order_totals_mv AS
  SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id;

-- Ad-hoc computation: order_totals_mv has no index, so it is read from storage
SELECT * FROM order_totals_mv WHERE customer_id = 42;
```

**Expected performance:** depends on what the dataflow must read. When the
dataflow can reuse indexes on its inputs in the cluster, as in the join above,
the query can return in milliseconds. When an input has no usable index, the
dataflow reads it from storage and builds the state it needs, so latency grows
with the amount of data read. Check the `Used Indexes` section of
[`EXPLAIN`](/sql/explain-plan/) to see which indexes a query reuses. Ad-hoc
queries also use CPU and memory on the cluster, which can slow down other work
on the same cluster.

To turn a frequent ad-hoc query into an indexed read, create a view for the
query and [index it](/fundamentals/concepts/indexes/#indexes-on-views) in the
cluster that serves the query.

## Identify the query type

Use [`EXPLAIN`](/sql/explain-plan/) to see which type a query uses before you
run it:

| Query type | `EXPLAIN` output |
|------------|------------------|
| Indexed read | Starts with `Explained Query (fast path):`, and reads with `→Index Lookup` (point lookup) or `→Indexed` (full index scan) |
| Ad-hoc computation | Starts with `Explained Query:` |

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

A `SELECT` with only a `LIMIT`, and no filter or `ORDER BY`, on an object with
no index in the cluster may read a few rows directly from storage instead of
building a dataflow. `EXPLAIN` shows this as `→ReadStorage`. This is useful
for peeking at data, but because `LIMIT` without `ORDER BY` returns arbitrary
rows, don't rely on it in application queries.

To find ad-hoc queries that already ran, filter the [statement
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
