---
title: "Query policies"
description: "Protect a cluster from running out of resources by rejecting queries that build temporary dataflows or read from object storage."
menu:
  main:
    parent: "clusters"
    weight: 9
    identifier: "query-policies"
---

{{< private-preview />}}

Ad hoc queries can build temporary dataflows or read from object storage on a
cluster that serves an application. These queries compete for memory and CPU
with the queries your application depends on, and can cause the cluster to run
out of memory. A query policy lets you warn about or reject these queries on a
cluster, while the indexes and materialized views on the cluster keep running.

A query policy is a named, region-wide object with one or more rules. You
attach a policy to a cluster, and it applies to every role that runs queries on
that cluster. A cluster can have at most one policy, and many clusters can share
a policy.

## Background: the query lifecycle

Materialize answers a `SELECT` in one of two ways. An indexed read returns
results directly from an existing index on the cluster, which is cheap. Any
other query builds a temporary dataflow on the cluster, which can read inputs
from object storage and hold intermediate state in memory until the query
finishes. Query policies inspect the plan before execution and decide which of
these plans the cluster accepts. See [Understand the query
lifecycle](/serve-results/query-lifecycle/).

## Choosing a rule

Each rule rejects queries whose execution plan includes one feature.

| Rule value | Rejects |
|------------|---------|
| `temporary_dataflow` | Queries that build a [temporary dataflow](/sql/explain-plan/#fast-path-queries), even if the dataflow reads only from indexes. For example, aggregations, joins, and `SUBSCRIBE`. |
| `storage_read` | Queries that read from object storage. For example, reads from a table, source, or materialized view that is not indexed on the cluster. |

To pick rules for a common goal, see [Recommended rules](#recommended-rules).

To check which rule a query would match, run [`EXPLAIN`](/sql/explain-plan/).
An `Explained Query (fast path)` plan builds no temporary dataflow. A plan with
a `ReadStorage` operator or a `Source` entry reads from object storage.

## What a policy checks

A policy checks `SELECT`, `SUBSCRIBE`, and `COPY TO` statements, and the reads
in `INSERT INTO ... SELECT`, `UPDATE`, and `DELETE` statements.

A policy does not check:

- Queries that return a constant result.
- DDL, such as `CREATE INDEX` or `CREATE MATERIALIZED VIEW`, or the dataflows
  that maintain indexes and materialized views.
- `EXPLAIN` statements, including `EXPLAIN ANALYZE`.

Query policies are not resource limits. A fast path read from a large index can
still be expensive. A policy also does not cancel queries that are already
running when you attach or change it.

## Tutorial: Set up a query policy

This tutorial protects a cluster named `serving` so that it only serves fast
path reads from indexes. It uses `warn` mode to find affected queries before
enforcing the policy. The examples use this table:

```mzsql
CREATE TABLE orders (order_id int, customer_id int, amount numeric);
```

### Before you begin

- Query policies are in private preview. [Contact our
  team](https://materialize.com/support/) to enable them.
- To create a policy, you need the `CREATEQUERYPOLICY` system privilege.
- To attach a policy to a cluster, you need `CREATE` privileges on the cluster
  and `USAGE` privileges on the policy. The policy owner has `USAGE` by
  default.

### Step 1. Create a policy in warn mode

Create a policy with both rules. Policies use `warn` mode by default.

```mzsql
CREATE QUERY POLICY indexed_reads_only (
  RULES (
    no_temporary_dataflows (action = 'reject', metric = 'query_plan_includes', value = 'temporary_dataflow'),
    no_storage_reads (action = 'reject', metric = 'query_plan_includes', value = 'storage_read')
  )
);
```

### Step 2. Attach the policy to the cluster

```mzsql
ALTER CLUSTER serving SET (QUERY POLICY = indexed_reads_only);
```

You can also attach a policy when you create a cluster:

```mzsql
CREATE CLUSTER serving (SIZE = 'M.1-small', QUERY POLICY = indexed_reads_only);
```

### Step 3. Find the queries the policy would reject

In `warn` mode, matching queries still run, and Materialize returns a `NOTICE`
for each rule they match. This aggregation over an unindexed table matches both
rules:

```mzsql
SET cluster = serving;
SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id;
```

```nofmt
NOTICE:  query would be rejected under query policy "indexed_reads_only" (rule "no_temporary_dataflows"): this query would build a temporary dataflow
DETAIL:  Query policy "indexed_reads_only" rule "no_temporary_dataflows" applies to this query on cluster "serving". Policy mode is 'warn', so this rule does not reject the query.
HINT:  Run EXPLAIN to inspect the query plan. Create an index that serves the query on this cluster, or use a cluster whose query policy permits this plan.
NOTICE:  query would be rejected under query policy "indexed_reads_only" (rule "no_storage_reads"): this query would read from object storage
DETAIL:  Query policy "indexed_reads_only" rule "no_storage_reads" applies to this query on cluster "serving". Policy mode is 'warn', so this rule does not reject the query.
HINT:  Run EXPLAIN to inspect the query plan. Create an index that serves the query on this cluster, or use a cluster whose query policy permits this plan.
```

For each query your application needs, create a view and an index on the
cluster that serve it. DDL is not checked, so you can create the index on the
protected cluster:

```mzsql
CREATE VIEW orders_by_customer AS
  SELECT customer_id, sum(amount) AS total FROM orders GROUP BY customer_id;
CREATE INDEX orders_by_customer_idx IN CLUSTER serving
  ON orders_by_customer (customer_id);
```

The application can now read the result with a fast path query, which matches
neither rule:

```mzsql
SELECT * FROM orders_by_customer WHERE customer_id = 10;
```

Run any query the policy should not cover on a different cluster.

### Step 4. Enforce the policy

```mzsql
ALTER QUERY POLICY indexed_reads_only SET (MODE = 'enforce');
```

Matching queries now fail with SQLSTATE `53000` (`insufficient_resources`):

```mzsql
SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id;
```

```nofmt
ERROR:  query rejected: this query would build a temporary dataflow
DETAIL:  Query policy "indexed_reads_only" rule "no_temporary_dataflows" applies to this query on cluster "serving".
HINT:  Run EXPLAIN to inspect the query plan. Create an index that serves the query on this cluster, or use a cluster whose query policy permits this plan.
```

### Step 5. Detach the policy

To remove the policy from the cluster:

```mzsql
ALTER CLUSTER serving RESET (QUERY POLICY);
```

## Recommended rules

### Allow only indexed reads, without joins

Include both rules. Queries must be [fast path](/sql/explain-plan/#fast-path-queries)
reads from an index on the cluster, such as point lookups and filters on a
single indexed object. Joins and aggregations need their own view and index.
This is the strictest option, and the one the [tutorial](#tutorial-set-up-a-query-policy)
uses.

### Allow indexed reads, with joins

Include only `storage_read`. Queries can join, aggregate, and filter indexed
objects in a temporary dataflow, but cannot read unindexed tables, sources, or
materialized views. Use this when ad hoc queries over indexed data are
acceptable, but loading unindexed data into memory is not.

```mzsql
CREATE QUERY POLICY indexed_inputs_only (
  RULES (
    no_storage_reads (action = 'reject', metric = 'query_plan_includes', value = 'storage_read')
  )
);
```

### Allow direct reads, without computation

Include only `temporary_dataflow`. Queries cannot build a temporary dataflow,
but can still read directly from an unindexed materialized view, for example
`SELECT * FROM my_mv LIMIT 10`, because that read does not build a dataflow.
Use this to block joins and aggregations while still allowing quick lookups.

```mzsql
CREATE QUERY POLICY no_ad_hoc_dataflows (
  RULES (
    no_temporary_dataflows (action = 'reject', metric = 'query_plan_includes', value = 'temporary_dataflow')
  )
);
```

## Manage query policies

- To list policies, use [`SHOW QUERY POLICIES`](/sql/show-query-policies/).
- To see policy rules and attachments, query
  [`mz_internal.mz_query_policy_rules`](/sql/system-catalog/mz_internal/#mz_query_policy_rules)
  and
  [`mz_internal.mz_query_policy_attachments`](/sql/system-catalog/mz_internal/#mz_query_policy_attachments).
- To let another role attach a policy, grant it `USAGE` on the policy with
  [`GRANT PRIVILEGE`](/sql/grant-privilege/). Revoking `USAGE` does not detach
  the policy from clusters it is already attached to.
- A policy that is attached to a cluster can only be dropped with
  [`DROP QUERY POLICY ... CASCADE`](/sql/drop-query-policy/), which detaches it
  from all clusters.

## Related pages

- [`CREATE QUERY POLICY`](/sql/create-query-policy/)
- [`ALTER QUERY POLICY`](/sql/alter-query-policy/)
- [`DROP QUERY POLICY`](/sql/drop-query-policy/)
- [`SHOW QUERY POLICIES`](/sql/show-query-policies/)
- [`ALTER CLUSTER`](/sql/alter-cluster/)
