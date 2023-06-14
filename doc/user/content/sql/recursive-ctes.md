---
title: Recursive CTEs
description: "Recursive CTEs are used to define recursive queries."
menu:
  main:
    parent: 'transform'
    name: Recursive CTEs
    weight: 23
---

{{< beta >}}
Recursive CTEs using the `WITH MUTUALLY RECURSIVE` syntax
{{</ beta >}}

Recursive CTEs operate on the recursively-defined structures like trees or graphs implied from queries over your data.

## Syntax

### with_recursive_cte

{{< diagram "with-recursive-ctes.svg" >}}

### recursive_cte_binding

{{< diagram "recursive-cte-binding.svg" >}}

Field | Use
------|-----
**RETURN AT RECURSION LIMIT $n** | An optional clause indicating that the fixpoint computation should stop after `$n` iterations and use the current values computed for each `recursive_cte_binding` in the `select_stmt`. This could be useful when debugging and validating the correctness of recursive queries, or when you know exactly how many iterations you want to have, regardless of reaching a fixpoint. See the [Examples](#examples) section for an example.
**ERROR AT RECURSION LIMIT $n** |  An optional clause indicating that the fixpoint computation should stop after `$n` iterations and fail the query with an error. Adding this clause with a reasonably high limit is a good safeguard against accidentally running a non-terminating dataflow in your production clusters.
**recursive_cte_binding**  | A binding that gives the SQL fragment defined under `select_stmt` a `cte_ident` alias. This alias can be used in the same binding or in all other (preceding and subsequent) bindings in the enclosing recursive CTE block. Note that in contrast to [the `cte_binding` definition](../select#cte_binding), a `recursive_cte_binding` needs to explicitly state its type as a comma-separated list of (`col_ident` `col_type`) pairs.

## Details

Within a recursive CTEs block, any `cte_ident` alias can be referenced in all `recursive_cte_binding` definitions that live under the same block, as well as in the final `select_stmt` for that block.

Semantically, a recursive CTE block will initialize all of its bindings to the empty collection and sequentially update them in a loop until either:

1. a fixpoint is reached (i.e., if the collections stop changing across loop iterations), or
2. some early exit criteria are satisfied.

Note that Materialize's ability to [efficiently handle incremental changes to your inputs](https://materialize.com/guides/incremental-computation/) extends across loop iterations.
For each iteration, Materialize performs work resulting only from the input changes for this iteration and feeds back the resulting output changes to the next iteration.
When the set of changes for all bindings becomes empty, the recursive computation stops and the final `select_stmt` is evaluated.

{{< warning >}}
In the absence of recursive CTEs, every `SELECT` query is guaranteed to compute its result or fail with an error within a finite amount of time.
However, introducing recursive CTEs complicates the situation as follows:

1. The query might not converge (and may never terminate).
   Non-terminating queries never return a result and can consume a lot of your cluster resources. See [an example](#non-terminating-queries) below.
2. A small update to a few (or even one) data points in your input might cascade in big updates in your recursive computation.
   This most likely will manifest in spikes of the cluster resources allocated to your recursive dataflows.
   See [an example](#queries-with-update-locality) below.
{{</ warning >}}

## Examples

Let's consider a very simple schema consisting of `users` that belong to a hierarchy of geographical `areas` and exchange `transfers` between each other.


### Example schema

```sql
-- A hierarchy of geographical locations with various levels of granularity.
CREATE TABLE areas(id int not null, parent int, name text);
-- A collection of users.
CREATE TABLE users(id char(1) not null, area_id int not null, name text);
-- A collection of transfers between these users.
CREATE TABLE transfers(src_id char(1), tgt_id char(1), amount numeric, ts timestamp);
```

### Example data

```sql
DELETE FROM areas;
DELETE FROM users;
DELETE FROM transfers;

INSERT INTO areas VALUES
  (1, 2    , 'Brooklyn'),
  (2, 3    , 'New York'),
  (3, 7    , 'United States of America'),
  (4, 5    , 'Kreuzberg'),
  (5, 6    , 'Berlin'),
  (6, 7    , 'Germany'),
  (7, null , 'Earth');
INSERT INTO users VALUES
  ('A', 1, 'Alice'),
  ('B', 4, 'Bob'),
  ('C', 2, 'Carol'),
  ('D', 5, 'Dan');
INSERT INTO transfers VALUES
  ('B', 'C', 20.0 , now() + '10 seconds'),
  ('A', 'D', 15.0 , now() + '20 seconds'),
  ('C', 'D', 25.0 , now() + '30 seconds'),
  ('A', 'B', 10.0 , now() + '40 seconds'),
  ('C', 'A', 35.0 , now() + '50 seconds');
```

### Transitive closure

The following view will compute `connected` as the transitive closure of a graph where:
* each `user` is a graph vertex, and
* a graph edge between users `x` and `y` exists only if a transfer from `x` to `y` was made recently (using the rather small `10 seconds` period here for the sake of illustration):

```sql
CREATE MATERIALIZED VIEW connected AS
WITH MUTUALLY RECURSIVE
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers WHERE mz_now() <= ts + interval '10s'
    UNION
    SELECT src_id, dst_id FROM connected
    UNION
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected;
```

You can insert [the example data](#example-data) and observe how the materialized view contents change over time from the `psql` with the `\watch` command:

```sql
SELECT * FROM connected;
\watch 1
```

{{< note >}}
Depending on your base data, the number of records in the `connected` result might get close to the square of the number of `users`.
{{</ note >}}

### Strongly connected components

Another thing that you might be interested in is identifying maximal sub-graphs where every pair of `users` are `connected` (the so-called [_strongly connected components (SCCs)_](https://en.wikipedia.org/wiki/Strongly_connected_component)) of the graph defined above.
This information might be useful to identify clusters of closely-tight users in your application.

Since you already have `connected` defined as a `MATERIALIZED VIEW`, you can piggy-back on that information to derive the SCCs of your graph.
Two `users` will be in the same SCC only if they are `connected` in both directions.
Consequently, given the `connected` contents, we can:

1. Restrict `connected` to the subset of `symmetric` connections that go in both directions.
2. Identify the `scc` of each `users` entry with the lowest `dst_id` of all `symmetric` neighbors and its own `id`.

```sql
CREATE MATERIALIZED VIEW strongly_connected_components AS
  WITH
    symmetric(src_id, dst_id) AS (
      SELECT src_id, dst_id FROM connected
      INTERSECT ALL
      SELECT dst_id, src_id FROM connected
    )
  SELECT u.id, least(min(c.dst_id), u.id)
  FROM users u
  LEFT JOIN symmetric c ON(u.id = c.src_id)
  GROUP BY u.id;
```

Again, you can insert [the example data](#example-data) and observe how the materialized view contents change over time from the `psql` with the `\watch` command:

```sql
SELECT * FROM strongly_connected_components;
\watch 1
```

{{< note >}}
The `strongly_connected_components` definition given above is not recursive, but relies on the recursive CTEs from the `connected` definition.
If you don't need to keep track of the `connected` contents for other reasons, you can use [this alternative SCC definition](https://twitter.com/frankmcsherry/status/1628519795971727366) which computes SCCs directly using repeated forward and backward label propagation.
{{</ note >}}

### Aggregations over a hierarchy

You might want to keep track of the aggregated net balance per area for a recent period of time.
This can be achieved in three steps:

1. Sum up the aggregated net balance for the set period for each user in an `user_balances` CTE.
2. Sum up the `user_balances` of users directly associated with an area in a `direct_balances` CTE.
3. Recursively add to `direct_balances` the `indirect_balances` sum of all child areas.

A materialized view that does the above three steps in three CTEs (of which the last one is recursive) can be defined as follows:

```sql
CREATE MATERIALIZED VIEW area_balances AS
  WITH MUTUALLY RECURSIVE
    user_balances(id char(1), balance numeric) AS (
      WITH
        credits AS (
          SELECT src_id as id, sum(amount) credit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY src_id
        ),
        debits AS (
          SELECT tgt_id as id, sum(amount) debit
          FROM transfers
          WHERE mz_now() <= ts + interval '10s'
          GROUP BY tgt_id
        )
      SELECT
        id,
        coalesce(debit, 0) - coalesce(credit, 0) as balance
      FROM
        users
        LEFT JOIN credits USING(id)
        LEFT JOIN debits USING(id)
    ),
    direct_balances(id int, parent int, balance numeric) AS (
      SELECT
        a.id as id,
        a.parent as parent,
        coalesce(sum(ub.balance), 0) as balance
      FROM
        areas a
        LEFT JOIN users u ON (a.id = u.area_id)
        LEFT JOIN user_balances ub ON (u.id = ub.id)
      GROUP BY
        a.id, a.parent
    ),
    indirect_balances(id int, parent int, balance numeric) AS (
      SELECT
        db.id as id,
        db.parent as parent,
        db.balance + coalesce(sum(ib.balance), 0) as balance
      FROM
        direct_balances db
        LEFT JOIN indirect_balances ib ON (db.id = ib.parent)
      GROUP BY
        db.id, db.parent, db.balance
    )
  SELECT
    id, balance
  FROM
    indirect_balances;
```

As before, you can insert [the example data](#example-data) and observe how the materialized view contents change over time from the `psql` with the `\watch` command:

```sql
SELECT id, name, balance FROM area_balances JOIN areas USING(id) ORDER BY id;
\watch 1
```

### Non-terminating queries

Let's look at a slight variation of the [transitive closure example](#transitive-closure) from above with the following changes:

1. All `UNION` operators are replaced with `UNION ALL`.
2. The `mz_now() < ts + $period` predicate from the first `UNION` branch is omitted.
3. The `WITH MUTUALLY RECURSIVE` clause has an optional `ERROR AT RECURSION LIMIT 100`.
4. The final result in this example is ordered by `src_id, dst_id`.

```sql
WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 100)
  connected(src_id char(1), dst_id char(1)) AS (
    SELECT DISTINCT src_id, tgt_id FROM transfers
    UNION ALL
    SELECT src_id, dst_id FROM connected
    UNION ALL
    SELECT c1.src_id, c2.dst_id FROM connected c1 JOIN connected c2 ON c1.dst_id = c2.src_id
  )
SELECT src_id, dst_id FROM connected ORDER BY src_id, dst_id;
```

After inserting [the example data](#example-data) you can observe that executing the above statement returns the following error:

```text
ERROR:  Evaluation error: Recursive query exceeded the recursion limit 100. (Use RETURN AT RECURSION LIMIT to not error, but return the current state as the final result when reaching the limit.)
```

The recursive CTE `connected` has not converged to a fixpoint within the first 100 iterations!
To see why, you can run variants of the same query where the

```sql
ERROR AT RECURSION LIMIT 100
```

clause is replaced by

```sql
RETURN AT RECURSION LIMIT $n -- where $n = 1, 2, 3, ...
```

and observe how the result changes after `$n` iterations.

{{< tabs >}}
{{< tab "After 1 iteration">}}
```text
 src_id | dst_id
--------+--------
 A      | B
 ...
```
{{< /tab >}}
{{< tab "After 2 iterations">}}
```text
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 ...
```
{{< /tab >}}
{{< tab "After 3 iterations">}}
```text
 src_id | dst_id
--------+--------
 A      | B
 A      | B
 A      | B
 ...
```
{{< /tab >}}
{{< /tabs >}}

Changing the `UNION` to `UNION ALL` in the `connected` definition caused a full copy of `transfer` to be added to the current value of `connected` in each iteration!
Consequently, `connected` never stops growing and the recursive CTE computation never reaches a fixpoint.

### Queries with "update locality"

The examples presented so far have the following "update locality" property:

{{< note >}}
A change in a source collection will usually cause a _bounded amount_ of changes to the contents of the recursive CTE bindings derived after each iteration.
{{</ note >}}

For example:

- Most of the time, inserting or removing `transfers` will not change the contents of the `connected` collection. This is true because:
  - An alternative path from `x` to `y` already existed before the insertion, or
  - An alternative path from `x` to `y` will exist after the removal.
- Inserting or removing `transfers` will not change most of the contents of the `area_balances` collection. This is true because:
  - Areas are organized in a hierarchy with a maximum height `h`.
  - A single transfer contributes directly only to the `area_balances` of the areas where the `src_id` and `tgt_id` belong.
  - A single transfer contributes indirectly only to the `area_balances` of the areas ancestor areas of the above two areas.
  - Consequently, each `transfer` change will affect at most `2` areas in each iteration and at most `2*h` areas in total.

However, note that not all iterative algorithms have this property. For example:

- In a naive PageRank implementation, the introduction of a link between two pages `x` and `y` will change the PageRank of these two pages and every page `z` transitively connected to either `x` or `y`.
  Since most graphs exhibit [small-world properties](https://en.wikipedia.org/wiki/Small-world_network), this might represent most of the existing pages.
- In a [naive k-means clustering algorithm](https://en.wikipedia.org/wiki/K-means_clustering), inserting or removing a new data point will affect the positions of the `k` mean points after each iteration.

Depending on the size and update frequency of your input collections, expressing algorithms that violate the "update locality" property (such as the examples above) using recursive CTEs in Materialize might lead to excessive CPU and memory consumption in the clusters that compute these recursive queries.

## Related pages

- [Regular CTEs](/sql/select/#regular-ctes)
- [`SELECT`](/sql/select)
