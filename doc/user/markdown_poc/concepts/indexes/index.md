<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Concepts](/docs/concepts/)

</div>

# Indexes

## Overview

In Materialize, indexes represent query results stored in memory
**within a [cluster](/docs/concepts/clusters/)**. You can create indexes
on [sources](/docs/concepts/sources/),
[views](/docs/concepts/views/#views), or [materialized
views](/docs/concepts/views/#materialized-views).

## Indexes on sources

<div class="note">

**NOTE:** In practice, you may find that you rarely need to index a
source without performing some transformation using a view, etc.

</div>

In Materialize, you can create indexes on a
[source](/docs/concepts/sources/) to maintain in-memory up-to-date
source data within the cluster you create the index. This can help
improve [query performance](#indexes-and-query-optimizations) when
serving results directly from the source or when [using
joins](/docs/transform-data/optimization/#join). However, in practice,
you may find that you rarely need to index a source directly.

<div class="highlight">

``` chroma
CREATE INDEX idx_on_my_source ON my_source (...);
```

</div>

## Indexes on views

In Materialize, you can create indexes on a
[view](/docs/concepts/views/#views "query
saved under a name") to maintain **up-to-date view results in memory**
within the [cluster](/docs/concepts/clusters/) you create the index.

<div class="highlight">

``` chroma
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

</div>

During the index creation on a
[view](/docs/concepts/views/#views "query saved under
a name"), the view is executed and the view results are stored in memory
within the cluster. **As new data arrives**, the index **incrementally
updates** the view results in memory.

Within the cluster, querying an indexed view is:

- **fast** because the results are served from memory, and

- **computationally free** because no computation is performed on read.

For best practices on using indexes, and understanding when to use
indexed views vs. materialized views, see [Usage
patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized view results are stored in durable storage
and **incrementally updated** as new data arrives. Indexing a
materialized view makes the already up-to-date view results available
**in memory** within the [cluster](/docs/concepts/clusters/) you create
the index. That is, indexes on materialized views require no additional
computation to keep results up-to-date.

<div class="note">

**NOTE:** A materialized view can be queried from any cluster whereas
its indexed results are available only within the cluster you create the
index. Querying a materialized view, whether indexed or not, from any
cluster is computationally free. However, querying an indexed
materialized view within the cluster where the index is created is
faster since the results are served from memory rather than from
storage.

</div>

For best practices on using indexes, and understanding when to use
indexed views vs. materialized views, see [Usage
patterns](#usage-patterns).

<div class="highlight">

``` chroma
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```

</div>

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot
use the indexes in another cluster.

For example, to create an index in the current cluster:

<div class="highlight">

``` chroma
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

</div>

You can also explicitly specify the cluster:

<div class="highlight">

``` chroma
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```

</div>

## Usage patterns

### Index usage

<div class="important">

**! Important:** Indexes are local to a cluster. Queries in one cluster
cannot use the indexes in another, different cluster.

</div>

Unlike some other databases, Materialize can use an index to serve query
results even if the query does not specify a `WHERE` condition on the
index key. Serving queries from an index is fast since the results are
already up-to-date and in memory.

For example, consider the following index:

<div class="highlight">

``` chroma
CREATE INDEX idx_orders_view_qty ON orders_view (quantity);
```

</div>

Materialize will maintain the `orders_view` in memory in
`idx_orders_view_qty`, and it will be able to use the index to serve a
various queries on the `orders_view` (and not just queries that specify
conditions on `orders_view.quantity`).

Materialize can use the index for the following queries (issued from the
same cluster as the index) on `orders_view`:

<div class="highlight">

``` chroma
SELECT * FROM orders_view;  -- scans the index
SELECT * FROM orders_view WHERE status = 'shipped';  -- scans the index
SELECT * FROM orders_view WHERE quantity = 10;  -- point lookup on the index
```

</div>

For the queries that do not specify a condition on the indexed field,
Materialize scans the index. For the query that specifies an equality
condition on the indexed field, Materialize performs a **point lookup**
on the index (i.e., reads just the matching records from the index).
Point lookups are the most efficient use of an index.

#### Point lookups

Materialize performs **point lookup** (i.e., reads just the matching
records from the index) on the index if the query’s `WHERE` clause:

- Specifies equality (`=` or `IN`) condition and **only** equality
  conditions on **all** the indexed fields. The equality conditions must
  specify the **exact** index key expression (including type) for point
  lookups. For example:

  - If the index is on `round(quantity)`, the query must specify
    equality condition on `round(quantity)` (and not just `quanity`) for
    Materialize to perform a point lookup.

  - If the index is on `quantity * price`, the query must specify
    equality condition on `quantity * price` (and not
    `price * quantity`) for Materialize to perform a point lookup.

  - If the index is on the `quantity` field which is an integer, the
    query must specify an equality condition on `quantity` with a value
    that is an integer.

- Only uses `AND` (conjunction) to combine conditions for **different**
  fields.

Point lookups are the most efficient use of an index.

For queries whose `WHERE` clause meets the point lookup criteria and
includes conditions on additional fields (also using `AND` conjunction),
Materialize performs a point lookup on the index keys and then filters
the results using the additional conditions on the non-indexed fields.

For queries that do not meet the point lookup criteria, Materialize
performs a full index scan (including for range queries). That is,
Materialize performs a full index scan if the `WHERE` clause:

- Does not specify **all** the indexed fields.
- Does not specify only equality conditions on the index fields or
  specifies an equality condition that specifies a different value type
  than the index key type.
- Uses OR (disjunction) to combine conditions for **different** fields.

Full index scans are less efficient than point lookups. The performance
of full index scans will degrade with data volume; i.e., as you get more
data, full scans will get slower.

#### Examples

Consider again the following index on a view:

<div class="highlight">

``` chroma
CREATE INDEX idx_orders_view_qty on orders_view (quantity);
```

</div>

The following table shows various queries and whether Materialize
performs a point lookup or an index scan.

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Query</th>
<th>Index usage</th>
</tr>
</thead>
<tbody>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view;</code></pre>
</div></td>
<td>Index scan.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10;</code></pre>
</div></td>
<td>Point lookup.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity IN (10, 20);</code></pre>
</div></td>
<td>Point lookup.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10 OR quantity = 20;</code></pre>
</div></td>
<td>Point lookup. Query uses <code>OR</code> to combine conditions on
the <strong>same</strong> field.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10 AND price = 5.00;</code></pre>
</div></td>
<td>Point lookup on <code>quantity</code>, then filter on
<code>price</code>.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE (quantity, price) = (10, 5.00);</code></pre>
</div></td>
<td>Point lookup on <code>quantity</code>, then filter on
<code>price</code>.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10 OR price = 5.00;</code></pre>
</div></td>
<td>Index scan. Query uses <code>OR</code> to combine conditions on
<strong>different</strong> fields.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity &lt;= 10;</code></pre>
</div></td>
<td>Index scan.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE round(quantity) = 20;</code></pre>
</div></td>
<td>Index scan.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>-- Assume quantity is an integer
SELECT * FROM orders_view WHERE quantity = &#39;hello&#39;;
SELECT * FROM orders_view WHERE quantity::TEXT = &#39;hello&#39;;</code></pre>
</div></td>
<td>Index scan, assuming <code>quantity</code> field in
<code>orders_view</code> is an integer. In the first query, the quantity
is implicitly cast to text. In the second query, the quantity is
explicitly cast to text.</td>
</tr>
</tbody>
</table>

Consider that the view has an index on the `quantity` and `price` fields
instead of an index on the `quantity` field:

<div class="highlight">

``` chroma
DROP INDEX idx_orders_view_qty;
CREATE INDEX idx_orders_view_qty_price on orders_view (quantity, price);
```

</div>

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Query</th>
<th>Index usage</th>
</tr>
</thead>
<tbody>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view;</code></pre>
</div></td>
<td>Index scan.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10;</code></pre>
</div></td>
<td>Index scan. Query does not include equality conditions on
<strong>all</strong> indexed fields.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10 AND price = 2.50;</code></pre>
</div></td>
<td>Point lookup.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view WHERE quantity = 10 OR price = 2.50;</code></pre>
</div></td>
<td>Index scan. Query uses <code>OR</code> to combine conditions on
<strong>different</strong> fields.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view
WHERE quantity = 10 AND (price = 2.50 OR price = 3.00);</code></pre>
</div></td>
<td>Point lookup. Query uses <code>OR</code> to combine conditions on
<strong>same</strong> field and <code>AND</code> to combine conditions
on <strong>different</strong> fields.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view
WHERE quantity = 10 AND price = 2.50 AND item = &#39;cupcake&#39;;</code></pre>
</div></td>
<td>Point lookup on the index keys <code>quantity</code> and
<code>price</code>, then filter on <code>item</code>.</td>
</tr>
<tr>
<td><div class="highlight">
<pre class="chroma" tabindex="0"><code>SELECT * FROM orders_view
WHERE quantity = 10 AND price = 2.50 OR item = &#39;cupcake&#39;;</code></pre>
</div></td>
<td>Index scan. Query uses <code>OR</code> to combine conditions on
<strong>different</strong> fields.</td>
</tr>
</tbody>
</table>

#### Limitations

Indexes in Materialize do not order their keys using the data type’s
natural ordering and instead orders by its internal representation of
the key (the tuple of key length and value).

As such, indexes in Materialize currently do not provide optimizations
for:

- Range queries; that is queries using `>`, `>=`, `<`, `<=`, `BETWEEN`
  clauses (e.g., `WHERE quantity > 10`, `price >= 10 AND price <= 50`,
  and `WHERE quantity BETWEEN 10 AND 20`).

- `GROUP BY`, `ORDER BY` and `LIMIT` clauses.

### Indexes on views vs. materialized views

In Materialize, both [indexes](/docs/concepts/indexes) on views and
[materialized views](/docs/concepts/views/#materialized-views)
incrementally update the view results when Materialize ingests new data.
Whereas materialized views persist the view results in durable storage
and can be accessed across clusters, indexes on views compute and store
view results in memory within a **single** cluster.

Some general guidelines for usage patterns include:

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Usage Pattern</th>
<th>General Guideline</th>
</tr>
</thead>
<tbody>
<tr>
<td>View results are accessed from a single cluster only;<br />
such as in a 1-cluster or a 2-cluster architecture.</td>
<td>View with an <a href="/docs/sql/create-index">index</a></td>
</tr>
<tr>
<td>View used as a building block for stacked views; i.e., views not
used to serve results.</td>
<td>View</td>
</tr>
<tr>
<td>View results are accessed across <a
href="/docs/concepts/clusters">clusters</a>;<br />
such as in a 3-cluster architecture.</td>
<td>Materialized view (in the transform cluster)<br />
Index on the materialized view (in the serving cluster)</td>
</tr>
<tr>
<td>Use with a <a href="/docs/serve-results/sink/">sink</a> or a <a
href="/docs/sql/subscribe"><code>SUBSCRIBE</code></a> operation</td>
<td>Materialized view</td>
</tr>
<tr>
<td>Use with <a
href="/docs/transform-data/patterns/temporal-filters/">temporal
filters</a></td>
<td>Materialized view</td>
</tr>
</tbody>
</table>

For example:

<div class="code-tabs">

<div class="tab-content">

<div id="tab-3-tier-architecture" class="tab-pane"
title="3-tier architecture">

![Image of the 3-tier-architecture
architecture](/docs/images/3-tier-architecture.svg)

In a [3-tier
architecture](/docs/manage/operational-guidelines/#three-tier-architecture)
where queries are served from a cluster different from the
compute/transform cluster that maintains the view results:

- Use materialized view(s) in the compute/transform cluster for the
  query results that will be served.

  If you are using **stacked views** (i.e., views whose definition
  depends on other views) to reduce SQL complexity, generally, only the
  topmost view (i.e., the view whose results will be served) should be a
  materialized view. The underlying views that do not serve results do
  not need to be materialized.

- Index the materialized view in the serving cluster(s) to serve the
  results from memory.

</div>

<div id="tab-2-tier-architecture" class="tab-pane"
title="2-tier architecture">

![Image of the
2-tier-architecture](/docs/images/2-tier-architecture.svg)

In a [2-tier
architecture](/docs/manage/appendix-alternative-cluster-architectures/#two-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve
  the results from memory.

<div class="tip">

**💡 Tip:** Except for when used with a
[sink](/docs/serve-results/sink/), [subscribe](/docs/sql/subscribe/), or
[temporal filters](/docs/transform-data/patterns/temporal-filters/),
avoid creating materialized views on a shared cluster used for both
compute/transformat operations and serving queries. Use indexed views
instead.

</div>

</div>

<div id="tab-1-tier-architecture" class="tab-pane"
title="1-tier architecture">

![Image of the
1-tier-architecture](/docs/images/1-tier-architecture.svg)

In a [1-tier
architecture](/docs/manage/appendix-alternative-cluster-architectures/#one-tier-architecture)
where queries are served from the same cluster that performs the
compute/transform operations:

- Use view(s) in the shared cluster.

- Index the view(s) to incrementally update the view results and serve
  the results from memory.

<div class="tip">

**💡 Tip:** Except for when used with a
[sink](/docs/serve-results/sink/), [subscribe](/docs/sql/subscribe/), or
[temporal filters](/docs/transform-data/patterns/temporal-filters/),
avoid creating materialized views on a shared cluster used for both
compute/transformat operations and serving queries. Use indexed views
instead.

</div>

</div>

</div>

</div>

### Indexes and query optimizations

By making up-to-date results available in memory, indexes can help
[optimize query performance](/docs/transform-data/optimization/), such
as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting
  individual keys).

Specific instances where indexes can be useful to improve performance
include:

- When used in ad-hoc queries.

- When used by multiple queries within the same cluster.

- When used to enable [delta
  joins](/docs/transform-data/optimization/#optimize-multi-way-joins-with-delta-joins).

For more information, see
[Optimization](/docs/transform-data/optimization).

### Best practices

Before creating an index, consider the following:

- If you create stacked views (i.e., views that depend on other views)
  to reduce SQL complexity, we recommend that you create an index
  **only** on the view that will serve results, taking into account the
  expected data access patterns.

- Materialize can reuse indexes across queries that concurrently access
  the same data in memory, which reduces redundancy and resource
  utilization per query. In particular, this means that joins do **not**
  need to store data in memory multiple times.

- For queries that have no supporting indexes, Materialize uses the same
  mechanics used by indexes to optimize computations. However, since
  this underlying work is discarded after each query run, take into
  account the expected data access patterns to determine if you need to
  index or not.

## Related pages

- [Optimization](/docs/transform-data/optimization)
- [Views](/docs/concepts/views)
- [`CREATE INDEX`](/docs/sql/create-index)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/concepts/indexes.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
