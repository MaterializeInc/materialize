---
audience: developer
canonical_url: https://materialize.com/docs/concepts/indexes/
complexity: advanced
description: Learn about indexes in Materialize.
doc_type: concept
keywords:
- As new data arrives
- 'within a

  [cluster](/concepts/clusters/)'
- CREATE THE
- up-to-date view results in memory
- incrementally updates
- CREATE INDEX
- CREATE INDEXES
- 'Note:'
- Indexes
product_area: Concepts
status: stable
title: Indexes
---

# Indexes

## Purpose
Learn about indexes in Materialize.

Read this to understand how this concept works in Materialize.


Learn about indexes in Materialize.


## Overview

In Materialize, indexes represent query results stored in memory **within a
[cluster](/concepts/clusters/)**. You can create indexes on
[sources](/concepts/sources/), [views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Indexes on sources

> **Note:** 
In practice, you may find that you rarely need to index a source
without performing some transformation using a view, etc.


In Materialize, you can create indexes on a [source](/concepts/sources/) to
maintain in-memory up-to-date source data within the cluster you create the
index. This can help improve [query
performance](#indexes-and-query-optimizations) when serving results directly
from the source or when [using joins](/transform-data/optimization/#join).
However, in practice, you may find that you rarely need to index a source
directly.

```mzsql
CREATE INDEX idx_on_my_source ON my_source (...);
```bash

## Indexes on views

In Materialize, you can create indexes on a [view](/concepts/views/#views "query
saved under a name") to maintain **up-to-date view results in memory** within
the [cluster](/concepts/clusters/) you create the index.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```text

During the index creation on a [view](/concepts/views/#views "query saved under
a name"), the view is executed and the view results are stored in memory within
the cluster. **As new data arrives**, the index **incrementally updates** the
view results in memory.

Within the cluster, querying an indexed view is:

- **fast** because the results are served from memory, and

- **computationally free** because no computation is performed on read.

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized view results are stored in durable storage and
**incrementally updated** as new data arrives. Indexing a materialized view
makes the already up-to-date view results available **in memory** within the
[cluster](/concepts/clusters/) you create the index. That is, indexes on
materialized views require no additional computation to keep results up-to-date.

> **Note:** 

A materialized view can be queried from any cluster whereas its indexed results
are available only within the cluster you create the index. Querying a
materialized view, whether indexed or not, from any cluster is computationally
free. However, querying an indexed materialized view within the cluster where
the index is created is faster since the results are served from memory rather
than from storage.


For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

```mzsql
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```bash

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot use the
indexes in another cluster.

For example, to create an index in the current cluster:

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```text

You can also explicitly specify the cluster:

```mzsql
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```bash

## Usage patterns

This section covers usage patterns.

### Index usage

<!-- Unresolved shortcode: <!-- Unresolved shortcode: > **Important:**  --> -->
Indexes are local to a cluster. Queries in one cluster cannot use the indexes in another, different cluster.
<!-- Unresolved shortcode: <!-- Unresolved shortcode:  --> -->

Unlike some other databases, Materialize can use an index to serve query results
even if the query does not specify a `WHERE` condition on the index key. Serving
queries from an index is fast since the results are already up-to-date and in
memory.

For example, consider the following index:

```mzsql
CREATE INDEX idx_orders_view_qty ON orders_view (quantity);
```text

Materialize will maintain the `orders_view` in memory in `idx_orders_view_qty`,
and it will be able to use the index to serve a various queries on the
`orders_view` (and not just queries that specify conditions on
`orders_view.quantity`).

Materialize can use the index for the following queries (issued from the same
cluster as the index) on `orders_view`:

```mzsql
SELECT * FROM orders_view;  -- scans the index
SELECT * FROM orders_view WHERE status = 'shipped';  -- scans the index
SELECT * FROM orders_view WHERE quantity = 10;  -- point lookup on the index
```text

For the queries that do not specify a condition on the indexed field,
Materialize scans the index. For the query that specifies an equality condition
on the indexed field, Materialize performs a **point lookup** on the index
(i.e., reads just the matching records from the index). Point lookups are the
most efficient use of an index.

#### Point lookups

Materialize performs **point lookup** (i.e., reads just the matching records
from the index) on the index if the query's `WHERE` clause:

- Specifies equality (`=` or `IN`) condition and **only** equality conditions on
  **all** the indexed fields. The equality conditions must specify the **exact**
  index key expression (including type) for point lookups. For example:

  - If the index is on `round(quantity)`, the query must specify equality
    condition on `round(quantity)` (and not just `quanity`) for Materialize to
    perform a point lookup.

  - If the index is on `quantity * price`, the query must specify equality
    condition on `quantity * price` (and not `price * quantity`) for Materialize
    to perform a point lookup.

  - If the index is on the `quantity` field which is an integer, the query must
    specify an equality condition on `quantity` with a value that is an integer.

- Only uses `AND` (conjunction) to combine conditions for **different** fields.

Point lookups are the most efficient use of an index.

For queries whose `WHERE` clause meets the point lookup criteria and includes
conditions on additional fields (also using `AND` conjunction), Materialize
performs a point lookup on the index keys and then filters the results using the
additional conditions on the non-indexed fields.

For queries that do not meet the point lookup criteria, Materialize performs a
full index scan (including for range queries). That is, Materialize performs a
full index scan if the `WHERE` clause:

- Does not specify **all** the indexed fields.
- Does not specify only equality conditions on the index fields or specifies an
  equality condition that specifies a different value type than the index key
  type.
- Uses OR (disjunction) to combine conditions for **different** fields.

Full index scans are less efficient than point lookups.  The performance of full
index scans will degrade with data volume; i.e., as you get more data, full
scans will get slower.

#### Examples

Consider again the following index on a view:

```mzsql
CREATE INDEX idx_orders_view_qty on orders_view (quantity);
```text

The following table shows various queries and whether Materialize performs a
point lookup or an index scan.


Consider that the view has an index on the `quantity` and `price` fields
instead of an index on the `quantity` field:

```mzsql
DROP INDEX idx_orders_view_qty;
CREATE INDEX idx_orders_view_qty_price on orders_view (quantity, price);
```


#### Limitations

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See index usage documentation --> --> -->

### Indexes on views vs. materialized views

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See views/indexes documentation for details --> --> -->
<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See views/indexes documentation for details --> --> -->
<!-- Unresolved shortcode: {{% include-md file="shared-content/mat-view-use-c... -->

### Indexes and query optimizations

By making up-to-date results available in memory, indexes can help [optimize
query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

<!-- Unresolved shortcode: {{% views-indexes/index-query-optimization-specifi... -->

### Best practices

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See views/indexes documentation for details --> --> -->

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>