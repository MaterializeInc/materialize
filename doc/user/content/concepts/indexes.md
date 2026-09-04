---
title: Indexes
description: "Learn about indexes in Materialize."
menu:
  main:
    parent: concepts
    weight: 20
    identifier: 'concepts-indexes'
aliases:
  - /get-started/key-concepts/#indexes
  - /self-managed/v25.1/concepts/indexes/
  - /self-managed/v25.2/concepts/indexes/
---

## Overview

{{% include-from-yaml data="index_details" name="definition" %}}

## Creating indexes on objects

In Materialize, you can create indexes on [views](/concepts/views/#views) and
[materialized views](/concepts/views/#materialized-views) as well as on
[sources, tables, and subsources](/concepts/sources/).

To create indexes on an object, use the [`CREATE INDEX`](/sql/create-index/)
command. To create the index in a cluster other than the active cluster, include
the `IN CLUSTER` clause in the `CREATE INDEX` statement.

{{% include-example file="examples/create_index" example="syntax" %}}

See [`CREATE INDEX`](/sql/create-index/) for the syntax details.

### Indexes on sources, tables, and subsources

{{< note >}}

In practice, you may find that you rarely need to index a source and its tables
or subsources without performing some transformation using a view, etc.

{{</ note >}}

In Materialize, you can create indexes on [sources, tables, or
subsources](/concepts/sources/) to maintain up-to-date data in the memory of
the cluster where you create the index. This can help improve [query
performance](#indexes-and-query-optimizations), for example when [using
joins](/transform-data/optimization/#join) in your transformation. However, in
practice, you may find that you rarely need to index these objects directly.

```mzsql
CREATE INDEX idx_on_my_source_table ON my_source_table(...);
```

### Indexes on views

In Materialize, you can [create indexes](/sql/create-index/) on a
[view](/concepts/views/#views "query saved under a name") to maintain
**up-to-date view results in memory** within the [cluster](/concepts/clusters/)
where you create the index.

- To create the index in the current active cluster (you can use the `SET
  CLUSTER` command to change the active cluster):

  ```mzsql
  CREATE INDEX idx_on_my_view ON my_view_name(...);
  ```

- To create the index in a specified cluster:

  ```mzsql
  CREATE INDEX idx_on_my_view IN CLUSTER serving_cluster ON my_view_name(...);
  ```

During the index creation, the view is executed and the view results are stored
in memory within the cluster. **As new data arrives**, the index **incrementally
updates** the view results in memory.

Querying a view from a cluster where the view is indexed is **fast** because
the results are already computed and are served from memory. Querying a view
from a cluster where the view isn't indexed requires executing the view each
time you query it.

### Indexes on materialized views

In Materialize, materialized view results are stored in durable storage and
**incrementally updated** as new data arrives. [Indexing](/sql/create-index/) a
materialized view makes the already up-to-date view results available **in
memory** within the [cluster](/concepts/clusters/) where you create the index.
That is, indexes on materialized views require no additional computation to keep
results up-to-date.

{{< note >}}

A materialized view can be queried from any cluster whereas its indexed results
are available only within the cluster where you create the index. Querying a
materialized view from any cluster, whether the materialized view is indexed or
not, is fast because the results are already computed. However, querying an
indexed materialized view from a cluster where the materialized view is indexed
is faster since the results are served from memory rather than from storage.

{{</ note >}}

- To create the index in the current active cluster (you can use the `SET
  CLUSTER` command to change the active cluster):

  ```mzsql
  CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...);
  ```

- To create the index in a specified cluster:

  ```mzsql
  CREATE INDEX idx_on_my_mat_view IN CLUSTER serving_cluster ON my_mat_view_name(...);
  ```

## Properties

### Cluster-local

{{% include-from-yaml data="index_details" name="index-cluster-local" %}} As
such, references to the indexed object from a different cluster cannot use the
index.

### Data distribution and ordering

{{% include-from-yaml data="index_details" name="index-key-distribution" %}}

{{% include-from-yaml data="index_details" name="index-key-ordering-within-workers" %}}

### Serving ad-hoc queries

Within a cluster, all ad-hoc queries that reference an indexed object read from
the index, regardless of whether the index is optimized for the query. This
includes queries that do not specify a `WHERE` condition on the index key.
Because the indexed results are already up-to-date and in memory, reading from
an index avoids recomputing the results.

- **Point lookups**: For queries that specify an equality condition on the full
  index key, Materialize can perform a point lookup, reading only the matching
  records from the index. Point lookups are the most efficient use of an index.
  See [Point lookups](#point-lookups) for the exact requirements.

- **Index scans**: Otherwise, Materialize scans the index. Although the indexed
  results are already up-to-date and in memory, a full index scan must examine
  the indexed results and is less efficient than a point lookup. The performance
  of full index scans degrades with data volume.

### Index use by objects

{{% include-from-yaml data="index_details" name="index-reuse" %}}

To inspect index reuse and dependencies:

- To check whether a new index would reuse an existing index before creating
  it, use [`EXPLAIN CREATE INDEX`](/sql/explain-plan/).

- To find which indexes and materialized views use an index, query
  [`mz_internal.mz_materialization_dependencies`](/reference/system-catalog/mz_internal/#mz_materialization_dependencies).

### Limitations

{{% include-from-yaml data="index_details" name="index-not-optimized" %}}

## Point lookups vs index scans

### Point lookups

Point lookups read just the matching records from the index and are the most
efficient use of an index. Materialize performs a point lookup if the query's
`WHERE` clause:

- Specifies equality (`=` or `IN`) condition and **only** equality conditions on
  **all** the indexed fields. The equality conditions must specify the **exact**
  index key expression (including type) for point lookups. For example:

  - If the index is on `round(quantity)`, the query must specify equality
    condition on `round(quantity)` (and not just `quantity`) for Materialize to
    perform a point lookup.

  - If the index is on `quantity * price`, the query must specify equality
    condition on `quantity * price` (and not `price * quantity`) for Materialize
    to perform a point lookup.

  - If the index is on the `quantity` field which is an integer, the query must
    specify an equality condition on `quantity` with a value that is an integer.

- Only uses `AND` (conjunction) to combine conditions for **different** fields.

For queries whose `WHERE` clause meets the point lookup criteria and includes
conditions on additional fields (also using `AND` conjunction), Materialize
performs a point lookup on the index keys and then filters the results using the
additional conditions on the non-indexed fields.

### Index scans

For queries that do not meet the [point lookup criteria](#point-lookups),
Materialize performs a full index scan (including for range queries). That is,
Materialize performs a full index scan if the `WHERE` clause:

- Does not specify **all** the indexed fields.
- Does not specify only equality conditions on the index fields or specifies an
  equality condition that specifies a different value type than the index key
  type.
- Uses `OR` (disjunction) to combine conditions for **different** fields.

Full index scans are less efficient than point lookups. The performance of full
index scans will degrade with data volume; i.e., as you get more data, full
scans will get slower.

### Examples

Within a cluster, indexes can serve queries that reference an indexed object,
regardless of whether the index is optimized for the query.

Consider the following index on the `orders_view`:

```mzsql
CREATE INDEX idx_orders_view_qty ON orders_view (quantity);
```

Materialize can use the index to serve various queries on the `orders_view`
(and not just queries that specify conditions on `orders_view.quantity`). For
example:

```mzsql
SELECT * FROM orders_view;  -- scans the index
SELECT * FROM orders_view WHERE status = 'shipped';  -- scans the index
SELECT * FROM orders_view WHERE quantity = 10;  -- point lookup on the index
```

For the queries that do not satisfy the [point-lookup
conditions](#point-lookups), Materialize scans the index.

The following table shows various queries and whether Materialize performs a
point lookup or an index scan.

{{% yaml-table data="examples/index_usage/index_usage_key_quantity" %}}

Consider that the view has an index on the `quantity` and `price` fields
instead of an index on the `quantity` field:

```mzsql
DROP INDEX idx_orders_view_qty;
CREATE INDEX idx_orders_view_qty_price on orders_view (quantity, price);
```

{{% yaml-table data="examples/index_usage/index_usage_key_quantity_price" %}}

## Usage

### Indexes on views vs. materialized views

{{% include-from-yaml data="index_view_details" name="table-usage-pattern-intro" %}}
{{% include-from-yaml data="index_view_details" name="table-usage-pattern" %}}
{{% include-md file="content/headless/mat-view-use-cases.md" %}}

### Indexes and query optimizations

By making up-to-date results available in memory, indexes can help [optimize
query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

{{% include-from-yaml data="index_view_details" name="index-query-optimization-specific-instances" %}}

### Best practices

{{% include-from-yaml data="index_view_details" name="index-best-practices" %}}

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
