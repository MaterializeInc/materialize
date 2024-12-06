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
---

## Overview

In Materialize, indexes represent query results stored in memory **within a
[cluster](/concepts/clusters/)**. You can create indexes on [sources](/concepts/sources/),
[views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Index structure

Indexes in Materialize have the following structure for each unique row:

```none
((tuple of indexed key expression), (tuple of the row, i.e. stored columns))
```

Unlike some other databases where indexes store the key and a pointer to the
associated rows, indexes in Materialize store **both** the key and any
associated rows. As such, the in-memory size of indexes are proportional to the
current size of the source or view they represent. See [Indexes and
memory](#indexes-and-memory) for more details.

## Indexes on sources

{{< note >}}
In practice, you may find that you rarely need to index a source
or a table without performing some transformation using a view, etc.
{{</ note>}}

In Materialize, you can create indexes on a [source](/concepts/sources/) to
maintain in-memory, up-to-date source data in the cluster you create the index.
This can help improve [query performance](#indexes-and-query-optimizations) when
[using joins](/transform-data/optimization/#join) or when serving results
directly from a source. However, in practice, you may find that you rarely need
to index a source directly.

```mzsql
CREATE INDEX idx_on_my_source ON my_source (...);
```

## Indexes on views

In Materialize, you can create indexes on a [view](/concepts/views/#views "query
saved under a name") to maintain **up-to-date view results in memory** within
the [cluster](/concepts/clusters/) you create the index.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

During the index creation on a [view](/concepts/views/#views "query saved under
a name"), the view is executed and the view results are stored in memory within
the cluster. **As new data arrives**, the index **incrementally updates** the
results in memory.

Within the cluster, querying an indexed view is:

- **fast** because the results are served from memory, and

- **computationally free** because no computation is performed on read.

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized view results are stored in durable storage and
**incrementally updated** as new data arrives. That is, the materialized view
contains up-to-date results. You can index a materialized view to make the
already up-to-date view results available **in memory** within the
[cluster](/concepts/clusters/) you create the index. That is, indexes on
materialized views require no additional computation to keep results up-to-date.

```mzsql
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```

{{< note >}}

A materialized view can be queried from any cluster whereas its indexed results
are available only within the cluster you create the index. Querying a
materialized view, whether indexed or not, from any cluster is computationally
free. However, querying an indexed materialized view within the cluster where
the index is created is faster since the results are served from memory rather
than from storage.

{{</ note >}}

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes and clusters

Indexes are local to a cluster. Queries executed against a cluster where the
desired indexes do not exist **will not** use the indexes in another cluster.

For example:

- Create an index in the current cluster:

  ```mzsql
  CREATE INDEX idx_on_my_view ON my_view_name(item) ;
  ```

  Then, only queries in the current cluster can use the index depending on the
  query pattern.

- Explicitly specify the cluster when creating the index:

  ```mzsql
  CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
  ```

  Then, only queries in the `active_cluster` cluster can use the index depending
  on the query pattern.

To verify that a query on the view uses the index when executed from the same
cluster, run [`EXPLAIN PLAN FOR`](/sql/explain-plan/) the query in the cluster.
For example, to verify that a `SELECT` from `my_view` uses the index when
executed from the `active_cluster` cluster, run [`EXPLAIN PLAN
FOR`](/sql/explain-plan/) on the query.

```mzsql
SET cluster = active_cluster;
EXPLAIN PLAN FOR SELECT * FROM my_view;
```

The plan includes both a `(fast path)` indicator as well as a `Used Indexes`
information, indicating that the query would use the index.

To verify that a query from a different cluster does not use the index, run
`EXPLAIN PLAN FOR` the query from a different cluster. The returned plan does
not include either the `(fast path)` indicator or the `Used Indexes`
information.

```mzsql
SET cluster = not_current_cluster;
EXPLAIN PLAN FOR SELECT * FROM my_view;
```

See also [Indexes and query optimization](/transform-data/optimization/#indexes)
for information on when queries can use the index.

## Indexes and memory

{{% views-indexes/indexes-memory-footprint %}}

## Usage patterns

### Indexes on views vs. materialized views

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

### Indexes and query optimizations

By making up-to-date results available in memory, indexes can help [optimize
query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

{{% views-indexes/index-query-optimization-specific-instances %}}

### Best practices

{{% views-indexes/index-best-practices %}}

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
