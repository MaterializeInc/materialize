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
[cluster](/concepts/clusters/)**. You can create indexes on
[sources](/concepts/sources/), [views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Indexes on sources

{{< note >}}
In practice, you may find that you rarely need to index a source
without performing some transformation using a view, etc.
{{</ note >}}

In Materialize, you can create indexes on a [source](/concepts/sources/) to
maintain in-memory up-to-date source data within the cluster you create the
index. This can help improve [query
performance](#indexes-and-query-optimizations) when serving results directly
from the source or when [using joins](/transform-data/optimization/#join).
However, in practice, you may find that you rarely need to index a source
directly.

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

```mzsql
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot use the
indexes in another cluster.

For example, to create an index in the current cluster:

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

You can also explicitly specify the cluster:

```mzsql
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```

## Usage patterns

### Indexes on views vs. materialized views

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
