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

In Materialize, indexes represent query results stored in memory within a
[cluster](/concepts/clusters/). You can create indexes on
[sources](/concepts/sources/), [views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Indexes on sources

{{< note >}}
In practice, you may find that you rarely need to index a source directly
without performing some sort of transformation using a view, etc.
{{</ note >}}

In Materialize, you can create indexes on sources to maintain in-memory
up-to-date source data.

```mzsql
CREATE INDEX idx_on_my_source ON my_source (...);
```

## Indexes on views

In Materialize, creating an index on a view keeps its results **incrementally
updated** as new data arrives. The results of the view are stored **in memory**
within the [cluster](/concepts/clusters/) the index is created in. Querying an
indexed view is fast, because the results are served from memory, and
computationally free, because no computation is performed on read.

In Materialize, indexes on a [view](/concepts/views/#views) **compute and, as
new data arrives, incrementally update** view results in memory within a
[cluster](/concepts/clusters/) instead of recomputing the results from scratch.

Within the cluster, the in-memory up-to-date results are immediately available
and computationally free to query.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, creating an index on a materialized view simply makes its
results available **in memory** within the [cluster](/concepts/clusters/) the
index is created in. The results of a materialized view are otherwise stored
in **durable storage**, and are **incrementally updated** as new data arrives.
This means that creating an index on a materialized view does not require
additional computation, and makes querying the materialized view faster within
the cluster the index is created in.

It's important to note that a materialized view can be queried from any cluster,
but its indexed results are only available within the cluster the index is
created in. Querying a materialized view from a cluster where the view isn't
indexed is slower, because results are served from storage (rather than
memory), but is also computationally free, because no computation is performed
on read.

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

By making incrementally-maintained results available in memory, indexes can
help [optimize query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

{{% views-indexes/index-query-optimization-specific-instances %}}

### Best practices

{{% views-indexes/index-considerations %}}

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
