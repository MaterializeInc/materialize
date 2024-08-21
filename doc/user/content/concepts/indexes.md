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

In Materialize, indexes on views both **compute, and as new data arrives,
incrementally update view results in memory** within that
[cluster](/concepts/clusters/). Within the cluster, the in-memory up-to-date
results are immediately available and computationally free to query.

In Materialize, indexes on a [view](/concepts/views/#views) **compute and, as
new data arrives, incrementally update** view results in memory within a
[cluster](/concepts/clusters/) instead of recomputing the results from scratch.

Within the cluster, the in-memory up-to-date results are immediately available
and computationally free to query.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

For considerations on using indexes as well as using indexes vs. materialized
views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized views already maintain the up-to-date results in
durable storage. Indexing a [materialized
view](/concepts/views/#materialized-views) loads the already up-to-date results
into memory; that is, the indexes on materialized views are able to serve
up-to-date results without themselves performing the incremental updates.
However, unlike materialized views, indexes are accessible only within a
[cluster](/concepts/clusters/).

For considerations on using indexes as well as using indexes vs. materialized
views, see [Usage patterns](#usage-patterns).

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

In addition to providing incrementally-maintained results in memory, indexes
can help [optimize query performance](/transform-data/optimization/), such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).

{{% views-indexes/index-query-optimization-specific-instances %}}

### General considerations

{{% views-indexes/index-considerations %}}

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
