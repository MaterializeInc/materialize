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


Indexes can help [optimize query performance](/transform-data/optimization/),
such as:

- Provide faster sequential access than unindexed data.

- Provide fast random access for lookup queries (i.e., selecting individual
  keys).


{{< tip >}}

In Materialize, you do not need to always create indexes to help optimize
computations. For queries that have no supporting indexes, Materialize uses the
same mechanics used by indexes to optimize computations.

Specific instances where indexes can be useful to improve performance include:

- When used in ad-hoc queries.

- When used by multiple queries within the same cluster.

- When used to enable delta joins.

For more information, see [Optimization](/transform-data/optimization).

{{</ tip >}}

## Indexes and views

In Materialize, indexes on a [view](/concepts/views/#views) **maintain and
incrementally update** view results in memory within a
[cluster](/concepts/clusters/). The in-memory up-to-date results are accessible
to queries within the cluster.

## Indexes and materialized views

In Materialize, indexing a
[materialized view](/concepts/views/#materialized-views) maintains results in memory within the [cluster](/concepts/clusters/). Because
materialized views maintain the up-to-date results in durable storage, indexes
on materialized views serve up-to-date results without themselves performing the
incremental computation. However, unlike the materialized view, indexes are
local to the cluster.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
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

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
