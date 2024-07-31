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

Indexes can [optimize query performance](/transform-data/optimization).
For example, indexes in Materialize can:

- Provide faster sequential access.

- Provide fast random access for lookup queries (i.e., selecting individual keys).

Additionally, because indexes in Materialize are maintained in memory, indexing
views and materialized views can provide further performance improvements.

## Indexes and views

In Materialize, indexes on a [view](/concepts/views/#views) **maintain and
incrementally update** view results in memory within a
[cluster](/concepts/clusters/). The in-memory up-to-date results are accessible
to queries within the cluster, even for queries that do not use the index
key(s).

## Indexes and materialized views

In Materialize, indexing a
[materialized view](/concepts/views/#materialized-views) maintains results in memory within the [cluster](/concepts/clusters/). Because
materialized views maintain the up-to-date results in durable storage, indexes
on materialized views serve up-to-date results without themselves performing the
incremental computation. The in-memory results are accessible to queries within
the cluster, even for queries that do not use the index key(s).

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot use the
indexes in another cluster.

## Related pages

- [Optimization](/transform-data/optimization)
- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)

<style>
red { color: Red; font-weight: 500; }
</style>
