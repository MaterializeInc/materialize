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

Indexes can [optimize query performance](/transform-data/optimization).  In
addition, because indexes in Materialize are maintained in memory, indexing a
view (non-materialized and materialized) may provide further performance
improvements.

## Indexes and views

In Materialize, indexing a [non-materialized
view](/concepts/views/#non-materialized-views) causes view results to be
**maintained and incrementally updated in memory** within the
[cluster](/concepts/clusters/). The in-memory up-to-date results are accessible
to queries within the cluster, even for queries that do not use the index
key(s).

## Indexes and materialized views

In Materialize, indexing a 
[materialized view](/concepts/views/#materialized-views) maintains results from durable
storage in memory within the [cluster](/concepts/clusters/). Because
materialized view maintains the up-to-date results in durable storage, indexes
on materialized views serve up-to-date results without themselves performing the
incremental computation. The in-memory results are accessible to queries within
the cluster, even for queries that do not use the index key(s).

## Indexes and clusters

Indexes are local to a cluster. Queries in a different cluster cannot use the
indexes in another cluster.

## Related pages

- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)
