---
title: Indexes
description: Conceptual page on indexes.
menu:
  main:
    parent: concepts
    weight: 20
    identifier: 'concepts-indexes'
---

## Overview

Indexes can improve query performance.

## Indexes and non-materialized views

In Materialize, indexing a [non-materialize
view](/concepts/views/#non-materialized-views) causes view results to be
**maintained and incrementally updated in memory** within the
[cluster](/concepts/clusters/). The in-memory up-to-date results are accessible
to queries within the cluster, even for queries that do not use the index
key(s).

Queries from other clusters cannot use the index.

## Indexes and materialized views

In Materialize, indexing a [materialize
view](/concepts/views/#materialized-views) loads the already up-to-date view
results from durable storage to memory within the
[cluster](/concepts/clusters/).  Because materialized view maintains the
up-to-date results in durable storage, indexes on materialized views serve
up-to-date results without themselves performing the incremental computation.
The in-memory up-to-date results are accessible to queries within the cluster,
even for queries that do not use the index key(s).

Queries from other clusters cannot use the index.


## Related pages

- [Views](/concepts/views)
- [`CREATE INDEX`](/sql/create-index)
