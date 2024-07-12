---
title: Views (Non-Materialized and Materialized)
description: Learn about views in Materialize.
menu:
  main:
    parent: concepts
    weight: 15
    identifier: 'concepts-views'
aliases:
  - /get-started/key-concepts/#views
---

## Overview

Views represent queries that are saved under a name for reference. Views provide
a shorthand for the underlying query.

Materialize offers the following types of views:

Type |
-----|------------
[**Non-materialized views**](#non-materialized-views) | Results are **not** persisted in durable storage. Non-materialized views can be indexed to maintain and incrementally update results in memory.
[**Materialized views**](#materialized-views) | Results **are** persisted and incrementally updated in durable storage. Materialized views can be indexed to load the updated results from durable storage into memory.

All views in Materialize are built by reading data from
[sources](/concepts/sources) and other views.

## Non-materialized views

A non-materialized view, also generally referred to as a "view", saves a query
under a name to provide a shorthand for referencing the query. The query is
**not** executed during non-materialized view creation, and a non-materialized
view does **not** persist its results in durable storage.

**However**, you can [index](/concepts/indexes/) a non-materialized view to
**maintain and incrementally update view results** in memory within a cluster.
This enables queries within that [cluster](/concepts/clusters/) to use the index
to access view results from memory.  See [Indexes and non-materialized
views](#indexes-and-non-materialized-views) for more information.

See also:

- [`CREATE VIEW`](/sql/create-view)

### Indexes and non-materialized views

Indexes can improve query performance. In Materialize, non-materialized views
can be [indexed](/concepts/indexes/). Indexing a non-materialize view causes
view results to be **maintained and incrementally updated in memory** within
that [cluster](/concepts/clusters/). The in-memory up-to-date results are
accessible to queries within the cluster, even for queries that do not use the
index key(s).

Indexes are local to a cluster.

See also:

- [Indexes](/concepts/indexes/)
- [`CREATE INDEX`](/sql/create-index/)

### Clusters and non-materialized views

A non-materialized view can be referenced in any [cluster](/concepts/clusters/).
However, if the non-materialized view is indexed, in-memory results are only
accessible to queries within the cluster.

Indexes are local to a cluster.  Queries in a different cluster cannot use the
indexes in another cluster.

## Materialized views

A materialized view, like a non-materialized view, saves a query under a name to
provide a shorthand for referencing the query. But, unlike a non-materialized
view, the query is executed during the view creation, and a materialized view
**persists and incrementally updates** its results in durable storage.

You can index a materialized view to load the up-to-date view results in memory
within the cluster. This enables queries within the cluster to use the index to
access view results from memory.  See [Indexes and materialized
views](#indexes-and-materialized-views) for more information.

See also:

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)


### Indexes and materialized views

Indexes can improve query performance. In Materialize,
[indexing](/concepts/indexes/) a materialized view loads the already up-to-date
view results from durable storage to memory within the
[cluster](/concepts/clusters/).  Because materialized view maintains the
up-to-date results in durable storage, indexes on materialized views serve
up-to-date results without themselves performing the incremental computation.
The in-memory up-to-date results are accessible to queries within the cluster,
even for queries that do not use the index key(s).

Indexes are local to a cluster.

See also:

- [Indexes](/concepts/indexes/)
- [`CREATE INDEX`](/sql/create-index/)

### Clusters and materialized views

A materialized view can be referenced in any [cluster](/concepts/clusters/).
Because the up-to-date results of a materialized view are persisted in durable
storage, you can decouple the computational resources used for view maintenance
from the resources used for query serving.

Indexes are local to a cluster.  Queries in a different cluster cannot use the
indexes in another cluster.

## Related pages

- [`CREATE VIEW`](/sql/create-view)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`CREATE INDEX`](/sql/create-index)
