---
title: Views
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

Type |
-----|------------
[**Views**](#views) | Results are **not** persisted in durable storage. Views can be indexed to maintain and incrementally update results in memory within a cluster.
[**Materialized views**](#materialized-views) | Results **are** persisted and incrementally updated in durable storage. Materialized views can be indexed to maintain the results in memory.

All views in Materialize are built by reading data from
[sources](/concepts/sources) and other views.

## Views

A view saves a query under a name to provide a shorthand for referencing the
query. Views are not associated with a cluster, and can be referenced across
[clusters](/concepts/clusters/).

During view creation, the underlying query is not executed, so results are not
persisted. This means that view results will be recomputed from scratch each
time the view is accessed.
results are not persisted in durable storage.

```mzsql
CREATE VIEW my_view_name AS
  SELECT ... FROM ...  ;
```

<red>However</red>, in Materialize, you can create an [index](/concepts/indexes/)
on a view to **maintain and incrementally update** its results in memory within
a cluster.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

Queries within that cluster can use the index to access view results from
memory.  See [Indexes and views](#indexes-and-views) for more information.

See also:

- [`CREATE VIEW`](/sql/create-view)  for complete syntax information
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information

### Indexes and views

In Materialize, views can be [indexed](/concepts/indexes/).

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

Indexes on views both **maintain and incrementally update view results in
memory** within that [cluster](/concepts/clusters/). The in-memory up-to-date
results are accessible to queries within the cluster, even for queries that do
not use the index key(s).

See also:

- [Indexes](/concepts/indexes)
- [Optimization](/transform-data/optimization)
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information

## Materialized views

In Materialize, a materialized view is a view whose underlying query is executed
during the view creation, and the view results are both **persisted and
incrementally updated** in durable storage. Materialized views can be referenced across [clusters](/concepts/clusters/).

```mzsql
CREATE MATERIALIZED VIEW my_mat_view_name AS
  SELECT ... FROM ...  ;
```

You can index a materialized view to maintain the results in memory
within the cluster. This enables queries within the cluster to use the index to
access view results from memory.


See also:

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view) for complete
  syntax information


### Indexes and materialized views

In Materalize, materialized views can be indexed.

```mzsql
CREATE INDEX idx_on_my_view ON my_mat_view_name(...) ;
```

Because materialized views maintain the up-to-date results in durable storage,
indexes on materialized views serve up-to-date results without themselves
performing the incremental computation. The in-memory, up-to-date results are
accessible to queries within the cluster, even for queries that do not use the
index key(s).

See also:

- [Indexes](/concepts/indexes)
- [Optimization](/transform-data/optimization)
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information


## General information

- Views can be referenced across [clusters](/concepts/clusters/).

- Materialized views can be referenced across [clusters](/concepts/clusters/).

- [Indexes](/concepts/indexes) are local to a cluster.

- Views can be monotonic; that is, views can be recognized as append-only.

- Materialized views are not monotonic; that is, materialized views cannot be
  recognized as append-only.


<style>
red { color: Red; font-weight: 500; }
</style>
