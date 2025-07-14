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

Type                   |
-----------------------|-------------------
[ **Views** ]( #views ) | Results are recomputed from scratch each time the view is accessed. You can create an **[index](/concepts/indexes/)** on a view to keep its results **incrementally updated** and available **in memory** within a cluster. |
[**Materialized views**](#materialized-views) | Results are persisted in **durable storage** and **incrementally updated**. You can create an [**index**](/concepts/indexes/) on a materialized view to make the results available in memory within a cluster.

## Views

A view saves a query under a name to provide a shorthand for referencing the
query. Views are not associated with a [cluster](/concepts/clusters/) and can
be referenced across clusters.

During view creation, the underlying query is not executed. Each time the view
is accessed, view results are recomputed from scratch.

```mzsql
CREATE VIEW my_view_name AS
  SELECT ... FROM ...  ;
```

**However**, in Materialize, you can create an [index](/concepts/indexes/) on a
view to keep view results **incrementally updated** in memory within a cluster.
That is, with **indexed views**, you do not recompute the view results each time
you access the view in the cluster; queries can access the already up-to-date
view results in memory.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

See [Indexes and views](#indexes-on-views) for more information.

See also:

- [`CREATE VIEW`](/sql/create-view)  for complete syntax information
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information

### Indexes on views

In Materialize, views can be [indexed](/concepts/indexes/). Indexes represent
query results stored in memory. Creating an index on a view executes the
underlying view query and stores the view results in memory within that
[cluster](/concepts/clusters/).

For example, to create an index in the current cluster:

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

You can also explicitly specify the cluster:

```mzsql
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```

**As new data arrives**, the index **incrementally updates** view results in
memory within that [cluster](/concepts/clusters/). Within the cluster, the
**in-memory up-to-date** results are immediately available and computationally
free to query.

See also:

- [Indexes](/concepts/indexes)
- [Optimization](/transform-data/optimization)
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information

## Materialized views

In Materialize, a materialized view is a view whose underlying query is executed
during the view creation. The view results are persisted in durable storage,
**and, as new data arrives, incrementally updated**. Materialized views can be
referenced across [clusters](/concepts/clusters/).

To create materialized views, use the [`CREATE MATERIALIZED
VIEW`](/sql/create-materialized-view) command:

```mzsql
CREATE MATERIALIZED VIEW my_mat_view_name AS
  SELECT ... FROM ...  ;
```

See also:

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view) for complete
  syntax information

### Hydration and materialized views

Materialized view undergoes hydration when it is created or when its cluster is
restarted. Hydration refers to the reconstruction of in-memory state by reading
data from Materialize’s storage layer; hydration does not require reading data
from the upstream system.

During hydration, materialized views require memory proportional to both
the input and output.

### Indexes on materialized views

In Materialize, materialized views can be queried from any cluster. In addition,
in Materialize, materialized views can be indexed to make the results available
in memory within the cluster associated with the index. For example, in a 3-tier
architecture where you have a separate source cluster(s), a separate
compute/transform cluster(s) with materialized views, and a separate serving
cluster(s), you can create **in the serving cluster** an index on the
materialized views.

```mzsql
CREATE INDEX idx_on_my_view ON my_mat_view_name(...) ;
```

Because materialized views already maintain the up-to-date results in durable
storage, indexes on materialized views can serve up-to-date results without
having to perform additional computation.

{{< note >}}
Querying a materialized view, whether indexed or not, from any cluster is
computationally free. However, querying an indexed materialized view within the
cluster associated with the index is faster since the results are served from
memory rather than from storage.
{{</ note >}}


See also:

- [Indexes](/concepts/indexes)
- [Optimization](/transform-data/optimization)
- [`CREATE INDEX`](/sql/create-index/)  for complete syntax information

## Indexed views vs. materialized views

{{% views-indexes/table-usage-pattern-intro %}}

{{% views-indexes/table-usage-pattern %}}

{{% include-md file="shared-content/mat-view-use-cases.md" %}}

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
