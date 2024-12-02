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
[cluster](/concepts/clusters/)**. Indexes in Materialize store **both** the key
and any associated rows.

You can create indexes on [sources](/concepts/sources/), tables,
[views](/concepts/views/#views), or [materialized
views](/concepts/views/#materialized-views).

## Indexes on sources and tables

{{< note >}}
In practice, you may find that you rarely need to index a source
or a table without performing some transformation using a view, etc.
{{</ note>}}

In Materialize, you can create indexes on a [source](/concepts/sources/) or a
[table](/sql/create-table) to maintain in-memory, up-to-date source or table data
in the cluster you create the index. This can help improve [query
performance](#indexes-and-query-optimizations) when [using
joins](/transform-data/optimization/#join) or when serving results directly
from a source or a table. However, in practice, you may find that you rarely
need to index a source or a table directly.

```mzsql
CREATE INDEX idx_on_my_source ON my_source (...);
CREATE INDEX idx_on_my_table ON my_table (...);
```

Indexes in Materialize store **both** the key and any associated rows.

## Indexes on views

In Materialize, you can create indexes on a [view](/concepts/views/#views "query
saved under a name") to maintain **up-to-date view results in memory** within
the [cluster](/concepts/clusters/) you create the index.

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

During the index creation on a [view](/concepts/views/#views "query saved under
a name"), the view is executed. The index stores both the key and the associated
results (i.e., the associated rows) in memory within the cluster. **As
new data arrives**, the index **incrementally updates** the view results.

Within the cluster, querying an indexed view is:

- **fast** because the results are served from memory, and

- **computationally free** because no computation is performed on read.

For best practices on using indexes, and understanding when to use indexed views
vs. materialized views, see [Usage patterns](#usage-patterns).

## Indexes on materialized views

In Materialize, materialized view results are stored in durable storage and
**incrementally updated** as new data arrives. That is, the materialized view
contains up-to-date results. You can index a materialized view to make the
already up-to-date view results available **in memory** within the
[cluster](/concepts/clusters/) you create the index. That is, indexes on
materialized views require no additional computation to keep results up-to-date.

```mzsql
CREATE INDEX idx_on_my_mat_view ON my_mat_view_name(...) ;
```

The index stores both the key and the associated view results in memory within
the cluster.

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

## Indexes and clusters

Indexes are local to a cluster. Queries executed against a cluster where the
desired indexes do not exist **will not** use the
indexes in another cluster.

For example, the following statement creates an index in the current cluster:

```mzsql
CREATE INDEX idx_on_my_view ON my_view_name(...) ;
```

Only queries in the current cluster can use the index.

Similarly, the following statement creates an index in the specified cluster
named `active_cluster`:

```mzsql
CREATE INDEX idx_on_my_view IN CLUSTER active_cluster ON my_view (...);
```

Only queries in the `active_cluster` cluster can use the index.

## Usage patterns

### Indexes on views vs. materialized views

{{% views-indexes/table-usage-pattern-intro %}}
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
