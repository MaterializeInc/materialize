---
title: "CREATE MATERIALIZED VIEW"
description: "`CREATE MATERIALIZED VIEW` defines a view that is persisted in durable storage and incrementally updated as new data arrives."
pagerank: 40
menu:
  main:
    parent: 'commands'
---

Use `CREATE MATERIALIZED VIEW` to:

- Create a materialized view that maintains [fresh
  results](/concepts/reaction-time) by persisting them in durable storage and
  incrementally updating them as new data arrives.

- Create a replacement for an existing materialized view that can be applied in
  place with [`ALTER MATERIALIZED VIEW ... APPLY
  REPLACEMENT`](/sql/alter-materialized-view/).

Materialized views are particularly useful when you need **cross-cluster
access** to results or want to sink data to external systems like
[Kafka](/sql/create-sink). When you create a materialized view, a
[cluster](/concepts/clusters/), responsible for maintaining the view, is
associated with it, but the results can be **queried from any cluster**. This
allows you to separate the compute resources used for view maintenance from
those used for serving queries.

If you do not need durability or cross-cluster sharing, and you are primarily
interested in fast query performance within a single cluster, you may prefer to
[create a view and index it](/concepts/views/#views). In Materialize, [indexes
on views](/concepts/indexes/) also maintain results incrementally, but store
them in memory, scoped to the cluster where the index was created. This approach
offers lower latency for direct querying within that cluster.

## Syntax

{{< tabs level=3 >}}
{{< tab "CREATE MATERIALIZED VIEW" >}}

{{% include-syntax file="examples/create_materialized_view" example="syntax" %}}

{{< /tab >}}

{{< tab "CREATE REPLACEMENT MATERIALIZED VIEW" >}}

{{% include-headless "/headless/replacement-views/public-preview-annotation" %}}

Create a replacement materialized view for an existing materialized view.

{{% include-syntax file="examples/create_materialized_view" example="syntax-replacement" %}}

The created replacement materialized view starts hydrating immediately and can
later be applied to replace the specified materialized view. For more
information, see [Creating replacement materialized
views](#creating-replacement-materialized-views).

{{< /tab >}}
{{< /tabs >}}

## Details

### Usage pattern

{{% include-from-yaml data="index_view_details" name="table-usage-pattern-intro" %}}
{{% include-from-yaml data="index_view_details" name="table-usage-pattern" %}}

### Indexing materialized views

Although you can query a materialized view directly, these queries will be
issued against Materialize's storage layer. This is expected to be fast, but
still slower than reading from memory. To improve the speed of queries on
materialized views, we recommend creating [indexes](../create-index) based on
common query patterns.

It's important to keep in mind that indexes are **local** to a cluster, and
maintained in memory. As an example, if you create a materialized view and
build an index on it in the `quickstart` cluster, querying the view from a
different cluster will _not_ use the index; you should create the appropriate
indexes in each cluster you are referencing the materialized view in.

[//]: # "TODO(morsapaes) Point to relevant operational guide on indexes once
this exists+add detail about using indexes to optimize materialized view
stacking."


### Non-null assertions

Because materialized views may be created on arbitrary queries, it may not in
all cases be possible for Materialize to automatically infer non-nullability of
some columns that can in fact never be null. In such a case, `ASSERT NOT NULL`
clauses may be used as described in the syntax section above. Specifying
`ASSERT NOT NULL` for a column forces that column's type in the materialized
view to include `NOT NULL`. If this clause is used erroneously, and a `NULL`
value is in fact produced in a column for which `ASSERT NOT NULL` was
specified, querying the materialized view will produce an error until the
offending row is deleted.

### Creating replacement materialized views

{{% include-headless "/headless/replacement-views/public-preview-annotation" %}}

{{% include-headless "/headless/replacement-views/associated-commands-blurb/"
%}}

{{% include-from-yaml data="examples/create_materialized_view"
name="create-replacement-view-syntax-details" %}}

{{< include-from-yaml data="examples/alter_materialized_view"
name="prereq-recommendations-short" >}}

The replacement view is dropped when you apply the replacement view. For more
information on applying the replacement view, including recommendations and
CPU/memory considerations, see [`ALTER MATERIALIZED VIEW ... APPLY
REPLACEMENT...`](/sql/alter-materialized-view/#replacing-a-materialized-view)

See also:

- [Replace materialized
views](/transform-data/updating-materialized-views/replace-materialized-view/)
guide for a step-by-step tutorial.

#### Query performance of replacement views

{{% include-headless "/headless/replacement-views/querying-replacement-view" %}}

#### Restrictions and limitations

{{% include-headless
"/headless/replacement-views/replacement-view-target-restrictions" %}}

{{% include-headless "/headless/replacement-views/replacement-view-index-restrictions" %}}

## Examples

### Creating a materialized view

{{% include-example file="examples/create_materialized_view" example="example-create-materialized-view" %}}

### Using non-null assertions

```mzsql
CREATE MATERIALIZED VIEW users_and_orders WITH (
  -- The semantics of a FULL OUTER JOIN guarantee that user_id is not null,
  -- because one of `users.id` or `orders.user_id` must be not null, but
  -- Materialize cannot yet automatically infer that fact.
  ASSERT NOT NULL user_id
)
AS
SELECT
  coalesce(users.id, orders.user_id) AS user_id,
  ...
FROM users FULL OUTER JOIN orders ON users.id = orders.user_id
```

[//]: # "TODO(morsapaes) Add more elaborate examples with \timing that show
things like querying materialized views from different clusters, indexed vs.
non-indexed, and so on."

### Creating a replacement materialized view

{{% include-headless "/headless/replacement-views/public-preview-annotation" %}}

{{% include-from-yaml file="examples/create_materialized_view"
name="replacement-view-syntax-details" %}}

{{% include-example file="examples/create_materialized_view"
example="example-create-replacement-materialized-view" %}}

To replace the existing view with its replacement, see [`ALTER MATERIALIZED
VIEW`](../alter-materialized-view).

See also:

- [Replace materialized views guide
](/transform-data/updating-materialized-views/replace-materialized-view/)


## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-materialized-view" %}}

## Additional information

- Materialized views are not monotonic; that is, materialized views cannot be
  recognized as append-only.

## Related pages

- [`ALTER MATERIALIZED VIEW`](../alter-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
