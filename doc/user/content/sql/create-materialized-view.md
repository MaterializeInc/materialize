---
title: "CREATE MATERIALIZED VIEW"
description: "`CREATE MATERIALIZED VIEW` defines a view that is persisted in durable storage and incrementally updated as new data arrives."
pagerank: 40
menu:
  main:
    parent: 'commands'
---

`CREATE MATERIALIZED VIEW` defines a view that is persisted in durable storage and
incrementally updated as new data arrives.

A materialized view specifies a [cluster](/get-started/key-concepts/#clusters) that
is tasked with keeping its results up-to-date, but **can be referenced in
any cluster**. This allows you to effectively decouple the computational
resources used for view maintenance from the resources used for query serving.

## Syntax

{{< diagram "create-materialized-view.svg" >}}

Field | Use
------|-----
**OR REPLACE** | If a materialized view exists with the same name, replace it with the view defined in this statement. You cannot replace views that other views or sinks depend on, nor can you replace a non-view object with a view.
**IF NOT EXISTS** | If specified, _do not_ generate an error if a materialized view of the same name already exists. <br/><br/>If _not_ specified, throw an error if a view of the same name already exists. _(Default)_
_view&lowbar;name_ | A name for the materialized view.
**(** _col_ident_... **)** | Rename the `SELECT` statement's columns to the list of identifiers, both of which must be the same length. Note that this is required for statements that return multiple columns with the same identifier.
_cluster&lowbar;name_ | The cluster to maintain this materialized view. If not specified, defaults to the active cluster.
_select&lowbar;stmt_ | The [`SELECT` statement](../select) whose results you want to maintain incrementally updated.
**(** **ASSERT NOT NULL** _col_ident_... **)** | ***Private preview.** This option has known performance or stability issues and is under active development.* A list of columns for which to create [non-null assertions](#non-null-assertions).

## Details

### Usage patterns

Maintaining a materialized view in durable storage has resource and latency
costs that should be carefully considered depending on the main usage of the
view. It's a good idea to create a materialized view if:

* The results need to be available across clusters;
* View maintenance and query serving would benefit from being scaled
  independently;
* The final consumer of the view is a sink or a [`SUBSCRIBE`](../subscribe) operation.

On the other hand, if you only need to access a view from a single cluster, you
should consider creating a [non-materialized view](../create-view) and building
an index on it instead. The index will incrementally maintain the results of
the view updated in memory within that cluster, allowing you to avoid the costs
and latency overhead of materialization.

[//]: # "TODO(morsapaes) Point to relevant architecture patterns once these
exist."

### Indexes

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

{{< private-preview />}}

Because materialized views may be created on arbitrary queries, it may
not in all cases be possible for Materialize to automatically infer non-nullability
of some columns that can in fact never be null. In such a case,
`ASSERT NOT NULL` clauses may be used as described in the syntax
section above. Specifying `ASSERT NOT NULL` for a column forces that
column's type in the materialized view to include `NOT NULL`. If this
clause is used erroneously, and a `NULL` value is in fact produced in
a column for which `ASSERT NOT NULL` was specified, querying the
materialized view will produce an error until the offending row is deleted.

## Examples

### Creating a materialized view

```sql
CREATE MATERIALIZED VIEW winning_bids AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

### Using non-null assertions

```sql
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

## Privileges

The privileges required to execute this statement are:

- Ownership of existing `view_name` if `OR REPLACE` is specified.
- `CREATE` privileges on the containing schema.
- `CREATE` privileges on the containing cluster.
- `USAGE` privileges on all types used in the materialized view definition.
- `USAGE` privileges on the schemas that all types in the statement are contained in.

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
