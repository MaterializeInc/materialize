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

#### `with_options`

{{< diagram "with-options.svg" >}}

| Field      | Value     | Description                                                                                                                                                       |
| ---------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ASSERT NOT NULL** _col_ident_ | `text` | The column identifier for which to create a [non-null assertion](#non-null-assertions). To specify multiple columns, use the option multiple times. |
| **RETAIN HISTORY FOR** _retention_period_ | `interval` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data for performing [time travel queries](/transform-data/patterns/time-travel-queries). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`. |
| **REFRESH _refresh_strategy_** | | ***Private preview.** This option has known performance or stability issues and is under active development.* The refresh strategy for the materialized view. See [Refresh strategies](#refresh-strategies) for syntax options. <br>Default: `ON COMMIT`. |

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

Because materialized views may be created on arbitrary queries, it may not in
all cases be possible for Materialize to automatically infer non-nullability of
some columns that can in fact never be null. In such a case, `ASSERT NOT NULL`
clauses may be used as described in the syntax section above. Specifying
`ASSERT NOT NULL` for a column forces that column's type in the materialized
view to include `NOT NULL`. If this clause is used erroneously, and a `NULL`
value is in fact produced in a column for which `ASSERT NOT NULL` was
specified, querying the materialized view will produce an error until the
offending row is deleted.

### Refresh strategies

{{< private-preview />}}

Depending on your use case, you might have data that doesn't require
up-to-the-second freshness, or that can be accessed using different patterns to
optimize for performance and cost (e.g., hot vs. cold data). To support these
use cases, you can tweak the refresh strategy of a materialized view.

{{< note >}}
We **do not** recommend using this feature if you're looking for very frequent
refreshes (e.g., every few minutes). For cost savings to be significant in
Materialize, the target refresh interval should be at least a few hours;
otherwise, you'll want to stick with the [default behavior](#refresh-on-commit).
{{< /note >}}

Materialized views configured with a refresh strategy are **not incrementally
maintained**, and must recompute their results from scracth on every refresh.
Because these views can be hosted in [scheduled clusters](../sql/create-cluster/#scheduling),
which automatically turn on and off based on the configured refresh strategies,
this feature can lead to significant cost savings when handling large volumes of
historical data that is updated less frequently.

[//]: # "TODO(morsapaes) We should add a SQL pattern that walks through a
full-blown example of how to implement the cold, warm, hot path with refresh
strategies."

#### Refresh on commit

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH ON COMMIT</code></p>

By default, Materialize refreshes a materialized view on every change to its
inputs (i.e., on commit) — this guarantees that results are incrementally
updated, fresh and consistent as new data arrives. Refresh on commit is
the **default** when you create a materialized view that doesn't explicitly
specify a refresh strategy, and is the **recommended behavior for the vast
majority of use cases**.

#### Refresh at

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH AT</code> { <code>CREATION</code> | <i>timestamp</i> }</p>

This strategy allows configuring a materialized view to **refresh at a specific
time**. The refresh time can be specified as a timestamp, or using the `ON
CREATION` clause, which triggers a first refresh when the materialized view is
created.

**Example**

To create a materialized view that is refreshed at creation, and then at the
specified dates:

```sql
CREATE MATERIALIZED VIEW mv_refresh_at
IN CLUSTER my_refresh_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first scheduled refresh
  REFRESH AT CREATION,
  -- Refresh at a specific (future) time
  REFRESH AT '2024-06-06 12:00:00',
  -- Refresh at another specific (future) time
  REFRESH AT '2024-06-08 22:00:00'
)
AS SELECT ... FROM ...;
```

You can specify multiple `REFRESH AT` strategies in the same `CREATE` statement,
and combine them with the [refresh every strategy](#refresh-every). We
recommend **always** using the `REFRESH AT CREATION` strategy with `REFRESH
EVERY`, so the materialized view is available for querying ahead of the first
scheduled refresh.

#### Refresh every

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH EVERY</code> <i>interval</i> [ <code>ALIGNED TO</code> <i>timestamp</i> ]</code></p>

This strategy allows configuring a materialized view to **refresh at regular
intervals**. The `ALIGNED TO` clause additionally allows specifying the _phase_
of the scheduled refreshes: for daily refreshes, it specifies the time of the
day when the refresh will happen; for weekly refreshes, it specifies the day of
the week and the time of the day when the refresh will happen. If `ALIGNED TO`
is not specified, it defaults to the time when the materialized view is
created.

**Example**

To create a materialized view that is refreshed at creation, and then once a day
at 10PM UTC:

```sql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first scheduled refresh
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-04-17 22:00:00'
)
AS SELECT ...  FROM ...;
```

You can specify multiple `REFRESH EVERY` strategies in the same `CREATE`
statement, and combine them with the [refresh at strategy](#refresh-at).

#### Introspection

To check details about the (non-default) refresh strategies associated with any materialized
view in the system, you can query
the [`mz_internal.mz_materialized_view_refresh_strategies`](../system-catalog/mz_internal/#mz_materialized_view_refresh_strategies)
and [`mz_internal.mz_materialized_view_refreshes`](../system-catalog/mz_internal/#mz_materialized_view_refreshes)
system catalog tables:

```sql
SELECT mv.id AS materialized_view_id,
       mv.name AS materialized_view_name,
       rs.type AS refresh_strategy,
       rs.interval AS refresh_interval,
       rs.aligned_to AS refresh_interval_phase,
       rs.at AS refresh_time,
       r.last_completed_refresh,
       r.next_refresh
FROM mz_internal.mz_materialized_view_refresh_strategies rs
JOIN mz_internal.mz_materialized_view_refreshes r ON r.materialized_view_id = rs.materialized_view_id
JOIN mz_materialized_views mv ON rs.materialized_view_id = mv.id;
```

[//]: # "TODO(morsapaes) Add section linking to refresh strategies docs
in #27521."

## Examples

### Creating a materialized view

```mzsql
CREATE MATERIALIZED VIEW winning_bids AS
SELECT auction_id,
       bid_id,
       item,
       amount
FROM highest_bid_per_auction
WHERE end_time < mz_now();
```

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

### Using refresh strategies

```sql
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  -- Refresh every Tuesday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-04 12:00:00',
  -- Refresh every Thursday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-06 12:00:00',
  -- Refresh on creation, so the view is populated ahead of
  -- the first scheduled refresh
  REFRESH AT CREATION
)
AS SELECT ... FROM ...;
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
