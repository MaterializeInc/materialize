---
title: "CREATE MATERIALIZED VIEW"
description: "`CREATE MATERIALIZED VIEW` defines a view that is persisted in durable storage and incrementally updated as new data arrives."
pagerank: 40
menu:
  main:
    parent: 'commands'
---

{{< note >}}

Depending on your use case, instead of a materialized view, you might prefer to
[create a view and index it](/concepts/views/#views). In Materialize, [indexes
on views](/concepts/indexes/) **maintain and incrementally update view results
in memory** for the cluster where you create the index.  See [Usage Patterns](#usage-patterns).

{{</ note >}}

`CREATE MATERIALIZED VIEW` defines a view that is persisted in durable storage and
incrementally updated as new data arrives.

A materialized view specifies a [cluster](/concepts/clusters/) that
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
| **RETAIN HISTORY FOR** _retention_period_ | `interval` | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.
| **REFRESH _refresh_strategy_** | | ***Private preview.** This option has known performance or stability issues and is under active development.* The refresh strategy for the materialized view. See [Refresh strategies](#refresh-strategies) for syntax options. <br>Default: `ON COMMIT`. |

## Details

### Usage patterns

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

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
use cases, you can configure a non-default refresh strategy for materialized
views.

{{< note >}}
We **do not** recommend using this feature if you're looking for very frequent
refreshes (e.g., every few minutes). For cost savings to be significant in
Materialize, the target refresh interval should be at least a few hours;
otherwise, you'll want to stick with the [default behavior](#refresh-on-commit).
{{< /note >}}

Materialized views configured with a refresh strategy are **not incrementally
maintained** and must recompute their results from scratch on every refresh.
Because these views can be hosted in [scheduled clusters](/sql/create-cluster/#scheduling),
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

Depending on your use case, it might make sense to trade-off freshness for
performance and cost. For example, if it's important to keep results up-to-date
for the most recent data, but not as much once data goes over a certain time
threshold, it might be tolerable for changes to the older data to take a longer
time to reflect.

**Example**

To implement this pattern, you can maintain the recent data in a regular
materialized view that refreshes on commit, create a second materialized view
for data that goes over a specific threshold (e.g., one week) using a
[refresh every](#refresh-every) strategy with the desired freshness interval
(e.g., one day), and then union these views to get the entire result set.

```mzsql
CREATE MATERIALIZED VIEW mv AS
SELECT ...
-- Keep data newer than one week
WHERE mz_now() <= event_ts + INTERVAL '1' WEEK;
```

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at midnight UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-04-17 00:00:00'
) AS
SELECT ...
-- Keep data older than one week
WHERE mz_now() > event_ts + INTERVAL '1' WEEK
```

```mzsql
CREATE VIEW v_mv_results AS
SELECT * FROM mv
UNION ALL
SELECT * FROM mv_refresh_every;
```

#### Refresh at

<p style="font-size:14px"><b>Syntax:</b> <code>REFRESH AT</code> { <code>CREATION</code> | <i>timestamp</i> }</p>

This strategy allows configuring a materialized view to **refresh at a specific
time**. The refresh time can be specified as a timestamp, or using the `AT CREATION`
clause, which triggers a first refresh when the materialized view is created.

**Example**

To create a materialized view that is refreshed at creation, and then at the
specified times:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_at
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh at a user-specified (future) time
  REFRESH AT '2024-06-06 12:00:00',
  -- Refresh at another user-specified (future) time
  REFRESH AT '2024-06-08 22:00:00'
)
AS SELECT ... FROM ...;
```

You can specify multiple `REFRESH AT` strategies in the same `CREATE` statement,
and combine them with the [`REFRESH EVERY` strategy](#refresh-every).

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

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 10PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-06 22:00:00'
) AS
SELECT ...;
```

You can specify multiple `REFRESH EVERY` strategies in the same `CREATE`
statement, and combine them with the [`REFRESH AT` strategy](#refresh-at). When
this strategy, we recommend **always** using the [`REFRESH AT CREATION`](#refresh-at)
clause, so the materialized view is available for querying ahead of the first
user-specified refresh time.

#### Querying materialized views with refresh strategies

Materialized views configured with [`REFRESH EVERY` strategies](#refresh-every)
have a period of unavailability around the scheduled refresh times — during this
period, the view **will not return any results**. To avoid unavailability
during the refresh operation, you must host these views in
[**scheduled clusters**](/sql/create-cluster/#scheduling), which can be
configured to automatically [turn on ahead of the scheduled refresh time](/sql/create-cluster/#hydration-time-estimate).

**Example**

To create a scheduled cluster that turns on 1 hour ahead of any scheduled
refresh times:

```mzsql
CREATE CLUSTER my_scheduled_cluster (
  SIZE = '3200cc',
  SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '1 hour')
);
```

You can then create a materialized view in this cluster, configured to refresh
at creation, then once a day at 12PM UTC:

```mzsql
CREATE MATERIALIZED VIEW mv_refresh_every
IN CLUSTER my_scheduled_cluster
WITH (
  -- Refresh at creation, so the view is populated ahead of
  -- the first user-specified refresh time
  REFRESH AT CREATION,
  -- Refresh every day at 12PM UTC
  REFRESH EVERY '1 day' ALIGNED TO '2024-06-18 00:00:00'
) AS
SELECT ...;
```

Because the materialized view is hosted on a scheduled cluster that is
configured to **turn on ahead of any scheduled refreshes**, you can expect
`my_scheduled_cluster` to be provisioned at 11PM UTC — or, 1 hour ahead of the
scheduled refresh time for `mv_refresh_every`. This means that the cluster can
backfill the view with pre-existing data — a process known as [_hydration_](/transform-data/troubleshooting/#hydrating-upstream-objects)
— ahead of the refresh operation, which **reduces the total unavailability window
of the view** to just the duration of the refresh.

If the cluster is **not** configured to turn on ahead of scheduled refreshes
(i.e., using the `HYDRATION TIME ESTIMATE` option), the total unavailability
window of the view will be a combination of the hydration time for all objects
in the cluster (typically long) and the duration of the refresh for the
materialized view (typically short).

Depending on the actual time it takes to hydrate the view or set of views in the
cluster, you can later adjust the hydration time estimate value for the
cluster using [`ALTER CLUSTER`](../alter-cluster/#schedule):

```mzsql
ALTER CLUSTER my_scheduled_cluster
SET (SCHEDULE = ON REFRESH (HYDRATION TIME ESTIMATE = '30 minutes'));
```

#### Introspection

To check details about the (non-default) refresh strategies associated with any materialized
view in the system, you can query
the [`mz_internal.mz_materialized_view_refresh_strategies`](../system-catalog/mz_internal/#mz_materialized_view_refresh_strategies)
and [`mz_internal.mz_materialized_view_refreshes`](../system-catalog/mz_internal/#mz_materialized_view_refreshes)
system catalog tables:

```mzsql
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

```mzsql
CREATE MATERIALIZED VIEW mv
IN CLUSTER my_refresh_cluster
WITH (
  -- Refresh every Tuesday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-04 12:00:00',
  -- Refresh every Thursday at 12PM UTC
  REFRESH EVERY '7 days' ALIGNED TO '2024-06-06 12:00:00',
  -- Refresh on creation, so the view is populated ahead of
  -- the first user-specified refresh time
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

## Additional information

- Materialized views are not monotonic; that is, materialized views cannot be
  recognized as append-only.

## Related pages

- [`SHOW MATERIALIZED VIEWS`](../show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](../show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](../drop-materialized-view)
