<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)
 /  [Overview](/docs/self-managed/v25.2/transform-data/)

</div>

# Troubleshooting

Once data is flowing into Materialize and you start modeling it in SQL,
you might run into some snags or unexpected scenarios. This guide
collects common questions around data transformation to help you
troubleshoot your queries.

## Why is my query slow?

The most common reasons for query execution taking longer than expected
are:

- Processing lag in upstream dependencies, like materialized views and
  indexes
- Index design
- Query design

Each of these reasons requires a different approach for troubleshooting.
Follow the guidance below to first detect the source of slowness, and
then address it accordingly.

### Lagging materialized views or indexes

#### Detect

When a materialized view or index upstream of your query is behind on
processing, your query must wait for it to catch up before returning
results. This is how Materialize ensures consistent results for all
queries.

To check if any materialized views or indexes are lagging, use the
workflow graphs in the Materialize console.

1.  Go to the Materialize console.
2.  Click on the **“Clusters”** tab in the side navigation bar.
3.  Click on the cluster that contains your upstream materialized view
    or index.
4.  Go to the **“Materialized Views”** or **“Indexes”** section, and
    click on the object name to access its workflow graph.

If you find that one of the upstream materialized views or indexes is
lagging, this could be the cause of your query slowness.

#### Address

To troubleshoot and fix a lagging materialized view or index, follow the
steps in the [dataflow
troubleshooting](/docs/self-managed/v25.2/transform-data/dataflow-troubleshooting)
guide.

*Do you have multiple materialized views chained on top of each other?
Are you seeing small amounts of lag?*  
Tip: avoid intermediary materialized views where not necessary. Each
chained materialized view incurs a small amount of processing lag from
the previous one.

Other options to consider:

- If you’ve gone through the dataflow troubleshooting and do not want to
  make any changes to your query, consider [sizing up your
  cluster](/docs/self-managed/v25.2/sql/create-cluster/#size).
- You can also consider changing your [isolation
  level](/docs/self-managed/v25.2/get-started/isolation-level/),
  depending on the consistency guarantees that you need. With a lower
  isolation level, you may be able to query stale results out of lagging
  indexes and materialized views.
- You can also check whether you’re using a [transaction](#transactions)
  and follow the guidance there.

### Slow query execution

Query execution time largely depends on the amount of on-the-fly work
that needs to be done to compute the result. You can cut back on
execution time in a few ways:

#### Indexing and query optimization

Like in any other database, index design affects query performance. If
the dependencies of your query don’t have
[indexes](/docs/self-managed/v25.2/sql/create-index/) defined, you
should consider creating one (or many). Check out the [optimization
guide](/docs/self-managed/v25.2/transform-data/optimization) for
guidance on how to optimize query performance. For information on when
to use a materialized view versus an index, check out the [materialized
view reference
documentation](/docs/self-managed/v25.2/sql/create-materialized-view/#details)
.

If the dependencies of your query are indexed, you should confirm that
the query is actually using the index! This information is available in
the query plan, which you can view using the
[`EXPLAIN PLAN`](/docs/self-managed/v25.2/sql/explain-plan/) command. If
you run `EXPLAIN PLAN` for your query and see the index(es) under
`Used indexes`, this means that the index was correctly used. If that’s
not the case, consider:

- Are you running the query in the same cluster which contains the
  index? You must do so in order for the index to be used.
- Does the index’s indexed expression (key) match up with how you’re
  querying the data?

#### Result filtering

If you are just looking to validate data and don’t want to deal with
query optimization at this stage, you can improve the efficiency of
validation queries by reducing the amount of data that Materialize needs
to read. You can achieve this by adding `LIMIT` clauses or [temporal
filters](/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/)
to your queries.

**`LIMIT` clause**

Use the standard `LIMIT` clause to return at most the specified number
of rows. It’s important to note that this only applies to basic queries
against **a single** source, materialized view or table, with no
ordering, filters or offsets.

<div class="highlight">

``` chroma
SELECT <column list or *>
FROM <source, materialized view or table>
LIMIT <25 or less>;
```

</div>

To verify whether the query will return quickly, use
[`EXPLAIN PLAN`](/docs/self-managed/v25.2/sql/explain-plan/) to get the
execution plan for the query, and validate that it starts with
`Explained Query (fast path)`.

**Temporal filters**

Use temporal flters to filter results on a timestamp column that
correlates with the insertion or update time of each row. For example:

<div class="highlight">

``` chroma
WHERE mz_now() <= event_ts + INTERVAL '1hr'
```

</div>

Materialize is able to “push down” temporal filters all the way down to
its storage layer, skipping over old data that isn’t relevant to the
query. For more details on temporal filter pushdown, see the [reference
documentation](/docs/self-managed/v25.2/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Other things to consider

#### Transactions

Transactions are a database concept for bundling multiple query steps
into a single, all-or-nothing operation. You can read more about them in
the [transactions](/docs/self-managed/v25.2/sql/begin) section of our
docs.

In Materialize, `BEGIN` starts a transaction block. All statements in a
transaction block will be executed in a single transaction until an
explicit `COMMIT` or `ROLLBACK` is given. All statements in that
transaction happen at the same timestamp, and that timestamp must be
valid for all objects the transaction may access.

What this means for latency: Materialize may delay queries against
“slow” tables, materialized views, and indexes until they catch up to
faster ones in the same schema. We recommend you avoid using
transactions in contexts where you require low latency responses and are
not certain that all objects in a schema will be equally current.

What you can do:

- Avoid using transactions where you don’t need them. For example, if
  you’re only executing single statements at a time.
- Double check whether your SQL library or ORM is wrapping all queries
  in transactions on your behalf, and disable that setting, only using
  transactions explicitly when you want them.

#### Client-side latency

To minimize the roundtrip latency associated with making requests from
your client to Materialize, make your requests as physically close to
your Materialize region as possible. For example, if you use the AWS
`us-east-1` region for Materialize, your client server would ideally
also be running in AWS `us-east-1`.

#### Result size

Smaller results lead to less time spent transmitting data over the
network. You can calculate your result size as
`number of rows returned x byte size of each row`, where
`byte size of each row = sum(byte size of each column)`. If your result
size is large, this will be a factor in query latency.

#### Cluster CPU

Another thing to check is how busy the cluster you’re issuing queries on
is. A busy cluster means your query might be blocked by some other
processing going on, taking longer to return. As an example, if you
issue a lot of resource-intensive queries at once, that might spike the
CPU.

The measure of cluster busyness is CPU. You can monitor CPU usage in the
[Materialize console](/docs/self-managed/v25.2/console/) by clicking the
**“Clusters”** tab in the navigation bar, and clicking into the cluster.
You can also grab CPU usage from the system catalog using SQL:

<div class="highlight">

``` chroma
SELECT cru.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization cru
LEFT JOIN mz_catalog.mz_cluster_replicas cr ON cru.replica_id = cr.id
LEFT JOIN mz_catalog.mz_clusters c ON cr.cluster_id = c.id
WHERE c.name = <CLUSTER_NAME>;
```

</div>

## Why is my query not responding?

The most common reasons for query hanging are:

- An upstream source is stalled
- An upstream object is still hydrating
- Your cluster is unhealthy

Each of these reasons requires a different approach for troubleshooting.
Follow the guidance below to first detect the source of the hang, and
then address it accordingly.

<div class="note">

**NOTE:** Your query may be running, just slowly. If none of the reasons
below detects your issue, jump to [Why is my query
slow?](#why-is-my-query-slow) for further guidance.

</div>

### Stalled source

To detect and address stalled sources, follow the [`Ingest data`
troubleshooting](/docs/self-managed/v25.2/ingest-data/troubleshooting)
guide.

### Hydrating upstream objects

When a source, materialized view, or index is created or updated, it
must first be backfilled with any pre-existing data — a process known as
*hydration*.

Queries that depend objects that are still hydrating will **block until
hydration is complete**. To see whether an object is still hydrating,
navigate to the [workflow graph](#detect) for the object in the
Materialize console.

Hydration time is proportional to data volume and query complexity. This
means that you should expect objects with large volumes of data and/or
complex queries to take longer to hydrate. You should also expect
hydration to be triggered every time a cluster is restarted or sized up.

### Unhealthy cluster

#### Detect

If your cluster runs out of memory (i.e., it OOMs), this will result in
a crash. After a crash, the cluster has to restart, which can take a few
seconds. On cluster restart, your query will also automatically restart
execution from the beginning.

If your cluster is CPU-maxed out (~100% utilization), your query may be
blocked while the cluster processes the other activity. It may
eventually complete, but it will continue to be slow and potentially
blocked until the CPU usage goes down. As an example, if you issue a lot
of resource-intensive queries at once, that might spike the CPU.

To see memory and CPU usage for your cluster in the Materialize console,
go to Console, click the **“Clusters”** tab in the navigation bar, and
click on the cluster name.

#### Address

Your query may have been the root cause of the increased memory and CPU
usage, or it may have been something else happening on the cluster at
the same time. To troubleshoot and fix memory and CPU usage, follow the
steps in the [dataflow
troubleshooting](/docs/self-managed/v25.2/transform-data/dataflow-troubleshooting)
guide.

For guidance on how to reduce memory and CPU usage for this or another
query, take a look at the [indexing and query
optimization](#indexing-and-query-optimization) and [result
filtering](#result-filtering) sections above.

If your query was the root cause, you’ll need to kill it for the
cluster’s memory or CPU to go down. If your query was causing an OOM,
the cluster will continue to be in an “OOM loop” - every time the
cluster restarts, the query restarts executing automatically then causes
an OOM again - until you kill the query.

If your query was not the root cause, you can wait for the other
activity on the cluster to stop and memory/CPU to go down, or switch to
a different cluster.

If you’ve gone through the dataflow troubleshooting and do not want to
make any changes to your query, consider [sizing up your
cluster](/docs/self-managed/v25.2/sql/create-cluster/#size). A larger
size cluster will provision more memory and CPU resources.

## Which part of my query runs slowly or uses a lot of memory?

You can [`EXPLAIN`](/docs/self-managed/v25.2/sql/explain-plan/) a query
to see how it will be run as a dataflow. In particular,
`EXPLAIN PHYSICAL PLAN` (the default) will show the concrete, fully
optimized plan that Materialize will run. That plan is written in our
“low-level intermediate representation” (LIR).

You can
[`EXPLAIN ANALYZE`](/docs/self-managed/v25.2/sql/explain-analyze) an
index or materialized view to attribute performance information to each
LIR operator.

## How do I troubleshoot slow queries?

Materialize stores a (sampled) log of the SQL statements that are issued
against your Materialize region in the last **three days**, along with
various metadata about these statements. You can access this log via the
**“Query history”** tab in the [Materialize
console](/docs/self-managed/v25.2/console/). You can filter and sort
statements by type, duration, and other dimensions.

This data is also available via the
[mz_internal.mz_recent_activity_log](/docs/self-managed/v25.2/sql/system-catalog/mz_internal/#mz_recent_activity_log)
catalog table.

It’s important to note that the default (and max) sample rate for most
Materialize organizations is 99%, which means that not all statements
will be captured in the log. The sampling rate is not user-configurable,
and may change at any time.

If you’re looking for a complete audit history, use the
[mz_audit_events](/docs/self-managed/v25.2/sql/system-catalog/mz_catalog/#mz_audit_events)
catalog table, which records all DDL commands issued against your
Materialize region.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/transform-data/troubleshooting.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
