---
title: "Troubleshooting"
description: "How to troubleshoot common data transformation scenarios where Materialize is not working as expected."
menu:
  main:
    name: "Troubleshooting"
    identifier: transform-troubleshooting
    parent: transform-data
    weight: 90
---

Once data is flowing into Materialize and you start modeling it in SQL, you
might run into some snags or unexpected scenarios. This guide collects common
questions around data transformation to help you troubleshoot your queries.

## Why is my query slow?

<!-- TODO: update this to use the query history UI once it's available -->
The most common reasons for query execution taking longer than expected are:

* Processing lag in upstream dependencies, like materialized views and indexes
* Index design
* Query design

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of slowness, and then address it
accordingly.

### Lagging materialized views or indexes

#### Detect

When a materialized view or index upstream of your query is behind on
processing, your query must wait for it to catch up before returning results.
This is how Materialize ensures consistent results for all queries.

To check if any materialized views or indexes are lagging, use the workflow
graphs in the Materialize console.

1. Go to https://console.materialize.com/.
2. Click on the **"Clusters"** tab in the side navigation bar.
3. Click on the cluster that contains your upstream materialized view or index.
4. Go to the **"Materialized Views"** or **"Indexes"** section, and click on the
object name to access its workflow graph.

If you find that one of the upstream materialized views or indexes is lagging,
this could be the cause of your query slowness.

#### Address

To troubleshoot and fix a lagging materialized view or index, follow the steps
in the [dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

*Do you have multiple materialized views chained on top of each other? Are you
seeing small amounts of lag?*<br>
Tip: avoid intermediary materialized views where not necessary. Each chained
materialized view incurs a small amount of processing lag from the previous
one.
<!-- TODO add more guidance on avoiding chained mat views-->

Other options to consider:

* If you've gone through the dataflow troubleshooting and do not want to make
  any changes to your query, consider [sizing up your cluster](/sql/create-cluster/#size).
* You can also consider changing your [isolation level](/get-started/isolation-level/),
  depending on the consistency guarantees that you need. With a lower isolation
  level, you may be able to query stale results out of lagging indexes and
  materialized views.
* You can also check whether you're using a [transaction](#transactions) and
  follow the guidance there.

### Slow query execution

Query execution time largely depends on the amount of on-the-fly work that needs
to be done to compute the result. You can cut back on execution time in a few
ways:

#### Indexing and query optimization

Like in any other database, index design affects query performance. If the
dependencies of your query don't have [indexes](/sql/create-index/) defined,
you should consider creating one (or many). Check out the [optimization guide](/transform-data/optimization)
for guidance on how to optimize query performance. For information on when
to use a materialized view versus an index, check out the
[materialized view reference documentation](/sql/create-materialized-view/#details) .

If the dependencies of your query are indexed, you should confirm that the query
is actually using the index! This information is available in the query plan,
which you can view using the [`EXPLAIN PLAN`](/sql/explain-plan/) command. If
you run `EXPLAIN PLAN` for your query and see the index(es) under `Used indexes`,
this means that the index was correctly used. If that's not the case, consider:

* Are you running the query in the same cluster which contains the index? You
  must do so in order for the index to be used.
* Does the index's indexed expression (key) match up with how you're querying
  the data?

#### Result filtering

If you are just looking to validate data and don't want to deal with query
optimization at this stage, you can improve the efficiency of validation
queries by reducing the amount of data that Materialize needs to read. You can
achieve this by adding `LIMIT` clauses or [temporal filters](https://materialize.com/docs/transform-data/patterns/temporal-filters/)
to your queries.

**`LIMIT` clause**

Use the standard `LIMIT` clause to return at most the specified number of rows.
It's important to note that this only applies to basic queries against **a
single** source, materialized view or table, with no ordering, filters or
offsets.

```mzsql
SELECT <column list or *>
FROM <source, materialized view or table>
LIMIT <25 or less>;
```

To verify whether the query will return quickly, use [`EXPLAIN PLAN`](https://materialize.com/docs/sql/explain-plan/)
to get the execution plan for the query, and validate that it starts with
`Explained Query (fast path)`.

**Temporal filters**

Use temporal flters to filter results on a timestamp column that correlates with
the insertion or update time of each row. For example:

```mzsql
WHERE mz_now() <= event_ts + INTERVAL '1hr'
```

Materialize is able to “push down” temporal filters all the way down to its
storage layer, skipping over old data that isn't relevant to the query. For
more details on temporal filter pushdown, see the [reference documentation](/transform-data/patterns/temporal-filters/#temporal-filter-pushdown).

### Other things to consider

#### Transactions
<!-- Copied from doc/user/content/manage/troubleshooting.md#Transactions -->
Transactions are a database concept for bundling multiple query steps into a
single, all-or-nothing operation. You can read more about them in the
[transactions](https://materialize.com/docs/sql/begin) section of our docs.

In Materialize, `BEGIN` starts a transaction block. All statements in a
transaction block will be executed in a single transaction until an explicit
`COMMIT` or `ROLLBACK` is given. All statements in that transaction happen at
the same timestamp, and that timestamp must be valid for all objects the
transaction may access.

What this means for latency: Materialize may delay queries against "slow"
tables, materialized views, and indexes until they catch up to faster ones in
the same schema. We recommend you avoid using transactions in contexts where
you require low latency responses and are not certain that all objects in a
schema will be equally current.

What you can do:

- Avoid using transactions where you don’t need them. For example, if you’re
  only executing single statements at a time.
- Double check whether your SQL library or ORM is wrapping all queries in
  transactions on your behalf, and disable that setting, only using
  transactions explicitly when you want them.

#### Client-side latency
<!-- Copied from doc/user/content/manage/troubleshooting.md#client-side-latency -->
To minimize the roundtrip latency associated with making requests from your
client to Materialize, make your requests as physically close to your
Materialize region as possible. For example, if you use the AWS `us-east-1`
region for Materialize, your client server would ideally also be running in AWS
`us-east-1`.

#### Result size
<!-- TODO: Use the query history UI to fetch result size -->
Smaller results lead to less time spent transmitting data over the network. You
can calculate your result size as `number of rows returned x byte size of each
row`, where `byte size of each row = sum(byte size of each column)`. If your
result size is large, this will be a factor in query latency.

#### Cluster CPU
Another thing to check is how busy the cluster you're issuing queries on is. A
busy cluster means your query might be blocked by some other processing going
on, taking longer to return. As an example, if you issue a lot of
resource-intensive queries at once, that might spike the CPU.

The measure of cluster busyness is CPU. You can monitor CPU usage in the
[Materialize console](https://console.materialize.com/) by clicking
the **"Clusters"** tab in the navigation bar, and clicking into the cluster.
You can also grab CPU usage from the system catalog using SQL:

```mzsql
SELECT cru.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization cru
LEFT JOIN mz_catalog.mz_cluster_replicas cr ON cru.replica_id = cr.id
LEFT JOIN mz_catalog.mz_clusters c ON cr.cluster_id = c.id
WHERE c.name = <CLUSTER_NAME>;
```

## Why is my query not responding?

The most common reasons for query hanging are:

* An upstream source is stalled
* An upstream object is still hydrating
* Your cluster is unhealthy

Each of these reasons requires a different approach for troubleshooting. Follow
the guidance below to first detect the source of the hang, and then address it
accordingly.

{{< note >}}
Your query may be running, just slowly. If none of the reasons below detects
your issue, jump to [Why is my query slow?](#why-is-my-query-slow) for further
guidance.
{{< /note >}}

### Stalled source

<!-- TODO: update this to use the query history UI once it's available -->
To detect and address stalled sources, follow the [`Ingest data` troubleshooting](/ingest-data/troubleshooting)
guide.

### Hydrating upstream objects

When a source, materialized view, or index is created or updated, it must first
be backfilled with any pre-existing data — a process known as _hydration_.

Queries that depend objects that are still hydrating will **block until
hydration is complete**. To see whether an object is still hydrating, navigate
to the [workflow graph](#detect) for the object in the Materialize console.

Hydration time is proportional to data volume and query complexity. This means
that you should expect objects with large volumes of data and/or complex
queries to take longer to hydrate. You should also expect hydration to be
triggered every time a cluster is restarted or sized up, including during
[Materialize's routine maintenance window](/releases#schedule).

### Unhealthy cluster

#### Detect

If your cluster runs out of memory (i.e., it OOMs), this will result in a crash.
After a crash, the cluster has to restart, which can take a few seconds. On
cluster restart, your query will also automatically restart execution from the
beginning.

If your cluster is CPU-maxed out (\~100% utilization), your query may be blocked
while the cluster processes the other activity. It may eventually complete, but
it will continue to be slow and potentially blocked until the CPU usage goes
down. As an example, if you issue a lot of resource-intensive queries at once,
that might spike the CPU.

To see memory and CPU usage for your cluster in the Materialize console, go to
https://console.materialize.com/, click the **“Clusters”** tab in the
navigation bar, and click on the cluster name.

#### Address

Your query may have been the root cause of the increased memory and CPU usage,
or it may have been something else happening on the cluster at the same time.
To troubleshoot and fix memory and CPU usage, follow the steps in the
[dataflow troubleshooting](/transform-data/dataflow-troubleshooting) guide.

For guidance on how to reduce memory and CPU usage for this or another query,
take a look at the [indexing and query optimization](#indexing-and-query-optimization)
and [result filtering](#result-filtering) sections above.

If your query was the root cause, you'll need to kill it for the cluster's
memory or CPU to go down. If your query was causing an OOM, the cluster will
continue to be in an "OOM loop" - every time the cluster restarts, the query
restarts executing automatically then causes an OOM again - until you kill the
query.

If your query was not the root cause, you can wait for the other activity on the
cluster to stop and memory/CPU to go down, or switch to a different cluster.

If you've gone through the dataflow troubleshooting and do not want to make any
changes to your query, consider [sizing up your cluster](/sql/create-cluster/#size).
A larger size cluster will provision more memory and CPU resources.

## Which part of my query runs slowly or uses a lot of memory?

{{< public-preview />}}

You can [`EXPLAIN`](/sql/explain-plan/) a query to see how it will be run as a
dataflow. In particular, `EXPLAIN PHYSICAL PLAN` will show the concrete, fully
optimized plan that Materialize will run. That plan is written in our "low-level
intermediate representation" (LIR).

For [indexes](/concepts/indexes) and [materialized
views](/concepts/views#materialized-views), you can use
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping)
to attribute various performance characteristics to the operators inside your
query.

Every time you create an index or materialized view, Materialize uses
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping)
to map the higher-level LIR operators to zero or more lower-level
dataflow operators. You can construct queries that will combine
information from
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping)
and other internal views to discover which parts of your query are
computationally expensive (e.g.,
[`mz_introspection.mz_compute_operator_durations_histogram`](/sql/system-catalog/mz_introspection/#mz_compute_operator_durations_histogram), [`mz_introspection.mz_scheduling_elapsed`](/sql/system-catalog/mz_introspection/#mz_scheduling_elapsed))
or consuming excessive memory (e.g., [`mz_introspection.mz_arrangement_sizes`](/sql/system-catalog/mz_introspection/#mz_arrangement_sizes)).

To show how you can use
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping)
to attribute performance characteristics, the attribution examples in this
section reference the `wins_by_item` index (and the underlying `winning_bids`
view) from the [quickstart
guide](/get-started/quickstart/#step-2-create-the-source):

```sql
CREATE SOURCE auction_house
FROM LOAD GENERATOR AUCTION
(TICK INTERVAL '1s', AS OF 100000)
FOR ALL TABLES;

CREATE VIEW winning_bids AS
  SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
    FROM auctions AS a
    JOIN bids AS b
      ON a.id = b.auction_id
   WHERE b.bid_time < a.end_time
     AND mz_now() >= a.end_time
   ORDER BY a.id, b.amount DESC, b.bid_time, b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

We attribute four different kinds of performance data to parts of the
`wins_by_item` query:

- [computation time](#attributing-computation-time)
- [memory usage](#attributing-memory-usage)
- [Top-k query sizing hints](#attributing-topk-hints)
- [worker skew](#localizing-worker-skew)

### Attributing computation time

When optimizing a query, it helps to be able to attribute 'cost' to its parts,
starting with how much time is spent computing in each part overall. Materialize
reports the time spent in each _dataflow operator_ in
[`mz_introspection.mz_compute_operator_durations_histogram`](/sql/system-catalog/mz_introspection/#mz_compute_operator_durations_histogram).
By joining it with
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping),
we can attribute the time spent in each operator to the higher-level, more
intelligible LIR operators.

For example, to find out how much time is spent in each operator for the `wins_by_item` index (and the underlying `winning_bids` view), run the following query:

```sql
SELECT mo.name AS name, global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator,
       SUM(duration_ns)/1000 * '1 microsecond'::INTERVAL AS duration, SUM(count) AS count
    FROM           mz_introspection.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_compute_operator_durations_histogram mcodh
                ON (mlm.operator_id_start <= mcodh.id AND mcodh.id < mlm.operator_id_end)
              JOIN mz_catalog.mz_objects mo
                ON (mlm.global_id = mo.id)
   WHERE mo.name IN ('wins_by_item', 'winning_bids')
GROUP BY mo.name, global_id, lir_id, operator, parent_lir_id, nesting
ORDER BY global_id, lir_id DESC;
```

The query produces results similar to the following (your specific numbers will
vary):

{{< yaml-table data="query_attribution_computation_time_output" >}}

- The `duration` column shows that the `TopK` operator is where we spend the
  bulk of the query's computation time.

- Creating an index on a view starts _two_ dataflows as denoted by the two
  `global_ids`:
  - `u148` is the dataflow for the `winning_bids` view (installed when the index
    is created), and
  - `u149` is the dataflow for the `wins_by_item` index on `winning_bids` (which
    arranges the results of the `winning_bids` view by the index key).

The LIR operators reported in `mz_lir_mapping.operator` are terser than those in
`EXPLAIN PHYSICAL PLAN`. Each operator is restricted to a single line of the
form `OPERATOR [INPUT_LIR_ID ...]`. For example `lir_id` 4 is the operator
`Arrange 3`, where `Arrange` is the operator that will arrange in memory the
results of `lir_id` 3 (`GET::PassArrangements u145`), which `GET`s/reads
dataflow `u145` (while not shown, `u145` is the `bids` table from the Auction
source).

To examine the query in more detail:

- The query works by finding every dataflow operator in the range
  (`mz_lir_mapping.operator_id_start` inclusive to
`mz_lir_mapping.operator_id_end` exclusive) and summing up the time spent
(`SUM(duration_ns)`).

- The query joins with
  [`mz_catalog.mz_objects`](/sql/system-catalog/mz_catalog/#mz_objects) to find
  the actual name corresponding to the `global_id`.  The `WHERE mo.name IN ...`
  clause of the query ensures we only see information about this index and view.
  If you leave this `WHERE` clause out, you will see information on _every_
  view, materialized view, and index on your current cluster.

- The `operator` is indented using [`REPEAT`](/sql/functions/#repeat) and
  `mz_lir_mapping.nesting`. The indenting, combined with ordering by
  `mz_lir_mapping.lir_id` descending, gives us a tree-shaped view of the LIR
  plan.

### Attributing memory usage

{{< tip >}}

If you have not read about [attributing computation time](#attributing-computation-time), please do so first, as it explains some core concepts.

{{< /tip >}}

To find the memory usage of each operator for the index and view, join
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping)
with
[`mz_introspection.mz_arrangement_sizes`](/sql/system-catalog/mz_introspection/#mz_arrangement_sizes):

```sql
  SELECT mo.name AS name, global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator,
         pg_size_pretty(SUM(size)) AS size
    FROM           mz_introspection.mz_lir_mapping mlm
         LEFT JOIN mz_introspection.mz_arrangement_sizes mas
                ON (mlm.operator_id_start <= mas.operator_id AND mas.operator_id < mlm.operator_id_end)
              JOIN mz_catalog.mz_objects mo
                ON (mlm.global_id = mo.id)
   WHERE mo.name IN ('wins_by_item', 'winning_bids')
GROUP BY mo.name, global_id, lir_id, operator, parent_lir_id, nesting
ORDER BY global_id, lir_id DESC;
```

The query produces results similar to the following (your specific numbers will
vary):

{{< yaml-table data="query_attribution_memory_usage_output" >}}

The results show:

- The [`TopK`](/transform-data/idiomatic-materialize-sql/top-k/) is
  overwhelmingly responsible for memory usage.

- Arranging the `bids` table (`lir_id` 4) and `auctions` table (`lir_id` 2) are
  cheap in comparison as is arranging the final output in the `wins_by_item`
  index (`lir_id` 8).

### Attributing `TopK` hints

{{< tip >}}

If you have not read about [attributing computation time](#attributing-computation-time), please do so first, as it explains some core concepts.

{{< /tip >}}

The
[`mz_introspection.mz_expected_group_size_advice`](/sql/system-catalog/mz_introspection/#mz_expected_group_size_advice)
looks at your running dataflow and suggests parameters you can set. We
can attribute this to particular parts of our query using
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping):

```sql
  SELECT mo.name AS name, mlm.global_id AS global_id, lir_id, parent_lir_id, REPEAT(' ', nesting * 2) || operator AS operator,
         levels, to_cut, hint, pg_size_pretty(savings) AS savings
    FROM           mz_introspection.mz_lir_mapping mlm
              JOIN mz_introspection.mz_dataflow_global_ids mdgi
                ON (mlm.global_id = mdgi.global_id)
         LEFT JOIN mz_introspection.mz_expected_group_size_advice megsa
                ON (megsa.dataflow_id = mdgi.id AND
                    mlm.operator_id_start <= megsa.region_id AND megsa.region_id < mlm.operator_id_end)
              JOIN mz_catalog.mz_objects mo
                ON (mlm.global_id = mo.id)
   WHERE mo.name IN ('wins_by_item', 'winning_bids')
ORDER BY mlm.global_id, lir_id DESC;
```

Each `TopK` operator will have an [associated `DISTINCT ON INPUT GROUP SIZE`
query hint](/transform-data/idiomatic-materialize-sql/top-k/#query-hints-1):

{{< yaml-table data="query_attribution_topk_hints_output" >}}

Here, the hinted `DISTINCT ON INPUT GROUP SIZE` is `255.0`. We can re-create our view and index using the hint as follows:

```sql
DROP VIEW winning_bids CASCADE;

CREATE VIEW winning_bids AS
    SELECT DISTINCT ON (a.id) b.*, a.item, a.seller
      FROM auctions AS a
      JOIN bids AS b
        ON a.id = b.auction_id
     WHERE b.bid_time < a.end_time
       AND mz_now() >= a.end_time
   OPTIONS (DISTINCT ON INPUT GROUP SIZE = 255) -- use hint!
  ORDER BY a.id,
    b.amount DESC,
    b.bid_time,
    b.buyer;

CREATE INDEX wins_by_item ON winning_bids (item);
```

Re-running the `TopK`-hints query will show only `null` hints; there are no
hints because our `TopK` is now appropriately sized. But if we re-run our [query
for attributing memory usage](#attributing-memory-usage), we can see that our
`TopK` operator uses a third of the memory it was using before:

{{< yaml-table data="query_attribution_memory_usage_w_hint_output" >}}

### Localizing worker skew

{{< tip >}}

If you have not read about [attributing computation time](#attributing-computation-time), please do so first, as it explains some core concepts.

{{< /tip >}}


[Worker skew](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers) occurs when your data do not end up getting evenly
partitioned between workers.  Worker skew can only happen when your
cluster has more than one worker. You can query
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
to determine how many workers a given cluster size has; in our example, there are 4 workers.

You can identify worker skew by comparing a worker's time spent to the
overall time spent across all workers:

```sql
 SELECT mo.name AS name, global_id, lir_id, REPEAT(' ', 2 * nesting) || operator AS operator,
        worker_id,
        ROUND(SUM(elapsed_ns) / SUM(aebi.total_ns), 2) AS ratio,
        SUM(epw.elapsed_ns) / 1000 * '1 microsecond'::INTERVAL AS elapsed_ns,
        SUM(aebi.total_ns) / 1000 * '1 microsecond'::INTERVAL AS avg_ns
   FROM                    mz_introspection.mz_lir_mapping mlm
        CROSS JOIN LATERAL (  SELECT SUM(elapsed_ns) AS total_ns
                                FROM mz_introspection.mz_scheduling_elapsed_per_worker mse
                               WHERE mlm.operator_id_start <= id AND id < mlm.operator_id_end
                            GROUP BY worker_id) aebi
        CROSS JOIN LATERAL (  SELECT worker_id, SUM(elapsed_ns) AS elapsed_ns
                                FROM mz_introspection.mz_scheduling_elapsed_per_worker mse
                               WHERE mlm.operator_id_start <= id AND id < mlm.operator_id_end
                            GROUP BY worker_id) epw
                      JOIN mz_catalog.mz_objects mo
                        ON (mlm.global_id = mo.id)
   WHERE mo.name IN ('wins_by_item', 'winning_bids')
GROUP BY mo.name, global_id, lir_id, nesting, operator, worker_id
ORDER BY global_id, lir_id DESC;
```

{{< yaml-table data="query_attribution_worker_skew_output" >}}

The `ratio` column tells you whether a worker is particularly over- or
under-loaded:

- a `ratio` below 1 indicates a worker doing a below average amount of work.

- a `ratio` above 1 indicates a worker doing an above average amount of work.

While there will always be some amount of variation, very high ratios indicate a
skewed workload.

### Writing your own attribution queries

Materialize maps LIR nodes to ranges of dataflow operators in
[`mz_introspection.mz_lir_mapping`](/sql/system-catalog/mz_introspection/#mz_lir_mapping).
By combining information from
[`mz_catalog`](/sql/system-catalog/mz_catalog/) and
[`mz_introspection`](/sql/system-catalog/mz_introspection/),
you can better understand your dataflows' behavior. Using the above queries as a
starting point, you can build your own attribution queries. When building your own, keep the following in mind:

- `mz_lir_mapping.operator` is not stable and **should not be parsed**.

  - If you want to traverse the LIR tree, use `mz_lir_mapping. parent_lir_id`.

  - To request additional metadata that would be useful for us to provide,
    please [contact our team](https://materialize.com/contact/).

- Include `GROUP BY global_id` to avoid mixing `lir_ids` from different
  `global_id`s. Mixing `lir_id`s from different `global_id`s will produce
  nonsense. `global_id`s will produce nonsense.

- Use `REPEAT(' ', 2 * nesting) || operator` and `ORDER BY lir_id DESC` to
  correctly render the LIR tree.

- `mz_lir_mapping.operator_id_start` is inclusive;
  `mz_lir_mapping.operator_id_end` is exclusive. If they are equal to each
  other, that LIR operator does not correspond to any dataflow operators.

- To only see output for views, materialized views, and indexes you're
  interested in, join with `mz_catalog.mz_objects` and restrict based on
  `mz_objects.name`. Otherwise, you will see information on everything installed
  in your current cluster.

## How do I troubleshoot slow queries?

Materialize stores a (sampled) log of the SQL statements that are issued against
your Materialize region in the last **three days**, along with various metadata
about these statements. You can access this log via the **"Query history"** tab
in the [Materialize console](https://console.materialize.com/). You can filter
and sort statements by type, duration, and other dimensions.

This data is also available via the
[mz_internal.mz_recent_activity_log](/sql/system-catalog/mz_internal/#mz_recent_activity_log)
catalog table.

It's important to note that the default (and max) sample rate for most
Materialize organizations is 99%, which means that not all statements will be
captured in the log. The sampling rate is not user-configurable, and may change
at any time.

If you're looking for a complete audit history, use the [mz_audit_events](/sql/system-catalog/mz_catalog/#mz_audit_events)
catalog table, which records all DDL commands issued against your Materialize
region.
