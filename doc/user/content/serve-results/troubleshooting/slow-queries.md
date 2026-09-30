---
title: "Troubleshooting: Slow queries"
description: "How to find slow queries in Materialize, see which stage of the query lifecycle takes the most time, and fix it."
menu:
  main:
    parent: "transform-troubleshooting"
    name: "Slow queries"
    weight: 10
---

This guide helps you find out why a query takes longer than expected to return
results, and how to fix it.

## Common causes

- **Lagging dependencies**: A materialized view, index, or source that the query
  reads from is behind. Materialize waits for it to catch up before it returns
  a consistent result.
- **No usable index**: The query can't be answered from an existing index, so
  Materialize builds a temporary dataflow or reads from storage for every
  execution.
- **Busy cluster**: Other work on the cluster, such as maintaining indexes and
  materialized views or running other queries, is using the CPU.
- **Transactions**: All statements in a transaction run at the same timestamp,
  so a fast object can wait for a slower one.
- **Large results or client distance**: Transmitting a large result, or a long
  network path between the client and Materialize, adds latency after the query
  has executed.

## Diagnosing the issue

### Find slow queries

List the slowest queries of the last hour from the [statement
log](/serve-results/troubleshooting/#query-history):

```mzsql
SELECT
  left(sql, 60) AS sql,
  cluster_name,
  execution_strategy,
  finished_at - began_at AS duration
FROM mz_internal.mz_recent_activity_log
WHERE statement_type = 'select'
  AND finished_status = 'success'
  AND cluster_name NOT LIKE 'mz_%'
  AND began_at > now() - INTERVAL '1 hour'
ORDER BY duration DESC
LIMIT 10;
```

```nofmt
                             sql                              | cluster_name | execution_strategy |   duration
--------------------------------------------------------------+--------------+--------------------+--------------
 SELECT o.customer_id, t.total FROM orders o JOIN order_total | quickstart   | standard           | 00:00:00.081
 SELECT count(*) FROM orders                                  | quickstart   | standard           | 00:00:00.026
 SELECT * FROM order_counts WHERE customer_id = 5             | quickstart   | standard           | 00:00:00.015
 SELECT * FROM order_totals WHERE customer_id = 5             | quickstart   | fast-path          | 00:00:00.005
```

The `execution_strategy` column shows how Materialize executed the query:

| Strategy | Meaning |
|----------|---------|
| `fast-path` | The cluster read the result directly from an existing index, or from storage for [small `LIMIT` queries](#return-less-data), without building a dataflow. |
| `standard` | The cluster built a temporary dataflow to compute the result, then dropped it. |
| `constant` | Materialize computed the result without a cluster. |

The query only lists successful statements. Canceled and failed statements
have no `execution_strategy`. To include them, remove the `finished_status`
filter.

The **Query history** tab in the [Materialize
console](/developer-tools/console/) shows the same information, and lets you
filter and sort statements by duration.

### Break down where the time goes

[`mz_statement_lifecycle_history`](/sql/system-catalog/mz_internal/#mz_statement_lifecycle_history)
records when each query reaches each stage of its lifecycle. Use it to split a
query's duration into stages:

```mzsql
WITH events AS (
  SELECT
    statement_id,
    max(occurred_at) FILTER (WHERE event_type = 'execution-began') AS began,
    max(occurred_at) FILTER (WHERE event_type = 'optimization-finished') AS optimized,
    max(occurred_at) FILTER (WHERE event_type = 'storage-dependencies-finished') AS storage_ready,
    max(occurred_at) FILTER (WHERE event_type = 'compute-dependencies-finished') AS compute_ready,
    max(occurred_at) FILTER (WHERE event_type = 'execution-finished') AS finished
  FROM mz_internal.mz_statement_lifecycle_history
  GROUP BY statement_id
)
SELECT
  left(a.sql, 40) AS sql,
  a.execution_strategy,
  e.optimized - e.began AS optimization,
  greatest(e.storage_ready, e.compute_ready) - e.optimized AS dependency_wait,
  e.finished - greatest(e.storage_ready, e.compute_ready) AS execution,
  e.finished - e.began AS total
FROM mz_internal.mz_recent_activity_log AS a
JOIN events AS e ON e.statement_id = a.execution_id
WHERE a.statement_type = 'select'
  AND a.cluster_name NOT LIKE 'mz_%'
  AND a.began_at > now() - INTERVAL '1 hour'
ORDER BY total DESC
LIMIT 10;
```

```nofmt
                   sql                    | execution_strategy | optimization | dependency_wait |  execution   |    total
------------------------------------------+--------------------+--------------+-----------------+--------------+--------------
 SELECT o.customer_id, t.total FROM order | standard           | 00:00:00.006 | 00:00:00.001    | 00:00:00.074 | 00:00:00.081
 SELECT count(*) FROM orders              | standard           | 00:00:00.007 | 00:00:00        | 00:00:00.019 | 00:00:00.026
 SELECT * FROM order_counts WHERE custome | standard           | 00:00:00.003 | 00:00:00        | 00:00:00.012 | 00:00:00.015
 SELECT * FROM order_totals WHERE custome | fast-path          | 00:00:00.003 | 00:00:00.001    | 00:00:00.001 | 00:00:00.005
```

Use the stage that dominates to pick a resolution:

| Stage | What happens | Resolution |
|-------|--------------|------------|
| `optimization` | Materialize parses, plans, and optimizes the query, and picks a timestamp. With [real-time recency](/sql/set/#other-configuration-parameters) enabled, this includes waiting for the latest upstream offsets. | Simplify the query, or [index a view](#use-an-index) that performs the complex part. In self-managed deployments, see [Check `environmentd`](#check-environmentd). |
| `dependency_wait` | Materialize waits for the sources, tables, materialized views, and indexes the query reads from to catch up to the chosen timestamp. | [Fix lagging dependencies](#fix-lagging-dependencies). |
| `execution` | The cluster computes the result and returns it to Materialize. Sending the rows to the client is not included. | [Use an index](#use-an-index), [reduce cluster load](#reduce-cluster-load), or [return less data](#return-less-data). |

If `total` is small but your client reports a much higher latency, the time is
spent outside of Materialize. See [Reduce client-side
latency](#reduce-client-side-latency).

### Check for lagging dependencies

To see how far behind each of your objects is, query
[`mz_wallclock_global_lag`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag):

```mzsql
SELECT o.name, o.type, l.lag
FROM mz_internal.mz_wallclock_global_lag AS l
JOIN mz_catalog.mz_objects AS o ON o.id = l.object_id
WHERE o.id LIKE 'u%'
ORDER BY l.lag DESC
LIMIT 10;
```

A lag of a few seconds is expected. Lag that is much higher, or that keeps
growing, means the object can't keep up with its inputs. To see lag visually,
open the object's workflow graph in the console: click **Clusters**, select the
cluster, select the object under **Materialized Views** or **Indexes**, then
open the **Workflow** tab.

To find why an object is lagging, see [Freshness
troubleshooting](/transform-data/freshness-troubleshooting/) and [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/). For a lagging
source, see [Troubleshoot ingestion](/ingest-data/troubleshooting/).

### Check the query plan

Run [`EXPLAIN`](/sql/explain-plan/) on the query, on the cluster where the
query runs:

```mzsql
EXPLAIN SELECT * FROM order_totals WHERE customer_id = 5;
```

```nofmt
Explained Query (fast path):
  →Map/Filter/Project
    Project: #0, #1
    →Index Lookup on materialize.public.order_totals (using materialize.public.order_totals_idx)
      Lookup values: (5)

Used Indexes:
  - materialize.public.order_totals_idx (lookup)

Target cluster: quickstart
```

`Explained Query (fast path)` means that no dataflow is built: an `Index
Lookup` or `Indexed` operator reads from an index, and `ReadStorage` reads from
storage. A plan without `(fast path)` means Materialize builds a temporary
dataflow on every execution, even if the plan lists `Used Indexes`.

### Check cluster utilization

A cluster near 100% CPU delays every query that runs on it:

```mzsql
SELECT c.name AS cluster, r.name AS replica, u.process_id, u.cpu_percent, u.memory_percent
FROM mz_internal.mz_cluster_replica_utilization AS u
JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = u.replica_id
JOIN mz_catalog.mz_clusters AS c ON c.id = r.cluster_id
WHERE c.name = 'quickstart';
```

You can also see CPU and memory for each cluster under **Clusters** in the
console.

## Resolution

### Fix lagging dependencies

- To find and fix the cause of lag in a materialized view or index, see
  [Freshness troubleshooting](/transform-data/freshness-troubleshooting/) and
  [Dataflow troubleshooting](/transform-data/dataflow-troubleshooting/). For a
  lagging source, see [Troubleshoot ingestion](/ingest-data/troubleshooting/).
- Avoid chaining materialized views where you don't need to. Each materialized
  view in a chain adds a small amount of lag to the next one.
- If you don't need [strict serializable](/serve-results/isolation-level/)
  results, use the `serializable` isolation level. Materialize can then serve
  results at the latest timestamp that all inputs can already serve, instead
  of waiting for the lagging object. The whole result is then as stale as the
  most lagging input.
- If you run queries inside a transaction, see [Avoid
  transactions](#avoid-unnecessary-transactions).
- If the dependencies can't keep up with their inputs, [size up the
  cluster](/sql/alter-cluster/) that maintains them.

### Use an index

- If the objects the query reads from don't have an
  [index](/fundamentals/concepts/indexes/), create one on the key the query filters or joins
  on. See [Optimization](/transform-data/optimization/).
- Run the query on the same cluster as the index. Indexes are local to a
  cluster.
- Make the index key match how you query the data. For example, a filter on
  `customer_id` needs an index on `customer_id`.
- Move joins and aggregations out of the query and into a view, then index the
  view. The query becomes a lookup against an existing index.

### Reduce cluster load

- Move queries that build dataflows onto a separate cluster, so that they don't
  compete with indexes and materialized views for CPU. Indexes are local to a
  cluster, so a query that uses an index on the original cluster can become
  more expensive on the new one. See [Expensive
  queries](/serve-results/troubleshooting/expensive-queries/).
- [Size up the cluster](/sql/alter-cluster/).

### Return less data

- Filter results with [temporal
  filters](/transform-data/patterns/temporal-filters/). Materialize can skip
  over old data in storage that doesn't match the filter.
- Add a `LIMIT` clause to exploratory queries. A query that selects from a
  single source, table, or materialized view with no filters, no ordering, and
  a `LIMIT` plus `OFFSET` below 25 reads directly from storage. `EXPLAIN` shows
  `Explained Query (fast path)` for these queries.
- Select only the columns you need. A large `result_size` in
  `mz_recent_activity_log` adds time to transmit the result.

### Avoid unnecessary transactions

All statements in a [transaction](/sql/begin/) run at the same timestamp, and
that timestamp must be valid for every object the transaction may access. As a
result, a query against a fast object can wait for a slower object in the same
schema.

- Don't use transactions for single statements.
- Check whether your SQL library or ORM wraps every query in a transaction, and
  disable that behavior.

### Reduce client-side latency

Run clients in the same cloud region as your Materialize region. For example,
if your Materialize region is in AWS `us-east-1`, run your client in AWS
`us-east-1`. To reuse connections instead of opening a new one for each query,
see [Connection pooling](/serve-results/connection-pooling/).

## Self-managed deployments

In self-managed deployments, you also operate the components between the client
and the cluster: `balancerd`, which terminates TLS and proxies connections, and
`environmentd`, which plans every query and dispatches it to a cluster. Either
can add latency that doesn't show up in cluster metrics.

### Make sure statement logging is enabled

The steps above need the statement log. Operator Helm chart versions earlier
than v26.40.0 disable statement logging. To check:

```mzsql
SHOW statement_logging_max_sample_rate;
```

If the result is `0`, the statement log is empty. To enable statement logging
and understand its cost, see [Query
History](/self-managed-deployments/query-history/).

### Attribute latency to a component

Compare three measurements over the same time window. Each one covers a smaller
part of the query path, so the gap between two of them tells you where the
time goes.

| Measurement | Covers |
|-------------|--------|
| Latency reported by your client | The full round trip: client, network, `balancerd`, `environmentd`, and the cluster. |
| `finished_at - began_at` in `mz_recent_activity_log` | `environmentd` and the cluster. |
| [`mz_compute_peek_duration_seconds`](/observability/essential-metrics/) | From when `environmentd` sends the query to the cluster until the result arrives. This includes waiting for dependencies and, for `standard` queries, building the temporary dataflow. |

To compute the average statement log latency over the last minute:

```mzsql
SELECT
  count(*) AS statements,
  round(avg(extract(epoch FROM finished_at - began_at)) * 1000, 1) AS avg_ms,
  round(max(extract(epoch FROM finished_at - began_at)) * 1000, 1) AS max_ms
FROM mz_internal.mz_recent_activity_log
WHERE began_at > now() - INTERVAL '1 minute'
  AND statement_type = 'select'
  AND finished_status = 'success'
  AND cluster_name NOT LIKE 'mz_%';
```

Then compare:

- **Client latency is much higher than the statement log**: The time is spent
  in the client, the network, or `balancerd`. See [Check
  `balancerd`](#check-balancerd) and [Check the client](#check-the-client).
- **Statement log latency is much higher than peek duration**: The time is
  spent in `environmentd` before the query reaches the cluster, for example in
  optimization or timestamp selection. See [Check
  `environmentd`](#check-environmentd).
- **Peek duration is high**: Use the [lifecycle
  breakdown](#break-down-where-the-time-goes) to see whether the query waits for
  dependencies or for execution on the cluster, then follow the steps in
  [Resolution](#resolution).

`mz_compute_peek_duration_seconds` has an `instance_id` label that holds the
cluster ID. Filter on it to compare the same clusters as the statement log
query, which excludes system clusters.

To collect `mz_compute_peek_duration_seconds` and other Prometheus metrics, see
[Grafana](/observability/self-managed/grafana/).

### Check `environmentd`

Every query passes through `environmentd`. Watch the CPU of the `environmentd`
pod while queries are slow. If it is saturated, or throttled by a CPU limit,
while cluster CPU is low, give `environmentd` more CPU with
`environmentdResourceRequirements` in the Materialize custom resource. See
[Materialize CRD field
descriptions](/self-managed-deployments/materialize-crd-field-descriptions/).
Changing this field rolls out a new `environmentd`.

### Check `balancerd`

`balancerd` handles every byte of every connection.

- Don't set CPU limits on `balancerd` pods, and run at least two replicas.
- Its memory use grows with the number of connections. Check the pods' restart
  count: a `balancerd` pod that is killed for running out of memory causes
  connection errors and latency spikes.

{{< warning >}}
A CPU-throttled pod can look idle on CPU usage graphs, because usage never
exceeds the configured limit. To detect throttling, check the
`container_cpu_cfs_throttled_periods_total` metric from cAdvisor instead of CPU
usage.
{{< /warning >}}

### Check the client

- Don't set CPU limits on load-testing or client processes. A throttled client
  queues requests and reports the queueing time as query latency.
- Don't measure through `kubectl port-forward` or SSH tunnels. They cap
  throughput far below what the deployment can serve.
- Run the client in the same region and network as Materialize.
