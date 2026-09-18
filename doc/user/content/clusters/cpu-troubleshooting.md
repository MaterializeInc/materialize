---
title: "Troubleshoot anomalies in CPU usage"
description: "How to diagnose and resolve anomalies in cluster CPU utilization."
menu:
  main:
    name: "Troubleshoot anomalies in CPU usage"
    identifier: cpu-troubleshooting
    parent: clusters
    weight: 90
---

A cluster replica's CPU goes to the dataflows that maintain the cluster's
indexes, materialized views, and sinks, and to the ad-hoc queries served from
it. An anomaly in CPU usage means one of those started doing more work, or that
the same work stopped being spread evenly across the replica's workers.

Each section below covers one cause, with the queries that diagnose it, how to
resolve it, and how to prevent it from recurring.

{{< important >}}
The introspection relations and `EXPLAIN ANALYZE` statements in this guide
report on **the cluster and replica your session is connected to**. Run
`SET CLUSTER TO <cluster_name>` first.

On a cluster with a replication factor above 1, that is not enough: each replica
collects this data about itself, so the query fails until you also pick one with
`SET cluster_replica = <replica_name>`. Re-run per replica, then clear the
targeting with `RESET cluster_replica`, which otherwise applies to every
subsequent query in the session.
{{< /important >}}

## Common causes

| Cause | Description |
| ----- | ----------- |
| [Worker skew](#worker-skew) | One worker does far more work than its peers, so the replica saturates a core while the rest idle. |
| [An expensive object](#an-expensive-object) | A single dataflow dominates the cluster's CPU. |
| [Hydration](#hydration) | A replica restart, cluster resize, or DDL forced objects to rebuild from their inputs. |
| [Ad-hoc query load](#ad-hoc-query-load) | `SELECT`s served by the cluster compete with its maintenance work. |
| [Upstream data volume](#upstream-data-volume) | More data is arriving from sources, so there is more incremental work to do. |
| [Memory pressure causing increased spill to disk](#memory-pressure-causing-increased-spill-to-disk) | A replica near its memory limit burns CPU on I/O rather than on your dataflows. |
| [An undersized cluster](#an-undersized-cluster) | The cluster has no headroom left for the workload it carries. |

## First, confirm the spike window and scope

Establish when the anomaly started and which replicas it affected, replacing
`<cluster_name>` with the name of your cluster:

```mzsql
SELECT
    u.occurred_at,
    r.name AS replica_name,
    u.cpu_percent
FROM mz_internal.mz_cluster_replica_utilization_history u
JOIN mz_catalog.mz_cluster_replicas r ON u.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
WHERE c.name = '<cluster_name>'
  AND u.occurred_at > now() - INTERVAL '6 hours'
ORDER BY u.occurred_at DESC;
```

```nofmt
        occurred_at         | replica_name | cpu_percent
----------------------------+--------------+--------------
 2026-09-15 16:26:44.899+00 | r1           | 39.634147625
 2026-09-15 16:25:44.9+00   | r1           | 34.412956225
 2026-09-15 16:24:44.899+00 | r1           |  34.81781385
 2026-09-15 16:23:44.904+00 | r1           |            0
 2026-09-15 16:22:46.4+00   | r1           |
(5 rows)
```

Samples land about once a minute, and each one is a difference against the
previous sample, so the oldest row of a replica's history has a `NULL`
`cpu_percent`.

Every replica of a cluster maintains the same objects, so:

- If **all replicas** spike together, the workload itself changed. Continue with
  the checks below.

- If **one replica** spikes, that replica is doing something its peers are not,
  most often rehydrating after a restart. See [Hydration](#hydration).

- If the anomaly **repeats on a fixed cadence**, or CPU stays high while the
  cluster's inputs are idle, suspect a [temporal
  filter](/transform-data/optimization/#improve-performance-when-using-temporal-filters).
  A window whose boundary moves with `mz_now()` retracts rows as they age out,
  so it generates work with no upstream input at all.

{{< important >}}
`cpu_percent` is a percentage of the replica's *total* allocation across all of
its cores, so it averages over workers. A replica whose work is concentrated on
one worker can peg a core while reporting a modest `cpu_percent`.

**Low CPU is therefore not a clean bill of health.** Degraded freshness with
unremarkable CPU is the signature of [worker skew](#worker-skew): the busy
worker is saturated, and averaging it against idle peers hides that.
{{< /important >}}

Which way the number points tells you which check to run first:

- **CPU is high**: the cluster is doing more work than it can absorb. Start
  with [an expensive object](#an-expensive-object).

- **CPU is low but freshness is degraded**: the work is not being spread across
  workers. Start with [worker skew](#worker-skew).

## Worker skew

Materialize distributes work across a replica's workers by hashing keys, such
as join keys and `GROUP BY` keys. If one key value accounts for a
disproportionate share of rows, the worker responsible for that value does
disproportionate work while its peers idle.

A cross join is the extreme case. It has no join key to hash on, so every cross
join running on a replica moves all of its data to a single worker, no matter
how the input values are distributed.

### Diagnosing the issue

**Step 1. Find the skewed object.** Connect to the cluster and run
[`EXPLAIN ANALYZE CLUSTER CPU WITH
SKEW`](/sql/explain-analyze/#explain-analyze--with-skew):

```mzsql
SET CLUSTER TO <cluster_name>;
EXPLAIN ANALYZE CLUSTER CPU WITH SKEW;
```

```nofmt
                  object                  | global_id | worker_id | max_operator_cpu_ratio | worker_elapsed  |   avg_elapsed   |  total_elapsed
------------------------------------------+-----------+-----------+------------------------+-----------------+-----------------+-----------------
 materialize.public.orders_enriched       | t356      | 2         |                   3.74 | 00:00:00.675664 | 00:00:00.207876 | 00:00:00.831505
 materialize.public.orders_enriched       | t356      | 1         |                   1.85 | 00:00:00.033013 | 00:00:00.207876 | 00:00:00.831505
 materialize.public.orders_by_customer    | t365      | 1         |                   1.57 | 00:00:00.188396 | 00:00:00.120242 | 00:00:00.48097
 materialize.public.orders_by_customer    | t365      | 2         |                   1.17 | 00:00:00.140562 | 00:00:00.120242 | 00:00:00.48097
 materialize.public.orders_by_customer    | t365      | 3         |                   0.81 | 00:00:00.097985 | 00:00:00.120242 | 00:00:00.48097
 materialize.public.orders_enriched       | t356      | 0         |                   0.77 | 00:00:00.079949 | 00:00:00.207876 | 00:00:00.831505
 materialize.public.orders_enriched       | t356      | 3         |                   0.73 | 00:00:00.042878 | 00:00:00.207876 | 00:00:00.831505
 materialize.public.orders_by_customer    | t365      | 0         |                   0.45 | 00:00:00.054026 | 00:00:00.120242 | 00:00:00.48097
(8 rows)
```

There is one row per worker per dataflow, sorted by ratio, so rows for
different objects interleave. A ratio near `1` means a worker is doing a
roughly average share of the work. Here `orders_enriched` has a worker at
`3.74` on a four-worker replica, close to the theoretical maximum of `4`: one
worker is doing nearly all of that dataflow's work.

{{< important >}}
`max_operator_cpu_ratio` is the maximum across all of a dataflow's operators, so
a high ratio can come from an operator that contributes almost nothing to the
dataflow's CPU time. Only act on a row whose `total_elapsed` is also
significant.
{{< /important >}}

Two further caveats:

- The numbers accumulate from the moment each dataflow was created, so a short
  burst of skew in a long-running dataflow is diluted by its history. To
  measure a window, sample twice and compare.

- On a cluster with many objects the output can run to thousands of rows. Use
  [`EXPLAIN ANALYZE ... AS SQL`](/sql/explain-analyze/#explain-analyze--as-sql)
  to get the underlying query, then filter and sort it yourself.

**Step 2. Localize the skew to an operator.** Drill into the object you
identified:

```mzsql
EXPLAIN ANALYZE CPU WITH SKEW FOR MATERIALIZED VIEW orders_enriched;
```

```nofmt
          operator           | worker_id | cpu_ratio | worker_elapsed  |   avg_elapsed   |  total_elapsed
-----------------------------+-----------+-----------+-----------------+-----------------+-----------------
 Differential Join %0 » %1   | 0         |      0.08 | 00:00:00.006928 | 00:00:00.087401 | 00:00:00.349606
 Differential Join %0 » %1   | 1         |      0.11 | 00:00:00.009606 | 00:00:00.087401 | 00:00:00.349606
 Differential Join %0 » %1   | 2         |      3.73 | 00:00:00.326225 | 00:00:00.087401 | 00:00:00.349606
 Differential Join %0 » %1   | 3         |      0.08 | 00:00:00.006846 | 00:00:00.087401 | 00:00:00.349606
   Arrange (#0{customer_id}) | 0         |      0.77 | 00:00:00.002539 | 00:00:00.00329  | 00:00:00.01316
   Arrange (#0{customer_id}) | 1         |      1.84 | 00:00:00.006057 | 00:00:00.00329  | 00:00:00.01316
   Arrange (#0{customer_id}) | 2         |      0.65 | 00:00:00.002136 | 00:00:00.00329  | 00:00:00.01316
   Arrange (#0{customer_id}) | 3         |      0.74 | 00:00:00.002426 | 00:00:00.00329  | 00:00:00.01316
     Read u11                |           |           |                 |                 |
   Arrange (#1{customer_id}) | 0         |       0.6 | 00:00:00.070684 | 00:00:00.117398 | 00:00:00.469594
   Arrange (#1{customer_id}) | 1         |      0.15 | 00:00:00.017521 | 00:00:00.117398 | 00:00:00.469594
   Arrange (#1{customer_id}) | 2         |      2.96 | 00:00:00.347571 | 00:00:00.117398 | 00:00:00.469594
   Arrange (#1{customer_id}) | 3         |      0.29 | 00:00:00.033816 | 00:00:00.117398 | 00:00:00.469594
     Read u10                |           |           |                 |                 |
(14 rows)
```

Use `FOR INDEX <object_name>` for an index. The operator with the highest
ratio, most often a `Join` or `Reduce`, is where the skew originates. Here it
is the join, and the arrangement it reads from names the offending key:
`customer_id`.

The per-object form reports `cpu_ratio` rather than the `max_operator_cpu_ratio`
of the cluster-wide form, since it is already per operator. Leaf `Read` rows
have no per-worker records of their own and report empty metrics.

**Step 3. Find the hot key.** A skewed `Join` or `Reduce` is almost always
caused by a **hot key**: one value in the join or `GROUP BY` column(s) accounts
for far more rows than the rest. Confirm it by counting rows per value:

```mzsql
SELECT customer_id, count(*) AS num_rows
FROM orders
GROUP BY customer_id
ORDER BY num_rows DESC
LIMIT 20;
```

```nofmt
 customer_id | num_rows
-------------+----------
           1 |   390000
        1024 |        1
        1280 |        1
        1536 |        1
        1792 |        1
        2048 |        1
...
(20 rows)
```

A single value with an outsized count confirms the hot key. A common trigger is
an upstream change that collapses a once-diverse column to a single value or to
`NULL`. See [Is work distributed equally across
workers?](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
for other causes of skew, such as cross joins and
`ORDER BY`/`LIMIT`/`OFFSET` queries.

{{< note >}}
Not every plan shape skews on a hot key. `count` and `sum` plan as an
accumulable reduce, whose cost barely depends on rows per key, and `min`/`max`
over a non-monotonic input plans as a bucketed hierarchical reduce, which
deliberately spreads one key across workers. Joins skew the hardest.
{{< /note >}}

### Resolution

Restructure the query so the hot key's rows aren't concentrated on a single
worker, for example by pre-aggregating or filtering rows before the join, or by
adding a column to the key so that values spread across more workers.

If the skew can't be eliminated, size the cluster for the busiest worker's load
rather than the average, since the other workers won't absorb it.

### Prevention

- Key joins and aggregations on columns with high cardinality. A column that is
  mostly one value, or mostly `NULL`, concentrates on one worker by
  construction.

- Watch for upstream schema changes that collapse a once-diverse column to a
  single value or to `NULL`. This is the most common way a healthy dataflow
  becomes skewed without any change on the Materialize side.

- Test with data whose key distribution matches production. Skew is a property
  of the data, not of the query, so uniformly distributed test data will not
  reproduce it.

- Monitor per-worker CPU rather than the replica average, since `cpu_percent`
  hides skew by construction.

## An expensive object

One dataflow can dominate a cluster's CPU without any skew: the work is spread
evenly across workers, there is just a lot of it.

### Diagnosing the issue

```mzsql
SET CLUSTER TO <cluster_name>;
EXPLAIN ANALYZE CLUSTER CPU;
```

```nofmt
                  object                  | global_id |  total_elapsed
------------------------------------------+-----------+-----------------
 materialize.public.orders_enriched       | t356      | 00:00:00.832362
 materialize.public.orders_by_customer    | t365      | 00:00:00.482028
(2 rows)
```

The output is sorted by `total_elapsed`, so the objects at the top are the
cluster's most expensive dataflows. Drill into one with
`EXPLAIN ANALYZE CPU FOR MATERIALIZED VIEW <object_name>` to see its operators.

{{< note >}}
`total_elapsed` accumulates from the moment the dataflow was created, so a
long-lived object can outrank one that is expensive *right now*. To see which
operators are busy at this moment, use
[`mz_compute_operator_durations_histogram`](/transform-data/dataflow-troubleshooting/#debugging-expensive-dataflows-and-operators).
{{< /note >}}

### Resolution

[Optimize the expensive object](/transform-data/optimization/). Cross joins and
joins without a suitable index are the usual culprits. Alternatively, move the
object to its own cluster, or size the cluster up with
[`ALTER CLUSTER ... SET (SIZE = '<new size>')`](/sql/alter-cluster/).

### Prevention

- Review the plan of a new object with [`EXPLAIN
  PLAN`](/sql/explain-plan/) before it reaches a production cluster, and check
  that joins use an index on the join key.

- Keep unrelated expensive objects on separate clusters, so that one object's
  cost cannot starve the others. See the [operational
  guidelines](/clusters/operational-guidelines/).

## Hydration

Hydration is CPU-intensive: the replica reprocesses each object's inputs from
scratch. Any event that drops a replica's in-memory state re-runs that work, and
hydrating a new object on a busy cluster can saturate it outright.

### Diagnosing the issue

Check what is still hydrating, including sources and sinks:

```mzsql
SELECT o.name, h.replica_id
FROM mz_internal.mz_hydration_statuses h
JOIN mz_catalog.mz_objects o ON h.object_id = o.id
JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE c.name = '<cluster_name>'
  AND NOT coalesce(h.hydrated, false);
```

```nofmt
  name  | replica_id
--------+------------
 big_mv |
(1 row)
```

Zero rows is the healthy steady state: everything on the cluster is hydrated.
A `NULL` `replica_id` means compute introspection has not reported on the object
yet, which is normal for an object that has only just been created.

`hydrated` is itself `NULL` for a sink that has no status row yet, so the
`coalesce` is what keeps those sinks in the result rather than filtering out
exactly the objects the query looks for.

### Resolution

Hydration resolves itself, so no action is needed unless it keeps recurring. If
it does, find what is triggering it:

- **Replica restarts**, including [OOM crash
  loops](/transform-data/freshness-troubleshooting/#check-for-oom-crash-loops),
  which rehydrate the whole cluster on every restart.

- **DDL or deploy activity**. Creating, altering, or dropping an object hydrates
  it on whichever cluster it lives on. See [Check for DDL or deploy
  activity](/transform-data/freshness-troubleshooting/#check-for-ddl-or-deploy-activity).

- **Cluster resizes**, which hydrate the new replicas before dropping the old
  ones.

### Prevention

- Introduce new objects through a [blue/green
  deployment](/developer-tools/dbt/blue-green-deployments/) rather than on a
  live production cluster, so hydration happens on a cluster that is not
  serving traffic.

- Size clusters so that they do not OOM, since an OOM crash loop rehydrates the
  whole cluster on every restart.

- To shorten hydration without paying for a larger cluster continuously, use
  [autoscaling](/clusters/autoscaling/).

## Ad-hoc query load

A cluster serves `SELECT`s from the same replicas that maintain its indexes and
materialized views, so query traffic and maintenance work compete for the same
CPU. A burst of queries, or a query that can't be answered from an index, shows
up as a cluster-wide spike. Client-side restart loops are a frequent cause: each
reconnect resubmits the same queries, and each one spins up a temporary
dataflow.

### Diagnosing the issue

```mzsql
SET CLUSTER TO <cluster_name>;

SELECT object_id, type, count(*) AS active_peeks
FROM mz_introspection.mz_active_peeks
GROUP BY object_id, type
ORDER BY active_peeks DESC;
```

```nofmt
 object_id | type  | active_peeks
-----------+-------+--------------
 u9        | index |            2
 t331      | index |            1
(2 rows)
```

A `u` prefix identifies a user object, here an index answering two reads. A `t`
prefix is a transient dataflow, built to answer a query that no existing
arrangement could serve. A steady supply of `t` rows means queries are paying to
build dataflows rather than reading from indexes.

This is a point-in-time snapshot of in-flight reads, so sample it repeatedly
during a spike. For the window after the fact, query
[`mz_internal.mz_recent_activity_log`](/sql/system-catalog/mz_internal/#mz_recent_activity_log)
instead.

### Resolution

Serve queries from a cluster separate from the one maintaining the objects, as
described in the [operational
guidelines](/clusters/operational-guidelines/#three-tier-architecture).

### Prevention

- Separate serving clusters from compute clusters from the start, so that a
  burst of query traffic cannot delay maintenance work. Use [role-based access
  control](/security/cloud/access-control/#role-based-access-control-rbac) to
  keep ad-hoc queries off production compute clusters.

- Index the columns that ad-hoc queries filter on, so that a query is answered
  from an existing arrangement instead of building a temporary dataflow. A view
  that is queried repeatedly is cheaper as a materialized view than as an
  ad-hoc `SELECT` over an unmaterialized one.

- Configure clients with backoff on reconnect, so that a client-side restart
  loop does not multiply into repeated query bursts.

## Upstream data volume

A dataflow's steady-state CPU is proportional to the rate of change flowing
through it, not to the size of its inputs. A cluster that was comfortable can
saturate when an upstream system starts writing faster, with no change on the
Materialize side.

### Diagnosing the issue

If the cluster hosts sources, sample their counters twice a minute apart and
compare:

```mzsql
SELECT
    s.name,
    ss.messages_received,
    ss.updates_committed
FROM mz_internal.mz_source_statistics ss
JOIN mz_catalog.mz_sources s ON ss.id = s.id
JOIN mz_catalog.mz_clusters c ON s.cluster_id = c.id
WHERE c.name = '<cluster_name>';
```

```nofmt
 name | messages_received | updates_committed
------+-------------------+-------------------
 lg   |               171 |               170
(1 row)
```

These counters are best-effort and only meaningful as rates; see [counter
metrics](/sql/system-catalog/mz_internal/#counter-metrics). Joining
`mz_catalog.mz_sources` restricts the result to top-level sources. To include
the tables created from a source, join `mz_catalog.mz_objects` instead.

`mz_source_statistics` is keyed by `(id, replica_id)`, so a cluster with a
replication factor above 1 returns one row per source per replica. Add
`ss.replica_id` to the select list to tell those rows apart.

### Resolution

If the rate is far above what the workload was sized for, either size the
cluster up or reduce the volume upstream.

If the sources live on a different cluster from the objects that spiked, check
that cluster too: downstream compute inherits its inputs' change rate. See
[Check source
ingestion](/transform-data/freshness-troubleshooting/#check-source-ingestion).

### Prevention

- Size clusters against the peak change rate rather than the average, and
  revisit the sizing when upstream workloads change.

- [Monitor freshness](/transform-data/monitor-freshness/) so that a rising
  change rate surfaces as lag before it surfaces as saturation.

## Memory pressure causing increased spill to disk

A replica that is close to its memory limit spills data to disk, and the
resulting paging registers as CPU time that isn't doing any of your work.

### Diagnosing the issue

Check the other columns of
[`mz_cluster_replica_utilization`](/sql/system-catalog/mz_internal/#mz_cluster_replica_utilization)
for the same window:

```mzsql
SELECT
    r.name AS replica_name,
    u.cpu_percent,
    u.memory_percent,
    u.disk_percent,
    u.swap_percent
FROM mz_internal.mz_cluster_replica_utilization u
JOIN mz_catalog.mz_cluster_replicas r ON u.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
WHERE c.name = '<cluster_name>';
```

```nofmt
 replica_name | cpu_percent |   memory_percent   | disk_percent | swap_percent
--------------+-------------+--------------------+--------------+--------------
 r1           | 0.863405875 | 0.8431712962962964 |              |
(1 row)
```

`disk_percent` and `swap_percent` are `NULL` when the replica reports no
corresponding limit, as above. A replica under memory pressure reports rising
values in both.

### Resolution

High `memory_percent` alongside rising `disk_percent` or `swap_percent` means
you are looking at a memory problem wearing a CPU costume. Size the cluster up,
or reduce the memory footprint of its objects.

### Prevention

- Alert on `memory_percent` as well as `cpu_percent`, so that memory pressure
  is caught before it presents as a CPU anomaly.

- Reduce the memory footprint of expensive objects, for example by filtering
  earlier in the dataflow or by reducing the number of arrangements. See
  [optimization](/transform-data/optimization/).

## An undersized cluster

If CPU is spread evenly across workers with no recent hydration, DDL, or
upstream change, the cluster is undersized for its workload.

### Diagnosing the issue

To measure how much headroom is left, subscribe to the time workers spend idle.
`SUBSCRIBE` streams results, so wrap it in `COPY ... TO STDOUT` or a cursor;
a bare `SUBSCRIBE` prints nothing in `psql`.

```mzsql
SET CLUSTER TO <cluster_name>;
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (
    SELECT sum(slept_for_ns * count)
    FROM mz_introspection.mz_scheduling_parks_histogram
);
FETCH 5 c WITH (timeout = '6s');
COMMIT;
```

```nofmt
 mz_timestamp  | mz_diff |      sum
---------------+---------+---------------
 1789489597053 |       1 | 1232890339840
 1789489598000 |      -1 | 1232890339840
 1789489598000 |       1 | 1239430677504
 1789489599000 |       1 | 1244927487744
 1789489599000 |      -1 | 1239430677504
(5 rows)
```

Each tick emits a retraction (`mz_diff = -1`) of the previous value and an
insertion (`mz_diff = 1`) of the new one.

The `sum` is **cumulative** parked nanoseconds across all of the replica's
workers, not a rate, so take the difference between two insertions to get the
idle nanoseconds for that interval. Over a window of `T` seconds, a fully idle
replica accrues `T × <number of workers> × 1e9` of them. As a rule of thumb, a
replica with healthy headroom stays above 10% of that. A size's worker count is
`processes * workers` from
[`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes),
since `workers` is per process.

Because this aggregates across workers, it will not reveal skew. Use it
alongside `EXPLAIN ANALYZE CLUSTER CPU`, not instead of it.

### Resolution

Size the cluster up with [`ALTER CLUSTER ... SET (SIZE = '<new
size>')`](/sql/alter-cluster/) or move objects to another cluster.

{{< important >}}
Sizing up does not always reduce CPU. A cluster runs each object's operators on
every worker, so the operator count grows with both the number of objects and
the number of workers, and coordination overhead grows with it. A cluster that
hosts many objects can therefore use *more* CPU after a resize.

If freshness degrades and CPU rises after sizing up, the cluster is too large
for its workload. Size back down, or split the objects across several smaller
clusters.
{{< /important >}}

### Prevention

- Track idle time continuously rather than sampling it during an incident, so
  that shrinking headroom is visible before the cluster saturates.

- Size from the hydration peak. If CPU during hydration stayed well below the
  replica's capacity, the cluster has room to shrink.

- Prefer several smaller clusters over one large one, particularly when hosting
  many objects. Cross-cluster joins and strict serializability still hold.

- Follow the [operational guidelines](/clusters/operational-guidelines/) for
  how to lay out clusters. See [Check cluster
  health](/transform-data/freshness-troubleshooting/#check-cluster-health) for
  the corresponding freshness symptoms.
