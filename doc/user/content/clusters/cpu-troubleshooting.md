---
title: "Cluster CPU troubleshooting"
description: "How to diagnose a spike in cluster CPU utilization."
menu:
  main:
    name: "Troubleshoot CPU spikes"
    identifier: cpu-troubleshooting
    parent: clusters
    weight: 90
---

A cluster replica's CPU goes to the dataflows that maintain the cluster's
indexes, materialized views, and sinks, and to the ad-hoc queries served from
it. A spike means one of those started doing more work, or that the same work
stopped being spread evenly across the replica's workers.

## Common causes

| Cause | Check |
| ----- | ----- |
| **Worker skew**: one worker does far more work than its peers, so the replica saturates a core while the rest idle. | [Check for worker skew](#check-for-worker-skew) |
| **An expensive object**: one dataflow dominates the cluster's CPU. | [Find the objects consuming CPU](#find-the-objects-consuming-cpu) |
| **Hydration**: a replica restart, cluster resize, or DDL forced objects to rebuild from their inputs. | [Check for recent hydration](#check-for-recent-hydration) |
| **Ad-hoc query load**: `SELECT`s served by the cluster compete with its maintenance work. | [Check ad-hoc query load](#check-ad-hoc-query-load) |
| **Upstream volume**: more data is arriving from sources, so there is more incremental work to do. | [Check upstream data volume](#check-upstream-data-volume) |
| **Memory pressure**: a replica that is paging to disk burns CPU on I/O rather than on your dataflows. | [Rule out memory pressure](#rule-out-memory-pressure) |
| **An undersized cluster**: the workload is spread evenly and nothing has changed; there is simply not enough compute. | [Rule out general cluster overload](#rule-out-general-cluster-overload) |

## Confirm the spike window and scope

Before diagnosing a cause, establish when the spike started and which replicas
it affected, replacing `<cluster_name>` with the name of your cluster:

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

Every replica of a cluster maintains the same objects, so:

- If **all replicas** spike together, the workload itself changed. Continue with
  the checks below.

- If **one replica** spikes, that replica is doing something its peers are not,
  most often rehydrating after a restart. See [Check for recent
  hydration](#check-for-recent-hydration).

- If the spike **repeats on a fixed cadence**, or CPU stays high while the
  cluster's inputs are idle, suspect a [temporal
  filter](/transform-data/optimization/#improve-performance-when-using-temporal-filters).
  A window whose boundary moves with `mz_now()` retracts rows as they age out,
  so it generates work with no upstream input at all.

{{< note >}}
`cpu_percent` is a percentage of the replica's *total* allocation across all of
its cores, so it averages over workers. A replica whose work is concentrated on
one worker can peg a core while reporting a modest `cpu_percent`. Degraded
freshness with unremarkable CPU is a strong signal of [worker
skew](#check-for-worker-skew), not a reason to stop looking.
{{< /note >}}

## Check for worker skew

Materialize distributes work across a replica's workers by hashing keys, such
as join keys and `GROUP BY` keys. If one key value accounts for a
disproportionate share of rows, the worker responsible for that value does
disproportionate work while its peers idle.

To check for skew across an entire cluster, connect to it and run
[`EXPLAIN ANALYZE CLUSTER CPU WITH SKEW`](/sql/explain-analyze/#explain-analyze--with-skew):

```mzsql
SET CLUSTER TO <cluster_name>;
EXPLAIN ANALYZE CLUSTER CPU WITH SKEW;
```

The output reports each dataflow's CPU time per worker against the average
across workers, alongside the `global_id` of the underlying index, materialized
view, or sink. A ratio near `1` means a worker is doing a roughly average share
of the work; a ratio far above `1` on one worker points to skew in that object.

{{< important >}}
`max_operator_cpu_ratio` is the maximum across all of a dataflow's operators, so
a high ratio can come from an operator that contributes almost nothing to the
dataflow's CPU time. Only act on a row whose `total_elapsed` is also
significant.
{{< /important >}}

Two further caveats:

- The numbers accumulate from the moment each dataflow was created, so a short
  burst of skew in a long-running dataflow is diluted by its history.

- On a cluster with many objects the output can run to thousands of rows. Use
  [`EXPLAIN ANALYZE ... AS SQL`](/sql/explain-analyze/#explain-analyze--as-sql)
  to get the underlying query, then filter and sort it yourself.

### Localize the skew to an operator

Once you've identified a skewed object by its `global_id`, drill into it to find
the specific operator responsible:

```mzsql
EXPLAIN ANALYZE CPU WITH SKEW FOR MATERIALIZED VIEW <object_name>;
```

(Use `FOR INDEX <object_name>` for an index.) The operator with the highest
ratio, most often a `Join` or `Reduce`, is where the skew originates.

### Find the hot key

A skewed `Join` or `Reduce` operator is almost always caused by a **hot key**:
one value in the join or `GROUP BY` column(s) accounts for far more rows than
the rest. Confirm it by counting rows per value on the suspect column:

```mzsql
SELECT <key_column>, count(*) AS num_rows
FROM <object_name>
GROUP BY <key_column>
ORDER BY num_rows DESC
LIMIT 20;
```

A single value with an outsized count relative to the rest confirms the hot key.
A common trigger is an upstream change that collapses a once-diverse column to a
single value or to `NULL`. See [Is work distributed equally across
workers?](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)
for other causes of skew, such as cross joins and
`ORDER BY`/`LIMIT`/`OFFSET` queries.

To resolve skew, restructure the query so the hot key's rows aren't concentrated
on a single worker, for example by pre-aggregating or filtering rows before the
join. If the skew can't be eliminated, size the cluster for the busiest worker's
load rather than the average, since the other workers won't absorb it.

## Find the objects consuming CPU

If the work is spread evenly across workers, find which objects the CPU is
going to:

```mzsql
SET CLUSTER TO <cluster_name>;
EXPLAIN ANALYZE CLUSTER CPU;
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

To resolve, [optimize the expensive object](/transform-data/optimization/), move
it to its own cluster, or size the cluster up with [`ALTER CLUSTER ... SET (SIZE
= '<new size>')`](/sql/alter-cluster/). Cross joins and joins without a suitable
index are the usual culprits.

## Check for recent hydration

Hydration is CPU-bound: the replica reprocesses each object's inputs from
scratch. Any event that drops a replica's in-memory state re-runs that work, and
hydrating a new object on a busy cluster can saturate it outright.

To check whether a hydration episode overlaps the spike window:

```mzsql
SELECT
    r.name AS replica_name,
    h.started_at,
    h.finished_at,
    h.object_count
FROM mz_internal.mz_replica_hydration_history h
JOIN mz_catalog.mz_clusters c ON h.cluster_id = c.id
LEFT JOIN mz_catalog.mz_cluster_replicas r ON h.replica_id = r.id
WHERE c.name = '<cluster_name>'
ORDER BY h.started_at DESC
LIMIT 10;
```

To check what is still hydrating right now, including sources and sinks:

```mzsql
SELECT o.name, h.replica_id
FROM mz_internal.mz_hydration_statuses h
JOIN mz_catalog.mz_objects o ON h.object_id = o.id
JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE c.name = '<cluster_name>'
  AND NOT h.hydrated;
```

Hydration resolves itself, so no action is needed unless it keeps recurring. If
it does, find what is triggering it:

- **Replica restarts**, including [OOM crash
  loops](/transform-data/freshness-troubleshooting/#check-for-oom-crash-loops),
  which rehydrate the whole cluster on every restart.

- **DDL or deploy activity**. Creating, altering, or dropping an object hydrates
  it on whichever cluster it lives on, which is why new objects belong in a
  [blue/green deployment](/developer-tools/dbt/blue-green-deployments/) rather
  than on a live production cluster. See [Check for DDL or deploy
  activity](/transform-data/freshness-troubleshooting/#check-for-ddl-or-deploy-activity).

- **Cluster resizes**, which hydrate the new replicas before dropping the old
  ones.

To shorten hydration without paying for a larger cluster continuously, see
[autoscaling](/clusters/autoscaling/).

## Check ad-hoc query load

A cluster serves `SELECT`s from the same replicas that maintain its indexes and
materialized views, so query traffic and maintenance work compete for the same
CPU. A burst of queries, or a query that can't be answered from an index, shows
up as a cluster-wide spike. Client-side restart loops are a frequent cause: each
reconnect resubmits the same queries, and each one spins up a temporary
dataflow.

Introspection relations report on the cluster you are connected to, so run the
following against the affected cluster:

```mzsql
SET CLUSTER TO <cluster_name>;

SELECT object_id, type, count(*) AS active_peeks
FROM mz_introspection.mz_active_peeks
GROUP BY object_id, type
ORDER BY active_peeks DESC;
```

This is a point-in-time snapshot of in-flight reads; sample it repeatedly during
a spike. For the window after the fact, query
[`mz_internal.mz_recent_activity_log`](/sql/system-catalog/mz_internal/#mz_recent_activity_log)
instead.

To resolve, serve queries from a cluster separate from the one maintaining the
objects, as described in the [operational
guidelines](/clusters/operational-guidelines/#three-tier-architecture).

## Check upstream data volume

A dataflow's steady-state CPU is proportional to the rate of change flowing
through it, not to the size of its inputs. A cluster that was comfortable can
saturate when an upstream system starts writing faster, with no change on the
Materialize side.

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

These counters are best-effort and only meaningful as rates; see [counter
metrics](/sql/system-catalog/mz_internal/#counter-metrics). If the rate is far
above what the workload was sized for, either size the cluster up or reduce the
volume upstream.

If the sources live on a different cluster from the objects that spiked, check
that cluster too: downstream compute inherits its inputs' change rate. See
[Check source
ingestion](/transform-data/freshness-troubleshooting/#check-source-ingestion).

## Rule out memory pressure

A replica that is close to its memory limit spills data to disk, and the
resulting paging registers as CPU time that isn't doing any of your work.
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

High `memory_percent` alongside rising `disk_percent` or `swap_percent` means
you are looking at a memory problem wearing a CPU costume. Resolve it by sizing
the cluster up or by reducing the memory footprint of its objects.

## Rule out general cluster overload

If CPU is spread evenly across workers with no recent hydration, DDL, or
upstream change, the cluster is undersized for its workload. To measure how
much headroom is left, subscribe to the time workers spend idle:

```mzsql
SET CLUSTER TO <cluster_name>;
SUBSCRIBE (SELECT sum(slept_for_ns * count) FROM mz_introspection.mz_scheduling_parks_histogram);
```

Each update reports the total time the cluster's workers have spent idle. Over a
window of `T` seconds a fully idle cluster accrues `T × <number of workers>`
seconds; as a rule of thumb, a cluster with healthy headroom stays above 10% of
that. ([`mz_catalog.mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
gives the worker count for a size.) Note that this aggregates across workers, so
it will not reveal skew.

To resolve, size the cluster up with [`ALTER CLUSTER ... SET (SIZE = '<new
size>')`](/sql/alter-cluster/) or move objects to another cluster. See [Check
cluster health](/transform-data/freshness-troubleshooting/#check-cluster-health)
for the corresponding freshness symptoms, and the [operational
guidelines](/clusters/operational-guidelines/) for how to lay out clusters.
