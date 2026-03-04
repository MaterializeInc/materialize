---
title: "Freshness troubleshooting"
description: "How to diagnose and resolve freshness problems in Materialize."
menu:
  main:
    name: "Freshness troubleshooting"
    identifier: freshness-troubleshooting
    parent: transform-data
    weight: 85
---

[Freshness](/concepts/reaction-time/#freshness) measures the time from when a change occurs in an upstream system to when it becomes visible in the results of a query.
This guide helps you diagnose why freshness is degraded and identify which component in the dependency graph is responsible.

## Key concepts

Materialize tracks freshness through **write frontiers**: each object (source, table, materialized view, index, sink) has a frontier timestamp representing the point up to which data has been processed.
The difference between an object's write frontier and wall-clock time is its **wallclock lag**.
The difference between an object's write frontier and its inputs' write frontiers is its **materialization lag**.

When an object's freshness degrades, the cause is one of:

* The **source** is not ingesting data fast enough (upstream connectivity, replication lag).
* The **cluster** is overloaded and cannot keep up with the rate of changes (CPU or memory pressure).
* A specific **edge in the dependency graph** introduces processing delay (e.g., an expensive materialized view or a slow sink).

## Step 1: Check current freshness

Query [`mz_internal.mz_wallclock_global_lag`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag) to see the current lag for all user objects:

```mzsql
SELECT o.id, o.name, o.type, wl.lag
FROM mz_internal.mz_wallclock_global_lag wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
WHERE o.id LIKE 'u%'
ORDER BY wl.lag DESC NULLS LAST
LIMIT 20;
```

Objects with a lag of a few seconds are typical for healthy systems.
Large lag values (minutes or hours) indicate a problem.

For historical trends, query [`mz_internal.mz_wallclock_global_lag_recent_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history):

```mzsql
SELECT o.name, wl.lag, wl.occurred_at
FROM mz_internal.mz_wallclock_global_lag_recent_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
WHERE o.id = '<object_id>'
ORDER BY wl.occurred_at DESC
LIMIT 50;
```

## Step 2: Identify which object lags

[`mz_internal.mz_materialization_lag`](/sql/system-catalog/mz_internal/#mz_materialization_lag) breaks down lag for each materialization (index, materialized view, sink) into two components:

* **`local_lag`**: how far behind the object is compared to its direct inputs.
* **`global_lag`**: how far behind the object is compared to its root inputs (sources and tables).

The difference between `global_lag` and `local_lag` is the lag accumulated by upstream objects.

```mzsql
SELECT
    o.name,
    o.type,
    ml.local_lag,
    ml.global_lag,
    slo.name AS slowest_local_input,
    sgo.name AS slowest_global_input
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
LEFT JOIN mz_catalog.mz_objects slo ON ml.slowest_local_input_id = slo.id
LEFT JOIN mz_catalog.mz_objects sgo ON ml.slowest_global_input_id = sgo.id
WHERE o.id LIKE 'u%'
  AND ml.global_lag > INTERVAL '5 seconds'
ORDER BY ml.global_lag DESC;
```

Interpretation:

* **`local_lag` is high, `global_lag` is similar**: the object itself is the bottleneck.
  The cluster running it may be overloaded, or the dataflow is expensive.
* **`local_lag` is low, `global_lag` is high**: an upstream dependency is the bottleneck.
  Look at the `slowest_global_input` to identify the root cause.
* **`local_lag` is zero, `global_lag` is zero, but wallclock lag is high**: the root source is behind.
  The entire pipeline is caught up relative to its inputs, but the inputs themselves lag behind wall-clock time.

## Step 3: Check source health

If the slowest root input is a source or subsource, check its status:

```mzsql
SELECT s.id, o.name, s.status, s.error
FROM mz_internal.mz_source_statuses s
JOIN mz_catalog.mz_objects o ON s.id = o.id
WHERE o.id LIKE 'u%'
  AND s.status <> 'running';
```

A source with status `stalled` or `starting` will hold back all downstream objects.
For PostgreSQL sources, the subsources share replication state with the parent source; if one subsource lags, all subsources of that source typically lag together.

Check the frontier of a specific source against wall-clock time:

```mzsql
SELECT
    o.name,
    to_timestamp(f.write_frontier::text::double / 1000) AS frontier_time,
    now() - to_timestamp(f.write_frontier::text::double / 1000) AS behind_wallclock
FROM mz_internal.mz_frontiers f
JOIN mz_catalog.mz_objects o ON f.object_id = o.id
WHERE o.id = '<source_id>';
```

## Step 4: Check cluster health

If the bottleneck is a materialized view, index, or sink, check the cluster's resource utilization:

```mzsql
SELECT
    c.name AS cluster_name,
    r.name AS replica_name,
    u.cpu_percent,
    u.memory_percent
FROM mz_internal.mz_cluster_replica_utilization u
JOIN mz_catalog.mz_cluster_replicas r ON u.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
ORDER BY u.cpu_percent DESC;
```

When a cluster has high CPU utilization, all objects on that cluster experience correlated freshness degradation.
High memory can force data to disk that also slows down processing.

To confirm that lag is cluster-wide, check all objects on the affected cluster:

```mzsql
SELECT
    o.name,
    o.type,
    ml.local_lag,
    ml.global_lag
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
WHERE o.cluster_id = (SELECT id FROM mz_catalog.mz_clusters WHERE name = '<cluster_name>')
  AND o.id LIKE 'u%'
ORDER BY ml.local_lag DESC;
```

If all objects on the cluster have similar `local_lag`, the cluster is the bottleneck.
Consider scaling the cluster up or moving expensive workloads to a dedicated cluster.

### Check for OOM crash loops

A cluster that repeatedly runs out of memory will have its replica crash and restart.
Each restart triggers rehydration, during which no progress is made, causing recurring freshness degradation.

Check the current replica status:

```mzsql
SELECT
    c.name AS cluster_name,
    rs.replica_id,
    rs.process_id,
    rs.status,
    rs.reason,
    rs.updated_at
FROM mz_internal.mz_cluster_replica_statuses rs
JOIN mz_catalog.mz_cluster_replicas r ON rs.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
WHERE c.name = '<cluster_name>';
```

A replica with status `offline` and reason `oom-killed` confirms the cluster is currently out of memory.

Check whether the replica has been restarting repeatedly:

```mzsql
SELECT
    rsh.replica_id,
    rsh.status,
    rsh.reason,
    rsh.occurred_at
FROM mz_internal.mz_cluster_replica_status_history rsh
JOIN mz_internal.mz_cluster_replica_history rh ON rsh.replica_id = rh.replica_id
WHERE rh.cluster_name = '<cluster_name>'
ORDER BY rsh.occurred_at DESC
LIMIT 20;
```

A repeating pattern of `offline` with reason `oom-killed` followed by `online` confirms a crash loop.
The time between restarts indicates the severity: a replica that OOMs every few minutes is fundamentally too small for its workload.

To see the full lifecycle of replicas, including how often new ones are created:

```mzsql
SELECT
    rh.replica_id,
    rh.size,
    rh.created_at,
    rh.dropped_at,
    rh.dropped_at - rh.created_at AS uptime
FROM mz_internal.mz_cluster_replica_history rh
WHERE rh.cluster_name = '<cluster_name>'
ORDER BY rh.created_at DESC
LIMIT 20;
```

**Resolution**: The cluster is undersized for its workload.
Scale it up to a larger size, or reduce the number of objects on the cluster.

## Step 5: Attribute lag through the dependency graph

For more complex pipelines, you may need to trace which specific edge in the dependency graph introduces delay.
The following query walks the full dependency chain and computes the delay introduced at each edge:

```mzsql
WITH MUTUALLY RECURSIVE
    depends_on(probe text, prev text, next text) AS (
        SELECT object_id, object_id, object_id
        FROM mz_internal.mz_frontiers
        WHERE object_id LIKE 'u%'
        UNION
        SELECT depends_on.probe, d.dependency_id, d.object_id
        FROM mz_internal.mz_materialization_dependencies d, depends_on
        WHERE depends_on.prev = d.object_id
    )
SELECT
    o_probe.name AS object_name,
    o_prev.name AS from_name,
    o_prev.type AS from_type,
    o_next.name AS to_name,
    o_next.type AS to_type,
    greatest(
        to_timestamp(fn.write_frontier::text::double / 1000)
            - to_timestamp(fp.write_frontier::text::double / 1000),
        INTERVAL '0'
    ) AS edge_delay
FROM depends_on
JOIN mz_internal.mz_frontiers fp ON depends_on.prev = fp.object_id
JOIN mz_internal.mz_frontiers fn ON depends_on.next = fn.object_id
JOIN mz_catalog.mz_objects o_probe ON depends_on.probe = o_probe.id
JOIN mz_catalog.mz_objects o_prev ON depends_on.prev = o_prev.id
JOIN mz_catalog.mz_objects o_next ON depends_on.next = o_next.id
WHERE depends_on.prev <> depends_on.next
  AND fn.write_frontier <= fp.write_frontier
  AND fp.write_frontier::text::numeric > fn.write_frontier::text::numeric
ORDER BY edge_delay DESC
LIMIT 30;
```

Each row represents an edge in the dependency graph where the downstream object (`to_name`) is behind its upstream input (`from_name`) by `edge_delay`.
The `object_name` column indicates which user-facing object is affected by this delay.

To trace a specific object's dependency chain, replace the first `SELECT` in the recursive CTE:

```mzsql
-- Replace the base case to trace a single object
SELECT '<object_id>', '<object_id>', '<object_id>'
```

## Step 6: Check sink lag

Sinks export data to external systems and often introduce lag due to batching and commit intervals.

```mzsql
SELECT
    o.name AS sink_name,
    s.status,
    ml.local_lag,
    slo.name AS slowest_input
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
JOIN mz_internal.mz_sink_statuses s ON s.id = o.id
LEFT JOIN mz_catalog.mz_objects slo ON ml.slowest_local_input_id = slo.id
WHERE o.type = 'sink'
  AND o.id LIKE 'u%'
ORDER BY ml.local_lag DESC;
```

## Measuring aggregate freshness

The steps above diagnose individual objects.
To measure overall freshness across your deployment, for example, to answer "what is our P99.999 freshness?", aggregate over [`mz_internal.mz_wallclock_global_lag_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_history).

### Filtering noise

Raw aggregation over all objects produces misleading results because several categories of objects report high lag without representing a real freshness problem:

* **Paused sources**: A deliberately paused source stops ingesting data and its lag grows indefinitely.
  Filter these out by joining `mz_internal.mz_source_statuses` and excluding `status = 'paused'`.
* **Zero-replica clusters**: Clusters with `replication_factor = 0` have no compute assigned.
  Their frontiers are frozen and lag grows linearly over time, but no work is expected.
* **Static data (dbt seeds, snapshots)**: Tables loaded once and never updated accumulate lag equal to their age.
  Filter by cluster name or object name patterns.
* **Non-production clusters**: Development or staging clusters may not represent production freshness.

### Peak and threshold-based freshness

At per-object sample sizes in `mz_wallclock_global_lag_history` (one row per minute, up to ~43,200 samples over 30 days), P99.999 is effectively `max(lag)` for any individual object.
A more useful approach is to count how many minutes exceed specific thresholds:

```mzsql
SELECT
    o.name,
    o.type,
    c.name AS cluster_name,
    max(wl.lag) AS peak_lag,
    avg(extract(epoch FROM wl.lag))::int || 's' AS avg_lag,
    count(*) AS total_minutes,
    count(*) FILTER (WHERE wl.lag > INTERVAL '10 seconds') AS above_10s,
    count(*) FILTER (WHERE wl.lag > INTERVAL '1 minute') AS above_1m,
    count(*) FILTER (WHERE wl.lag > INTERVAL '5 minutes') AS above_5m,
    count(*) FILTER (WHERE wl.lag > INTERVAL '30 minutes') AS above_30m
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
LEFT JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
LEFT JOIN mz_internal.mz_source_statuses ss ON o.id = ss.id
WHERE wl.occurred_at > now() - INTERVAL '7 days'
  AND o.id LIKE 'u%'
  AND wl.lag IS NOT NULL
  -- Exclude paused sources
  AND (ss.id IS NULL OR ss.status <> 'paused')
  -- Exclude zero-replica clusters
  AND (c.id IS NULL OR c.replication_factor > 0)
GROUP BY o.id, o.name, o.type, c.name
HAVING max(wl.lag) > INTERVAL '10 seconds'
ORDER BY max(wl.lag) DESC
LIMIT 30;
```

{{< note >}}
`avg(wl.lag)` does not work directly because Materialize does not support `sum(interval)`.
Use `avg(extract(epoch FROM wl.lag))` to compute the average in seconds instead.
{{< /note >}}

### Cluster-level freshness summary

To get a per-cluster summary (useful for SLO reporting):

```mzsql
SELECT
    c.name AS cluster_name,
    count(DISTINCT wl.object_id) AS objects,
    max(wl.lag) AS peak_lag,
    avg(extract(epoch FROM wl.lag))::int || 's' AS avg_lag,
    count(*) FILTER (WHERE wl.lag > INTERVAL '1 minute') AS minutes_above_1m
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
LEFT JOIN mz_internal.mz_source_statuses ss ON o.id = ss.id
WHERE wl.occurred_at > now() - INTERVAL '7 days'
  AND o.id LIKE 'u%'
  AND wl.lag IS NOT NULL
  AND (ss.id IS NULL OR ss.status <> 'paused')
  AND c.replication_factor > 0
GROUP BY c.name
ORDER BY max(wl.lag) DESC;
```

## Investigating historical spikes

Materialize retains wallclock lag history for up to 30 days in [`mz_internal.mz_wallclock_global_lag_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_history), binned by minute.
Use this data to find past freshness spikes and determine their cause.

### Find recent spikes

Identify objects that experienced the largest lag spikes over the last 7 days:

```mzsql
SELECT
    o.name,
    o.type,
    c.name AS cluster_name,
    max(wl.lag) AS peak_lag,
    avg(extract(epoch FROM wl.lag))::int || 's' AS avg_lag,
    count(*) FILTER (WHERE wl.lag > INTERVAL '10 seconds') AS minutes_above_10s
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
LEFT JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE wl.occurred_at > now() - INTERVAL '7 days'
  AND o.id LIKE 'u%'
  AND wl.lag IS NOT NULL
GROUP BY o.name, o.type, c.name, wl.object_id
HAVING max(wl.lag) > INTERVAL '1 minute'
ORDER BY max(wl.lag) DESC
LIMIT 30;
```

Exclude known-stalled objects (e.g., intentionally paused sources) by adding `AND o.id NOT IN (...)` to the `WHERE` clause.

### Determine spike scope

When a spike is identified, determine whether it affected a single object, a single cluster, or the entire environment.
The following query summarizes a time window by cluster:

```mzsql
SELECT
    c.name AS cluster_name,
    count(DISTINCT wl.object_id) AS objects_affected,
    max(wl.lag) AS peak_lag,
    min(wl.lag) FILTER (WHERE wl.lag IS NOT NULL) AS min_lag
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
LEFT JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE wl.occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
  AND o.id LIKE 'u%'
  AND wl.lag > INTERVAL '1 minute'
GROUP BY c.name
ORDER BY peak_lag DESC;
```

Interpreting the results:

* **Single object affected**: The problem is specific to that object's dataflow or its direct inputs.
* **All objects on one cluster affected**: The cluster was overloaded or experienced a replica restart.
  Check `mz_internal.mz_cluster_replica_utilization` for that time window.
* **All clusters affected, including progress sources**: A system-level event occurred (environment restart, upgrade, or storage layer disruption).
  Lag that grows linearly at 1 minute per minute indicates frontiers were completely frozen for the duration of the event.

### Inspect a specific spike

To see the minute-by-minute progression of a spike for a specific object:

```mzsql
SELECT wl.lag, wl.occurred_at
FROM mz_internal.mz_wallclock_global_lag_history wl
WHERE wl.object_id = '<object_id>'
  AND wl.occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
ORDER BY wl.occurred_at;
```

Lag that increases linearly (by ~1 minute per minute) indicates the object's frontier was completely frozen — no progress was being made.
Lag that fluctuates around a baseline indicates the object is processing but cannot keep up with its input rate.

## Common patterns

### Disconnected or stalled source

**Symptoms**: Extremely high wallclock lag (hours or days) on the source and all downstream objects.
All subsources of the same parent source show identical lag.

**Diagnosis**: Check `mz_internal.mz_source_statuses` for errors.
Common causes include network partitions, credential expiration, and upstream database restarts.

**Resolution**: Fix the connectivity issue.
Once the source reconnects, downstream objects catch up automatically.

### Overloaded cluster

**Symptoms**: All objects on the same cluster show elevated `local_lag` that correlates with CPU utilization.
CPU utilization is high.

**Diagnosis**: Check `mz_internal.mz_cluster_replica_utilization`.
Cross-reference with [expensive operators](/transform-data/dataflow-troubleshooting/#identifying-expensive-operators-in-a-dataflow) to find the heaviest workloads.

**Resolution**: Scale the cluster up, or move expensive workloads to a separate cluster.

### Expensive materialized view

**Symptoms**: One materialized view has high `local_lag` while others on the same cluster are fine.

**Diagnosis**: The view's dataflow is expensive.
Use [dataflow troubleshooting](/transform-data/dataflow-troubleshooting/) to identify expensive operators.

**Resolution**: Optimize the view query, or move it to a dedicated cluster with more resources.

### Skew

**Symptoms**: One index or materialized view has high `local_lag` while the CPU utilization is low

**Diagnosis**: The data might be skewed, or the query includes non-data-parallel patterns, like cross joins.
Use [dataflow troubleshooting](/transform-data/dataflow-troubleshooting/) to identify expensive operators.

**Resolution**: Optimize the view query to avoid the problem.

### OOM crash loop

**Symptoms**: An object shows persistent lag that fluctuates.
Historical lag data for the object has gaps.
The cluster has high memory utilization.

**Diagnosis**: Check [`mz_internal.mz_cluster_replica_status_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_status_history) for repeated `oom-killed` events and [`mz_internal.mz_cluster_replica_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_history) for short-lived replicas.
A typical pattern is: the replica hydrates for some time, OOMs, restarts, OOMs again within minutes, and repeats.
During this cycle the cluster makes no sustained progress on frontiers.

**Resolution**: Scale the cluster up.
The cluster cannot hold its working set in memory at its current size.

### System-wide freshness spike

**Symptoms**: All objects across multiple clusters spike simultaneously.
Progress sources (which have no cluster) are also affected.
Lag grows linearly at 1 minute per minute, then recovers.

**Diagnosis**: A system-level event froze all frontiers.
Common causes include environment restarts, version upgrades, and storage layer disruptions.
Use the [historical spike analysis](#determine-spike-scope) to confirm that all clusters were affected at the same time.

**Resolution**: These spikes are typically transient and self-resolving.
If they recur frequently, check environment upgrade schedules and storage layer health.

### Paused source

**Symptoms**: Extremely high wallclock lag (days or more) on a single source and all downstream objects.
Other sources are unaffected.

**Diagnosis**: The source is intentionally paused.
Check `mz_internal.mz_source_statuses` — a status of `paused` confirms this.
Unlike a stalled source, a paused source has no error and was deliberately stopped.

**Resolution**: If the source was paused intentionally, this is expected behavior.
If the source should be active, drop and recreate it, or investigate why it was paused.
When measuring aggregate freshness, exclude paused sources to avoid skewing metrics.

### Zero-replica cluster

**Symptoms**: All objects on a cluster show wallclock lag that grows linearly over time (1 minute per minute).
The cluster has no entries in `mz_internal.mz_cluster_replica_utilization`.
Source health for root inputs is normal.

**Diagnosis**: The cluster has `replication_factor = 0`, meaning no replicas are assigned.
With no compute, frontiers are frozen and lag grows indefinitely:

```mzsql
SELECT c.name, c.replication_factor
FROM mz_catalog.mz_clusters c
WHERE c.replication_factor = 0;
```

This is common for clusters used only during scheduled batch jobs (e.g., dbt snapshot runs) where replicas are scaled to zero between runs to save costs.

**Resolution**: This is expected behavior for zero-replica clusters.
Scale the cluster up when compute is needed.
When measuring aggregate freshness, exclude zero-replica clusters to avoid skewing metrics.

### Correlated subsource lag

**Symptoms**: Multiple subsources of a PostgreSQL source all show similar wallclock lag.

**Diagnosis**: PostgreSQL sources use a single replication stream for all subsources.
If one subsource slows down (e.g., due to a large transaction), all subsources are affected.

**Resolution**: This is expected behavior.
Check the parent source's status and the replication slot lag on the upstream PostgreSQL database.
