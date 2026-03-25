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

[Freshness](/concepts/reaction-time/#freshness) measures the time from when a
change occurs in an upstream system to when it becomes visible in the results of
a query. This guide can help diagnose why freshness is degraded for an object as
well as measure freshness across your deployment.

## Key concepts

| Concept | Description |
| ------- | ----------- |
| **Write frontier** |  The next timestamp at which data for an object can change; i.e., for an object, all its data changes with timestamp less than the write frontier has been processed. Materialize measures freshness using **write frontiers**. Each object (source, table, materialized view, index, sink) has its own write frontier. |
| **Wallclock lag** | The difference between wall-clock time and an object's write frontier. |
| **Materialization lag** | The difference between an object's write frontier and the write frontier of its inputs. Materialization lag can be considered in two parts:  <ul><li>**local_lag**: The lag between the object and its direct inputs. It represents the portion of lag introduced by the object itself.</li><li>**global_lag**: The lag between the object and its root inputs (sources and tables). It represents the total accumulated lag across the dataflow.</li></ul>|

## Common causes

Common causes of lag include:

- [**Materialization lag**](#check-materialization-lag): The object itself
  (and/or a specific input in its dependency graph) introduces a lag.
- [**Cluster health**](#check-cluster-health): The cluster is overloaded, out of
  memory, or missing compute. All objects on the cluster are affected.
- [**Source ingestion**](#check-source-ingestion): The source is behind. All
  downstream objects for the source are affected.

## Check materialization lag

### Step 1. Find lagging object(s)

Query the top 20 user objects by lag:

```mzsql
SELECT
  o.id AS object_id,
  o.name,
  o.type,
  wl.lag,
  c.name as cluster_name,
  c.id as cluster_id
FROM mz_internal.mz_wallclock_global_lag wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE o.id LIKE 'u%'
ORDER BY wl.lag DESC NULLS LAST
LIMIT 20;
```

- Lag of a few seconds typically indicates a healthy system.
- Lag on the order of minutes or hours typically indicates a problem.[^1]

To inspect an object's lag, see [Step
2a](#step-2a-identify-materialization-lag-contributors).

[^1]: Some objects may report high lag without representing a real issue (e.g.,
    [intentionally paused clusters](#check-for-no-compute)). Exclude from the
    query as appropriate.

### Step 2a. Identify materialization lag contributors.

`mz_internal.mz_materialization_lag` breaks down lag for each materialization
(index, materialized view, sink) into two components:

* **`local_lag`**: the lag introduced by the object itself; i.e., how far behind
  the object is compared to its direct inputs.
* **`global_lag`**: the object's lag relative to its root inputs (sources and
  tables).

Use the following query to determine how much lag is introduced by the object
itself versus inherited from upstream dependencies. Replace `<object_id>` with
your object's id:

```mzsql {hl_lines="12"}
SELECT
    o.id AS object_id,
    o.name,
    ml.local_lag,                                  -- lag introduced by the object itself
    ml.global_lag - ml.local_lag AS inherited_lag, -- lag inherited from dependencies
    ml.global_lag,                                 -- total lag
    ml.slowest_local_input_id
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
WHERE o.id = '<object_id>'
ORDER BY ml.global_lag DESC;
```

- To investigate a high `local_lag`, use [Dataflow
  troubleshooting](/transform-data/dataflow-troubleshooting/) to identify
  expensive operators for the object itself. If possible, optimize the query,
  resize the cluster, or move the object to a cluster with more resources.

- To investigate a high `inherited_lag`,

  - For indexes and materialized views, repeat Step 2a for
    `ml.slowest_local_input_id`, or use [Check dependency
    lags](#step-2b-check-dependency-lags) to see all upstream contributions to
    the lag.

  - For sinks, repeat Step 2a for `ml.slowest_local_input_id` to investigate the
    upstream object. Note that sinks can also introduce lag due to batching and
    commit intervals. Additionally, [swapping a
    sink](#check-for-ddl-or-deploy-activity) to point to a new upstream
    materialized view triggers reprocessing, which can temporarily increase lag.

### Step 2b. Check dependency lags

To find the lag contributions from upstream dependencies of a materialized view
or an index (replace `<object_id>` with the id of your materialized view/index):

```mzsql{hl_lines="3 30"}
WITH MUTUALLY RECURSIVE
    depends_on(probe text, prev text, next text) AS (
        SELECT '<object_id>', '<object_id>', '<object_id>' -- update
        UNION
        SELECT d.probe, dep.dependency_id, dep.object_id
        FROM mz_internal.mz_compute_dependencies dep
        JOIN depends_on d ON d.prev = dep.object_id
    )
SELECT
    o_probe.name AS object_name,
    o_prev.name AS from_name,
    o_prev.id   AS from_id,
    o_prev.type AS from_type,
    o_next.name AS to_name,
    o_next.type AS to_type,
    fp.write_frontier::text::numeric
        - fn.write_frontier::text::numeric AS lag_ms
FROM depends_on d
JOIN mz_internal.mz_frontiers fp ON d.prev = fp.object_id
JOIN mz_internal.mz_frontiers fn ON d.next = fn.object_id
JOIN mz_catalog.mz_objects o_probe ON d.probe = o_probe.id
JOIN mz_catalog.mz_objects o_prev ON d.prev = o_prev.id
JOIN mz_catalog.mz_objects o_next ON d.next = o_next.id
WHERE d.prev != d.next
  AND fp.write_frontier IS NOT NULL
  AND fn.write_frontier IS NOT NULL
  AND fn.write_frontier::text::numeric <=
      (SELECT write_frontier::text::numeric
         FROM mz_internal.mz_frontiers
        WHERE object_id = '<object_id>')                   -- update
ORDER BY lag_ms DESC;
```

Using the returned `from_id`, you can iterate Step 2a and Step 2b as needed. To
identify expensive operators for the object, use [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/). If possible,
optimize the query, resize the cluster, or move the object to a cluster with
more resources.

## Check cluster health

A cluster can become overloaded if:

- It is undersized for its workload; or

- It contains an inefficient or overly expensive object that effectively makes
  the cluster undersized.

When a cluster is overloaded, this may manifest as high CPU utilization, high
memory utilization forcing data to disk, or Out of Memory (OOM) crash loops.

### Check the CPU or memory pressure

{{< tabs >}}
{{< tab "Specific cluster" >}}

You can run the following query to check a cluster's resource utilization,
replacing `<cluster_name>` with the name of cluster:

```mzsql
SELECT
    c.name AS cluster_name,
    c.id as cluster_id,
    r.name AS replica_name,
    u.cpu_percent,
    u.memory_percent
FROM mz_internal.mz_cluster_replica_utilization u
JOIN mz_catalog.mz_cluster_replicas r ON u.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
WHERE c.name = '<cluster_name>'
ORDER BY u.cpu_percent DESC;
```
{{< /tab >}}
{{< tab "All clusters" >}}
You can run the following query to check resource utilization for all clusters:

```mzsql
SELECT
    c.name AS cluster_name,
    c.id as cluster_id,
    r.name AS replica_name,
    u.cpu_percent,
    u.memory_percent
FROM mz_internal.mz_cluster_replica_utilization u
JOIN mz_catalog.mz_cluster_replicas r ON u.replica_id = r.id
JOIN mz_catalog.mz_clusters c ON r.cluster_id = c.id
WHERE c.id LIKE 'u%'
ORDER BY u.cpu_percent DESC;
```
{{< /tab >}}
{{< /tabs >}}

- If the returned `cpu_percent` is high, all objects on that cluster experience
  correlated freshness degradation.

- If the returned `memory_percent` is high, Materialize may force data to disk,
  which can slow down processing.

To resolve, scale the cluster up to a larger size ([`ALTER CLUSTER ... SET (SIZE
= '<new size>')`](/sql/alter-cluster/)), and/or move enough objects to another
cluster to reduce load on the current cluster. If the pressure is caused by an
[inefficient/expensive object](#step-1-find-lagging-objects), optimize the query
if possible.

{{< tip >}}

When a sudden increase in lag (i.e., a spike in lag) that affects a
single cluster cannot be explained by CPU or memory pressure, [check whether
DDL or deploy activity](#check-for-ddl-or-deploy-activity) occurred during the
same window.

{{< /tip >}}

### Check for OOM crash loops

A cluster that repeatedly runs out of memory (OOM) will have its replica crash
and restart. Each restart triggers rehydration, during which no progress is
made, causing recurring freshness degradation.

To check if a replica for a cluster is in an OOM crash loop, check its status
history, replacing `<cluster_name>` with your cluster name:

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

A repeating pattern of `offline` with reason `oom-killed` followed by `online`
confirms a crash loop. A replica that OOMs every few minutes is fundamentally
too small for its workload.

To resolve, scale the cluster up to a larger size ([`ALTER CLUSTER ... SET (SIZE
= '<new size>')`](/sql/alter-cluster/)), and/or move enough objects to another
cluster to reduce load on the current cluster. If the pressure is caused by an
[inefficient/expensive object](#step-1-find-lagging-objects), optimize the query
if possible.

### Check for no compute

The cluster has `replication_factor = 0`, meaning no replicas (i.e., compute
resources) are assigned. On clusters without replicas, objects have frontiers
that are frozen, and their lag grows indefinitely.

{{< note >}}
Zero-replica clusters are common for scheduled batch jobs (e.g., dbt snapshot
runs) where replicas are scaled to zero between runs to save costs. High lag
on these clusters is expected and does not indicate a problem.
{{< /note >}}

To check, you can query `mz_catalog.mz_clusters` for clusters whose
`replication_factor = 0`.

```mzsql
SELECT c.name, c.replication_factor
FROM mz_catalog.mz_clusters c
WHERE c.replication_factor = 0;
```

- If the cluster was paused intentionally, no further action is needed.

- If compute is needed, set the replication factor to a non-zero integer
  ([`ALTER CLUSTER ... SET (REPLICATION FACTOR = <int>)`](/sql/alter-cluster/)).


## Check source ingestion

A source ingestion bottleneck occurs when the source is not ingesting data fast
enough. This can be caused by upstream connectivity issues, replication lag,
credential expiration, or a deliberately paused source.

### Check source status

To check if a source or its associated subsource/table is unhealthy, query
[`mz_internal.mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses):


```mzsql
SELECT s.id, o.name, s.type, s.status, s.error, s.details
FROM mz_internal.mz_source_statuses s
JOIN mz_catalog.mz_objects o ON s.id = o.id
WHERE o.id LIKE 'u%'
  AND s.status <> 'running';
```

A source with status `stalled`, `paused`, or `starting` will hold back all its
downstream objects. If the `status` for a source shows:

| Status   | Description |
| -------  | ----------- |
| `stalled`| Common causes include network partitions, credential expiration, and upstream database restarts. Check the returned `error` field and address appropriately. Once the source reconnects, downstream objects should catch up automatically. |
| `paused` | The cluster associated with the source has no compute/replica assigned (`replication_factor = 0`). See [Check for no compute](#check-for-no-compute). |
| `starting` | Wait for the source to transition to running. Downstream objects should catch up automatically.  |

## Investigating spikes

A spike in lag refers to a sudden increase in lag. Materialize retains wallclock
lag history for at least 30 days in
[`mz_internal.mz_wallclock_global_lag_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_history),
binned by minute. You can use this data to find past spikes and determine their
cause.

### Find spikes in lag

Identify objects that experienced the largest lag spikes over the last 7 days:

```mzsql
SELECT
    o.name,
    o.type,
    c.name AS cluster_name,
    max(wl.lag) AS peak_lag,
    min(wl.occurred_at) FILTER (WHERE wl.lag > INTERVAL '1 minute') AS earliest_spike_occurrence,
           -- earliest_spike_occurrence may differ from time of the peak_lag
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

You can exclude intentionally paused sources by adding `AND o.id NOT IN (...)`
to the `WHERE` clause.


### Determine spike scope

When a spike is identified, determine whether it affected a single object, a
single cluster, or the entire environment. The following query summarizes a time
window by cluster (substitute `<spike_start>`
and `<spike_end>` with your spike window):

```mzsql {hl_lines = "9"}
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
* **All objects on one cluster affected**: The cluster was overloaded or
  experienced a replica restart.
  - [Check the CPU/Memory pressure for the
    cluster](#check-the-cpu-or-memory-pressure) during that time window.
  - [Check for DDL or deploy activity](#check-for-ddl-or-deploy-activity) during
    that time window.

* **All clusters affected, including progress sources**: A system-level event
  occurred (environment restart, upgrade, or storage layer disruption). Lag that
  grows linearly at 1 minute per minute indicates frontiers were completely
  frozen for the duration of the event. These spikes are typically transient and
  self-resolving. If they recur frequently, check environment upgrade schedules and storage layer health.

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

### Check for DDL or deploy activity

Object creation, alteration, or deletion triggers rehydration, which can cause
transient freshness degradation.

When a decrease in freshness affects a single cluster but is not explained by
[CPU or memory pressure](#check-the-cpu-or-memory-pressure), check whether DDL
operations occurred during the spike window.
[`mz_catalog.mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events)
records all `CREATE`, `DROP`, and `ALTER` operations (substitute `<spike_start>`
and `<spike_end>` with your spike window):

```mzsql {hl_lines = "3"}
SELECT occurred_at, event_type, object_type, details
FROM mz_catalog.mz_audit_events
WHERE occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
  AND object_type IN ('materialized-view', 'index', 'sink', 'cluster', 'cluster-replica')
ORDER BY occurred_at
LIMIT 50;
```

Look for patterns such as:

* **Deploy clusters being created/dropped**: A blue-green deploy hydrating
  objects on a separate cluster can cause contention with live clusters. A
  typical symptom is all objects on the live cluster showing elevated wallclock
  lag (30 seconds to several minutes) for 10–20 minutes while source lag remains
  low. Use the [time-series correlation
  query](#distinguish-source-driven-vs-computation-driven-spikes) to confirm. If
  freshness degrades on objects that were *not* themselves modified during the
  deploy, [contact Support](/support/) with the spike time window and audit
  event output.

* **Sink `alter` events**: Sinks being swapped to point to new upstream MVs
  trigger reprocessing.

* **Bulk `drop`/`create` of MVs or indexes**: Mass DDL causes rehydration on the
  affected cluster.

### Distinguish source-driven vs. computation-driven spikes

`mz_materialization_lag` only shows the current breakdown of `local_lag` vs. `global_lag`.
To determine whether past spikes were caused by sources or by downstream computation, compare peak lag across clusters at each minute:

```mzsql
SELECT
    wl.occurred_at,
    max(wl.lag) FILTER (WHERE c.name = '<source_cluster>') AS source_peak_lag,
    max(wl.lag) FILTER (WHERE c.name = '<mv_cluster>') AS mv_peak_lag
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
LEFT JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
WHERE wl.occurred_at > now() - INTERVAL '7 days'
  AND o.id LIKE 'u%'
  AND c.name IN ('<source_cluster>', '<mv_cluster>')
  AND wl.lag IS NOT NULL
GROUP BY wl.occurred_at
HAVING max(wl.lag) FILTER (WHERE c.name = '<mv_cluster>') > INTERVAL '10 seconds'
ORDER BY wl.occurred_at DESC
LIMIT 30;
```

Interpreting the results:

* **`source_peak_lag` and `mv_peak_lag` move together**: the source is the bottleneck, and the MV cluster is inheriting its lag.
* **`source_peak_lag` stays low while `mv_peak_lag` spikes**: the MV cluster itself is falling behind, independent of its sources.
  This can happen during DDL operations, deploy events, or when the cluster is overloaded.


## Measuring aggregate freshness

This section provides queries to measure overall freshness across your deployment.


### Peak and threshold-based freshness

To count how many minutes exceed specific thresholds:

{{< tip >}}
You may want to exclude non-production (e.g., development/testing/staging) clusters that may produce misleading results.
{{< /tip >}}

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


### Cluster-level freshness summary

To get a per-cluster summary (useful for SLO reporting):

{{< tip >}}
You may want to exclude non-production (e.g., development/testing/staging) clusters that may produce misleading results.
{{< /tip >}}

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
  AND (ss.id IS NULL OR ss.status <> 'paused')   -- Exclude paused sources
  AND c.replication_factor > 0                   -- Exclude zero-replica clusters
GROUP BY c.name
ORDER BY max(wl.lag) DESC;
```

### Steady-state freshness analysis

To measure freshness outside of known events (deploys, restarts, upgrades),
exclude the specific time windows where spikes occurred rather than filtering by
lag threshold. Filtering by lag (e.g., excluding all minutes with lag above 1
minute) risks hiding genuine problems.

First, identify the spike windows using the [Find spikes in
lag](#find-spikes-in-lag). Then exclude those windows explicitly:

```mzsql
SELECT
    c.name AS cluster_name,
    count(DISTINCT wl.object_id) AS objects,
    max(wl.lag) AS peak_lag,
    avg(extract(epoch FROM wl.lag))::int || 's' AS avg_lag,
    count(*) FILTER (WHERE wl.lag > INTERVAL '5 seconds') AS minutes_above_5s,
    count(*) FILTER (WHERE wl.lag > INTERVAL '10 seconds') AS minutes_above_10s,
    count(*) FILTER (WHERE wl.lag > INTERVAL '30 seconds') AS minutes_above_30s,
    count(*) AS total_minutes
FROM mz_internal.mz_wallclock_global_lag_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
JOIN mz_catalog.mz_clusters c ON o.cluster_id = c.id
LEFT JOIN mz_internal.mz_source_statuses ss ON o.id = ss.id
WHERE wl.occurred_at > now() - INTERVAL '7 days'
  AND o.id LIKE 'u%'
  AND wl.lag IS NOT NULL
  AND (ss.id IS NULL OR ss.status <> 'paused')
  AND c.replication_factor > 0
  -- Exclude known event windows
  AND wl.occurred_at NOT BETWEEN '<event1_start>' AND '<event1_end>'
  AND wl.occurred_at NOT BETWEEN '<event2_start>' AND '<event2_end>'
GROUP BY c.name
ORDER BY max(wl.lag) DESC;
```

This gives an accurate picture of baseline freshness without masking unknown problems.