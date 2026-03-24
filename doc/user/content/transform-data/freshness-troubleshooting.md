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
a query. This guide can help you diagnose why freshness is degraded.

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
- [**Sink batch/commit intervals**](#check-sink-lag): A sink
  introduces lag due to batching and commit intervals.
- [**DDL or deploy activity**](#check-for-ddl-or-deploy-activity): A deploy or
  schema change triggers a rehydration.

## Check materialization lag

### Step 1. Find lagging object(s)

Query to see the current lag for all user objects:

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
- Lag on the order of mintues or hours typically indicates a problem.[^1]

To inspect an object's lag, see [Step
2](#step-2a-identify-materialization-lag-contributors).

[^1]: Some objects may report high lag without representing a real issue (e.g.,
    intentionally paused clusters). To exclude these, see [Filtering
    noise](#filtering-noise).

### Step 2a. Identify materialization lag contributors.

`mz_internal.mz_materialization_lag` breaks down lag for each materialization
(index, materialized view, sink) into two components:

* **`local_lag`**: the lag introduced by the object itself; i.e., how far behind
  the object is compared to its direct inputs.
* **`global_lag`**: the object's lag relative to its root inputs (sources and
  tables); i.e., how far behind the object is compared to its root inputs
  (sources and tables).

The difference between `global_lag` and `local_lag` is the lag
accumulated/inherited from the object's upstream dependencies.

Query
[`mz_internal.mz_materialization_lag`](/sql/system-catalog/mz_internal/#mz_materialization_lag)
to identify the lag contributors; replace the `<object_id>` with your object's
id:

```mzsql {hl_lines="12"}
SELECT
    o.id AS object_id,
    o.name,
    ml.local_lag,                                  -- lag introduced by the object itself
    ml.global_lag - ml.local_lag AS inherited_lag, -- lag inherited from dependencies
    ml.global_lag                                  -- total lag
FROM mz_internal.mz_materialization_lag ml
JOIN mz_catalog.mz_objects o ON ml.object_id = o.id
WHERE o.id = '<object_id>'
ORDER BY ml.global_lag DESC;
```

- To investigate a high `local_lag`, use [Dataflow
  troubleshooting](/transform-data/dataflow-troubleshooting/) to identify
  expensive operators for the object. If possible, optimize the query, resize
  the cluster, or move the object to a cluster with more resources.

- To investigate high `inherited_lag`, use [Check dependency
  lags](#step-2b-check-dependency-lags) to find which dependencies contributed
  the most delay and troubleshoot those objects.

### Step 2b: Check dependency lags

To find the lag contribution of an object's dependencies (replace `<object_id>`
with your object id):

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

Using the returned `from_id`, you can iterate Step 2a and Step 2b as needed. For
an object, use [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/) to identify
expensive operators for the object. If possible, optimize the query, resize the
cluster, or move the object to a cluster with more resources.

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

To resolve, scale the cluster up to a larger size([`ALTER CLUSTER ... SET (SIZE
= '<new size>')`](/sql/alter-cluster/)), and/or move enough objects to another
cluster to reduce load on the current cluster. If the pressure is caused by an
[inefficient/expensive object](#step-1-find-lagging-objects), optimize the query
if possible.

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

To resolve, scale the cluster up to a larger size([`ALTER CLUSTER ... SET (SIZE
= '<new size>')`](/sql/alter-cluster/)), and/or move enough objects to another
cluster to reduce load on the current cluster. If the pressure is caused by an
[inefficient/expensive object](#step-1-find-lagging-objects), optimize the query
if possible.

### Check for no compute

The cluster has `replication_factor = 0`, meaning no replicas (i.e., compute
resources) are assigned. On clusters without replicas, objects have frontiers
that are frozen, and their lag grows indefinitely.

{{< note >}}
- Zero-replica clusters are common for scheduled batch jobs (e.g., dbt snapshot
  runs) where replicas are scaled to zero between runs to save costs. High lag
  on these clusters is expected and does not indicate a problem.

- When measuring aggregate freshness, exclude intentionally paused sources to
  avoid skewing metrics.
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

A source with status `stalled`, `paused`, and `starting` will hold back all its
downstream objects. If the `status` for a source shows:

| Status   | Description |
| -------  | ----------- |
| `stalled`| Common causes include network partitions, credential expiration, and upstream database restarts. Check the returned `error` field and address appropriately. Once the source reconnects, downstream objects should catch up automatically. |
| `paused` | The cluster associated with the source has no compute/replica assigned (`replication_factor = 0`). See [Check for no compute](#check-for-no-compute). |
| `starting` | Wait for the source to transition to running. Downstream objects should catch up automatically.  |

## Check sink lag

Sinks often introduce lag due to batching and commit intervals.

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


## Check for DDL or deploy activity

Object creation, alteration, or deletion triggers rehydration, which can cause
transient freshness degradation.

### Correlate spikes with DDL events

When a spike affects a single cluster but is not explained by CPU or memory pressure, check whether DDL operations occurred during the spike window.
[`mz_catalog.mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events)
records all `CREATE`, `DROP`, and `ALTER` operations (substitute `<spike_start>`
and `<spike_end>` with your spike window):

```mzsql
SELECT occurred_at, event_type, object_type, details
FROM mz_catalog.mz_audit_events
WHERE occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
  AND object_type IN ('materialized-view', 'index', 'sink', 'cluster', 'cluster-replica')
ORDER BY occurred_at
LIMIT 50;
```

Look for patterns such as:

* **Deploy clusters being created/dropped**: a blue-green deploy hydrating objects on a separate cluster can cause contention with live clusters.
* **Sink `alter` events**: sinks being swapped to point to new upstream MVs trigger reprocessing.
* **Bulk `drop`/`create` of MVs or indexes**: mass DDL causes rehydration on the affected cluster.

### Check for deploy-related freshness degradation

**Symptoms**: All objects on a live cluster show elevated wallclock lag (30 seconds to several minutes) for 10–20 minutes.
Source lag remains low during the spike.
The spike coincides with DDL activity such as a deploy creating or altering objects.

**Diagnosis**: Check `mz_catalog.mz_audit_events` for DDL activity during the spike window:

```mzsql
SELECT occurred_at, event_type, object_type, details
FROM mz_catalog.mz_audit_events
WHERE occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
  AND event_type IN ('create', 'drop', 'alter')
  AND object_type IN ('cluster', 'cluster-replica', 'sink', 'materialized-view', 'index')
ORDER BY occurred_at
LIMIT 30;
```

Look for deploy clusters being created, sinks being altered, or bulk object creation/deletion that overlaps with the lag window.
Use the [time-series correlation query](#distinguish-source-driven-vs-computation-driven-spikes) to confirm that source lag stays low while the downstream cluster lags.

**Action**: If freshness degrades on objects that were not themselves modified during the deploy, file a support ticket.
Include the spike time window and the audit event output so the team can investigate the contention.

### Check for system-wide freshness spike

**Symptoms**: All objects across multiple clusters spike simultaneously.
Lag grows linearly at 1 minute per minute, then recovers.

**Diagnosis**: A system-level event froze all frontiers.
Common causes include environment restarts, version upgrades, and storage layer disruptions.
Use the [historical spike analysis](#determine-spike-scope) to confirm that all clusters were affected at the same time.

**Action**: These spikes are typically transient and self-resolving.
If they recur frequently, check environment upgrade schedules and storage layer health.

## Measuring aggregate freshness

The sections above diagnose individual objects.
To measure overall freshness across your deployment, for example, to answer "what is our P99.999 freshness?", aggregate over [`mz_internal.mz_wallclock_global_lag_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_history).

### Filtering noise

Raw aggregation over all objects produces misleading results because several categories of objects report high lag without representing a real freshness problem:

* **Paused sources**: A deliberately paused source (i.e., the source cluster has
  `replication_factor = 0`) stops ingesting data and its lag grows indefinitely.
  Filter these out by joining `mz_internal.mz_source_statuses` and excluding
  `status = 'paused'`.
* **Zero-replica clusters**: Clusters with `replication_factor = 0` have no
  compute assigned. If objects are on a cluster with no replias, their frontiers
  are frozen and lag grows linearly over time.
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

Materialize retains wallclock lag history for at least 30 days in [`mz_internal.mz_wallclock_global_lag_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_history), binned by minute.
Use this data to find past freshness spikes and determine their cause.

### Check historical trends.

To look at the historical trends for an object (such as if its lag has been
steadily growing, etc.), query
[`mz_internal.mz_wallclock_global_lag_recent_history`](/sql/system-catalog/mz_internal/#mz_wallclock_global_lag_recent_history),
replacing `<object_id>` with the ID of the object from the previous query:

```mzsql
SELECT o.name, wl.lag, wl.occurred_at
FROM mz_internal.mz_wallclock_global_lag_recent_history wl
JOIN mz_catalog.mz_objects o ON wl.object_id = o.id
WHERE o.id = '<object_id>'
ORDER BY wl.occurred_at DESC
LIMIT 50;
```



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

### Steady-state freshness analysis

To measure freshness outside of known events (deploys, restarts, upgrades), exclude the specific time windows where spikes occurred rather than filtering by lag threshold.
Filtering by lag (e.g., excluding all minutes with lag above 1 minute) risks hiding genuine problems.

First, identify the spike windows using the [spike analysis queries](#find-recent-spikes).
Then exclude those windows explicitly:

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
