---
title: "Cluster memory spike troubleshooting"
description: "Find what caused a cluster's memory to spike, and how to prevent it recurring."
menu:
  main:
    name: "Memory spikes"
    identifier: cluster-memory-troubleshooting
    parent: "troubleshoot-clusters"
    weight: 10
---

A memory spike is a sudden increase in a cluster replica's memory usage. A
spike that outgrows the replica's [heap
limit](/observability/replica-resource-usage/) makes Materialize spill to
disk, which slows the cluster down, or triggers an out-of-memory (OOM) kill and
replica restart. This guide helps you find what caused a spike and how to
prevent it recurring.

For sustained CPU/memory pressure and OOM crash loops on an undersized
cluster, see [Check cluster
health](/transform-data/freshness-troubleshooting/#check-cluster-health). This
guide focuses on **spikes**: sudden increases against an otherwise healthy
baseline.

## Step 1: Find when the spike happened

### Check historical spike

[`mz_internal.mz_cluster_replica_metrics_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics_history)
retains per-replica memory samples across restarts (at least 30 days by
default), so you can find a past spike even if the replica has since
recovered or restarted:

```mzsql
SELECT
    c.name AS cluster_name,
    r.name AS replica_name,
    h.process_id,
    max(h.heap_bytes) AS peak_heap_bytes,
    max(h.heap_limit) AS heap_limit_bytes,
    min(h.occurred_at) FILTER (WHERE h.heap_bytes > 0.9 * h.heap_limit) AS first_above_90pct
FROM mz_internal.mz_cluster_replica_metrics_history h
JOIN mz_catalog.mz_cluster_replicas r ON r.id = h.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
WHERE h.occurred_at > now() - INTERVAL '7 days'
GROUP BY c.name, r.name, h.process_id
HAVING max(h.heap_bytes) > 0.9 * max(h.heap_limit)
ORDER BY peak_heap_bytes DESC;
```
```
 cluster_name | replica_name | process_id | peak_heap_bytes | heap_limit_bytes |   first_above_90pct
--------------+--------------+------------+-----------------+-------------------+------------------------
 prod_compute | r1           |          0 |      15461882265 |       17179869184 | 2026-09-08 15:49:03+00
(1 row)
```

### Check a live spike

To check whether a replica is under memory pressure right now, query
[`mz_internal.mz_cluster_replica_metrics`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics):

```mzsql
SELECT
    c.name AS cluster_name,
    r.name AS replica_name,
    m.process_id,
    m.heap_bytes,
    m.heap_limit,
    round(100 * m.heap_bytes::numeric / m.heap_limit, 1) AS heap_pct
FROM mz_internal.mz_cluster_replica_metrics m
JOIN mz_catalog.mz_cluster_replicas r ON r.id = m.replica_id
JOIN mz_catalog.mz_clusters c ON c.id = r.cluster_id
ORDER BY heap_pct DESC;
```
```
 cluster_name | replica_name | process_id | heap_bytes  |  heap_limit | heap_pct
--------------+--------------+------------+-------------+-------------+----------
 prod_compute | r1           |          0 | 15461882265 | 17179869184 |     90.0
 prod_sources | r1           |          0 |  3221225472 | 17179869184 |     18.8
(2 rows)
```

For sub-minute detail and high-water marks that survive a spike shorter than
the sampling interval, see [Replica resource
usage](/observability/replica-resource-usage/).

## Step 2: Match the spike to a cause

Cross-reference the spike window against these common causes.

### A new object hydrated on the cluster

Creating an index, materialized view, or source loads its full result set into
memory: [hydration](/fundamentals/concepts/hydration/). If the new object
landed on a cluster that already runs production workloads, its hydration
competes with that cluster's steady-state memory.

Check [`mz_catalog.mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events)
for `create` events on the cluster around the spike window:

```mzsql
SELECT occurred_at, event_type, object_type, details
FROM mz_catalog.mz_audit_events
WHERE occurred_at BETWEEN '<spike_start>' AND '<spike_end>'
  AND object_type IN ('index', 'materialized-view', 'source')
ORDER BY occurred_at;
```

See also [Check for DDL or deploy
activity](/transform-data/freshness-troubleshooting/#check-for-ddl-or-deploy-activity).

**Resolution**: Put new objects on their own cluster and cut over with a
[blue/green deployment](/developer-tools/dbt/blue-green-deployments/) instead
of adding them to an existing production cluster. See [Operational
guidelines](/clusters/operational-guidelines/) and [Hydration
strategies](/fundamentals/concepts/hydration/#hydration-strategies).

### An ad-hoc query stood up a dataflow

If a query cannot be served by an existing index or materialized view, a new
dataflow is created to serve it. This is similar to hydrating a new index; it
causes memory usage to increase. See [Dataflows: mental model and basic
terminology](/transform-data/dataflow-troubleshooting/#dataflows-mental-model-and-basic-terminology)
for background.

Use `EXPLAIN PLAN FOR <query>` to check whether a query hits an existing index
or builds new operators, then use [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/) to identify which
dataflow or operator is consuming memory.

**Resolution**: Rewrite the query to reuse an existing index, or add one,
rather than relying on ad-hoc plans for hot-path queries. Some query patterns
force a new dataflow even though they look selective, for example a `WHERE`
clause using `<> ALL (...)` instead of `NOT IN (...)`.

### Many concurrent SUBSCRIBEs restarted at once

Like an ad-hoc query, an active `SUBSCRIBE` runs as its own dataflow. A cluster
serving many concurrent subscribes (for example, one per connected UI session)
can multiply its restart cost: every subscribe re-establishes its own snapshot
when its replica comes back online, on top of the cluster rebuilding its
indexes. That combined restart cost can exceed what the same subscribes cost
at steady state, and, if it exceeds the replica's memory limit, produce a
restart loop that looks unrecoverable because every restart recreates the same
spike.

Check how many subscribes are active on a cluster, using
[`mz_internal.mz_subscriptions`](/sql/system-catalog/mz_internal/#mz_subscriptions):

```mzsql
SELECT count(*) AS active_subscribes
FROM mz_internal.mz_subscriptions s
JOIN mz_catalog.mz_clusters c ON c.id = s.cluster_id
WHERE c.name = '<cluster_name>';
```

**Resolution**:
- Add jittered backoff to client reconnect logic so subscribes re-establish
  gradually instead of all at once.
- As a stopgap, moving the workload to a new cluster lets clients reconnect
  gradually against already-hydrated indexes, avoiding the simultaneous spike
  while you fix the reconnect pattern above.

### The working set outgrew memory

If a cluster's data no longer fits in memory, Materialize spills arrangements
to its scratch disk, which shows up as elevated `disk_bytes` on
`mz_cluster_replica_metrics` alongside memory near the limit, rather than an
outright OOM. Two common contributors:

- **Compaction state.** The same source data can require more memory to
  rehydrate on one replica than another if their upstream collections are
  compacted to different degrees; a freshly seeded environment can rehydrate
  cheaper than a long-running one that has retained more history.
- **Unnecessarily wide objects.** An index or materialized view that carries
  columns no downstream query uses still arranges them, growing the
  arrangement for no benefit.

**Resolution**: Project objects down to only the columns downstream queries
need, and use [Dataflow troubleshooting: Why is Materialize using so much
memory?](/transform-data/dataflow-troubleshooting/#why-is-materialize-using-so-much-memory)
to find which arrangement is largest.

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Dataflow troubleshooting](/transform-data/dataflow-troubleshooting/)
- [Freshness troubleshooting](/transform-data/freshness-troubleshooting/)
- [Replica resource usage](/observability/replica-resource-usage/)
- [Operational guidelines](/clusters/operational-guidelines/)
