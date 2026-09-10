---
title: "Cluster memory spike troubleshooting"
description: "Find what caused a cluster's memory to spike, and how to prevent it recurring."
menu:
  main:
    name: "Troubleshoot memory spikes"
    identifier: cluster-memory-troubleshooting
    parent: "clusters"
    weight: 90
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

[`mz_internal.mz_cluster_replica_metrics_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics_history)
retains per-replica memory samples across restarts, so you can find a past
spike even if the replica has since recovered or restarted:

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

To check whether a replica is under pressure right now, query
[`mz_internal.mz_cluster_replica_metrics`](/sql/system-catalog/mz_internal/#mz_cluster_replica_metrics),
or, for sub-minute detail and high-water marks that survive a spike shorter
than the sampling interval, see [Replica resource
usage](/observability/replica-resource-usage/).

To confirm the replica actually restarted, rather than just spiked and
recovered on its own, check
[`mz_internal.mz_cluster_replica_status_history`](/sql/system-catalog/mz_internal/#mz_cluster_replica_status_history)
as described in [Check for OOM crash
loops](/transform-data/freshness-troubleshooting/#check-for-oom-crash-loops).

## Step 2: Match the spike to a cause

Cross-reference the spike window against these common causes.

### A new object hydrated on the cluster

Creating an index, materialized view, or source loads its full result set into
memory: [hydration](/fundamentals/concepts/hydration/). If the new object
landed on a cluster that already runs production workloads, its hydration
competes with that cluster's steady-state memory. Check
[`mz_catalog.mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events)
for `create` events around the spike window, as described in [Check for DDL or
deploy activity](/transform-data/freshness-troubleshooting/#check-for-ddl-or-deploy-activity).

Put new objects on their own cluster and cut over with a [blue/green
deployment](/developer-tools/dbt/blue-green-deployments/) instead of adding
them to an existing production cluster. See [Operational
guidelines](/clusters/operational-guidelines/) and [Hydration
strategies](/fundamentals/concepts/hydration/#hydration-strategies).

### An ad-hoc query stood up a dataflow

A query that isn't served by an existing index or materialized view creates a
new, temporary dataflow that must build its own arrangements from scratch, on
top of whatever the cluster already maintains. A query pattern that forces
this even though it looks selective, for example a `WHERE` clause using
`<> ALL (...)` instead of `NOT IN (...)`, can spike memory the same way a
hydrating materialized view would.

- Use `EXPLAIN PLAN FOR <query>` to check whether a query hits an existing
  index or builds new operators, then use [Dataflow
  troubleshooting](/transform-data/dataflow-troubleshooting/) to identify which
  dataflow or operator is consuming memory.
- Rewrite the query to reuse an existing index, or add one, rather than
  relying on ad-hoc plans for hot-path queries.

### Many concurrent SUBSCRIBEs restarted at once

Like an ad-hoc query, an active `SUBSCRIBE` runs as its own dataflow. A cluster
serving many concurrent subscribes (for example, one per connected UI session)
can multiply its restart cost: every subscribe re-establishes its own snapshot
when its replica comes back online, on top of the cluster rebuilding its
indexes. That combined restart cost can exceed what the same subscribes cost
at steady state, and, if it exceeds the replica's memory limit, produce a
restart loop that looks unrecoverable because every restart recreates the same
spike.

If you hit this loop:

- Check how many subscribes are active on the cluster and how quickly clients
  reconnect after a disconnect; a burst of simultaneous reconnects reproduces
  the same spike after every restart.
- Add jittered backoff to client reconnect logic so subscribes re-establish
  gradually instead of all at once.
- As a stopgap, moving the workload to a new cluster lets clients reconnect
  gradually against already-hydrated indexes, avoiding the simultaneous spike
  while you fix the reconnect pattern above.

### A version upgrade or blue/green cutover hydrated an undersized cluster

Materialize upgrades clusters by spinning up a new generation, hydrating it,
and cutting over once it's ready; a manual [blue/green
deployment](/developer-tools/dbt/blue-green-deployments/) follows the same
pattern. Hydrating the new generation costs at least as much memory as the
current one already uses, so a cluster sized only for its steady state can OOM
during the swap even though it runs fine day to day. Budget cluster size for
hydration, not just steady state; see [Hydration
considerations](/clusters/operational-guidelines/#hydration-considerations).

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
  arrangement for no benefit. Project down to only the columns you need.

Use [Dataflow troubleshooting: Why is Materialize using so much
memory?](/transform-data/dataflow-troubleshooting/#why-is-materialize-using-so-much-memory)
to find which arrangement is largest.

### Work is skewed across workers

An overloaded cluster can look like a memory spike if most of the data lands
on one worker instead of being spread evenly. See [Is work distributed equally
across
workers?](/transform-data/dataflow-troubleshooting/#is-work-distributed-equally-across-workers)

## Related pages

- [Hydration](/fundamentals/concepts/hydration/)
- [Dataflow troubleshooting](/transform-data/dataflow-troubleshooting/)
- [Freshness troubleshooting](/transform-data/freshness-troubleshooting/)
- [Replica resource usage](/observability/replica-resource-usage/)
- [Operational guidelines](/clusters/operational-guidelines/)
