---
title: "Troubleshooting: Unresponsive queries"
description: "How to find out why a query in Materialize hangs or never returns, and how to cancel it."
menu:
  main:
    parent: "transform-troubleshooting"
    name: "Unresponsive queries"
    weight: 20
---

This guide helps you find out why a query hangs or doesn't return results, and
how to fix it.

A query that reads from an object that can't serve results yet waits until the
object is ready. This is how Materialize makes sure that every result is
consistent.

## Common causes

- **Snapshotting source**: A new source must read a snapshot of the existing
  upstream data before queries on it return.
- **Stalled source**: A source has stopped ingesting data, so its dependencies
  can't advance.
- **Hydrating objects**: After an index or materialized view is created, or its
  cluster restarts or is resized, Materialize rebuilds the object's state.
  Queries that read from it wait until *hydration* finishes.
- **Unhealthy cluster**: The cluster is out of memory and restarting, or its
  CPU is saturated.

If none of these causes applies, the query may be running, just slowly. See
[Slow queries](/serve-results/troubleshooting/slow-queries/).

## Diagnosing the issue

### Find running queries

List the queries that are still running, from the [statement
log](/serve-results/troubleshooting/#query-history):

```mzsql
SELECT
  a.execution_id,
  s.connection_id,
  a.cluster_name,
  now() - a.began_at AS running_for,
  left(a.sql, 60) AS sql
FROM mz_internal.mz_recent_activity_log AS a
JOIN mz_internal.mz_sessions AS s ON s.id = a.session_id
WHERE a.finished_at IS NULL
ORDER BY a.began_at;
```

```nofmt
             execution_id             | connection_id | cluster_name | running_for  |                             sql
--------------------------------------+---------------+--------------+--------------+--------------------------------------------------------------
 01a0ee6a-3442-7115-a3bb-f502ad3e5a30 |    1518224533 | quickstart   | 00:00:06.021 | SELECT count(*) FROM orders a, orders b WHERE a.amount + b.a
```

Note the `connection_id`. You need it to [cancel the
query](#cancel-the-query).

The statement log is sampled and written in batches, so a running query may
not appear here, especially if it started only a few seconds ago.

### Check for snapshotting sources

```mzsql
SELECT s.name, s.type, st.snapshot_committed
FROM mz_internal.mz_source_statistics AS st
JOIN mz_catalog.mz_sources AS s ON s.id = st.id
WHERE s.id LIKE 'u%'
  AND NOT st.snapshot_committed;
```

Any source in the result is still snapshotting, and queries that depend on it
wait until the snapshot completes.

### Check for stalled sources

```mzsql
SELECT name, type, status, error
FROM mz_internal.mz_source_statuses
WHERE id LIKE 'u%'
  AND status IN ('stalled', 'paused');
```

A source in the result isn't ingesting data. The `error` column shows why.

### Check for hydrating objects

```mzsql
SELECT o.name, o.type, r.name AS replica
FROM mz_internal.mz_hydration_statuses AS h
JOIN mz_catalog.mz_objects AS o ON o.id = h.object_id
JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
WHERE h.hydrated IS NOT TRUE;
```

Any object in the result is still hydrating on that replica. You can also see
hydration status on the object's workflow graph in the console: click
**Clusters**, select the cluster, select the object under **Materialized
Views** or **Indexes**, then open the **Workflow** tab.

### Check cluster health

Check whether the cluster's replicas restarted recently:

```mzsql
SELECT c.name AS cluster, rh.replica_name AS replica, h.status, h.reason, h.occurred_at
FROM mz_internal.mz_cluster_replica_status_history AS h
JOIN mz_internal.mz_cluster_replica_history AS rh ON rh.replica_id = h.replica_id
JOIN mz_catalog.mz_clusters AS c ON c.id = rh.cluster_id
WHERE h.occurred_at > now() - INTERVAL '1 day'
ORDER BY h.occurred_at DESC
LIMIT 10;
```

A status of `offline` with reason `oom-killed` means the replica ran out of
memory and restarted. Queries running on it restart from the beginning. If the
query itself caused the out-of-memory error, the replica restarts in a loop
until you [cancel the query](#cancel-the-query).

To check CPU and memory utilization, see [Check cluster
utilization](/serve-results/troubleshooting/slow-queries/#check-cluster-utilization).

## Resolution

### Cancel the query

Cancel a running query with
[`pg_cancel_backend`](/sql/functions/#pg_cancel_backend), using the
`connection_id` from [Find running queries](#find-running-queries):

```mzsql
SELECT pg_cancel_backend(1518224533);
```

The statement log then records the query with `finished_status = 'canceled'`.
The cluster can take a while to tear down the query's dataflow, so its CPU and
memory usage may stay high for some time after you cancel it.

### Wait for the snapshot or hydration

Snapshotting and hydration take time proportional to data volume and query
complexity. They finish on their own.

- To make snapshots and hydration faster, size up the cluster. See [Optimize
  hydration requirements](/clusters/optimize-hydration-requirements/).
- On Materialize Cloud, clusters also rehydrate after restarts during the
  [routine maintenance window](/releases/schedule/#cloud-upgrade-schedule).
- To learn more about hydration, see
  [Hydration](/fundamentals/concepts/hydration/).

### Fix the source

To fix a stalled source, see [Troubleshoot
ingestion](/ingest-data/troubleshooting/).

### Fix the cluster

- If your query caused the cluster to run out of memory or saturate its CPU,
  cancel it, then reduce its cost. See [Expensive
  queries](/serve-results/troubleshooting/expensive-queries/).
- If other work on the cluster caused it, wait for that work to finish, run
  the query on a different cluster, or [size up the
  cluster](/sql/alter-cluster/).
- To get notified before a cluster reaches its capacity, set up
  [alerting](/observability/cloud/alerting/#thresholds).
