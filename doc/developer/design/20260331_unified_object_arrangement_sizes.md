# Unified Per-Object Arrangement Sizes

- Associated: Console scalability improvements, Maintained Objects UI

## The Problem

The console needs per-object memory usage (arrangement sizes) for the Maintained Objects
list view, Cluster Details page, and Object Detail panel. Today, getting this data requires
querying `mz_arrangement_heap_size_raw` with `SET cluster`/`SET cluster_replica` session
variables, which targets the query to a specific compute replica.

This approach has critical scalability problems:

- **Hangs on busy clusters:** On a loaded 600cc cluster, the query takes 60+ seconds.
  On a 50cc at 185% memory, it never returns.
- **23/23 test failures:** The `largestMaintainedQueries` query (which uses this data)
  fails 100% of the time in scalability tests with a 30-second timeout.
- **Per-cluster overhead:** The console must issue separate queries per cluster/replica
  pair, each setting session variables. With multiple tabs open, this saturates the
  HTTP/1.1 connection pool (6 connections per domain).
- **Self-managed gap:** The cloud repo has a Prometheus HTTP endpoint
  (`/metrics/mz_compute`) that computes per-object arrangement sizes, but results flow
  to external Prometheus — not back into SQL. Self-managed customers have no access
  to this data at all.

## Success Criteria

1. Per-object arrangement memory is queryable from `mz_catalog_server` without session
   variables, returning results in <100ms regardless of cluster load.
2. `largestMaintainedQueries` failures drop from 23/23 to 0 in scalability tests.
3. `arrangementMemory` queries no longer hang on busy clusters.
4. Data is tagged by `replica_id`, enabling per-replica comparison.
5. Stale data from crashed/dropped replicas is cleaned up automatically on restart.
6. Works for both cloud and self-managed deployments.

## Out of Scope

- **Per-object CPU attribution.** CPU metrics remain replica-level only. This work enables
  memory-based cost attribution but not CPU-based.
- **Historical trending.** The unified collection reflects current state (differential),
  not timestamped snapshots. Historical arrangement size tracking would require a separate
  append-only collection with retention.
- **Per-subscribe observability.** Metrics for subscribe memory usage, lag, and update
  rates are valuable but deferred to a follow-up.
- **Quantization/batching.** Rounding arrangement sizes to reduce differential churn
  (e.g., nearest 10MB) is a performance optimization for later.
- **Data freshness guarantees.** If a replica is unresponsive, its data may be stale.
  Adding a `collection_timestamp` column is deferred to a follow-up.

## Solution Proposal

### Overview

Add a unified introspection source `mz_internal.mz_object_arrangement_sizes` backed by
a continuous `SUBSCRIBE` on each replica. This follows the established pattern used by
`mz_compute_error_counts_raw_unified` and `mz_compute_hydration_times`.

Each replica runs a subscribe that aggregates arrangement sizes per object from raw
per-operator logs. The introspection subscribe infrastructure automatically:
- Tags each row with `replica_id`
- Writes differential updates to a storage-managed collection
- Handles replica lifecycle (creation, crashes, reconnects)
- Defers deletion of old data until new subscribe produces output

The resulting collection is queryable from `mz_catalog_server` without session variables.

### Schema

```sql
mz_internal.mz_object_arrangement_sizes (
    replica_id  text NOT NULL,   -- prepended automatically by introspection subscribe infra
    object_id   text NOT NULL,   -- compute export ID (index or materialized view)
    size        int8             -- arrangement heap + batcher size in bytes
)
```

### Subscribe Query

Reuses the SQL from the existing `mz_introspection.mz_object_arrangement_sizes` view
(`src/catalog/src/builtin.rs:9131`):

```sql
SUBSCRIBE (
    SELECT
        ce.export_id AS object_id,
        (COALESCE(SUM(hs.size), 0) + COALESCE(SUM(bs.size), 0))::int8 AS size
    FROM mz_introspection.mz_compute_exports AS ce
    JOIN mz_introspection.mz_dataflow_operator_dataflows AS dod
        ON dod.dataflow_id = ce.dataflow_id
    LEFT JOIN (
        SELECT operator_id, COUNT(*) AS size
        FROM mz_introspection.mz_arrangement_heap_size_raw
        GROUP BY operator_id
    ) AS hs ON hs.operator_id = dod.id
    LEFT JOIN (
        SELECT operator_id, COUNT(*) AS size
        FROM mz_introspection.mz_arrangement_batcher_size_raw
        GROUP BY operator_id
    ) AS bs ON bs.operator_id = dod.id
    GROUP BY ce.export_id
)
```

This query is more complex than existing subscribes (4 JOINs vs single-table aggregation).
The trade-off is intentional: it pre-computes per-object totals on the replica, so the
console needs only a simple join against `mz_catalog_server`.

### Implementation

**Backend (9 changes across 6 files):**

| File | Change |
|------|--------|
| `src/storage-client/src/controller.rs:111` | Add `ComputeObjectArrangementSizes` to `IntrospectionType` enum |
| `src/pgrepr-consts/src/oid.rs:790` | Add `SOURCE_MZ_OBJECT_ARRANGEMENT_SIZES_OID = 17070` |
| `src/catalog/src/builtin.rs:8296` | Define `MZ_OBJECT_ARRANGEMENT_SIZES_UNIFIED` BuiltinSource |
| `src/catalog/src/builtin.rs:14284` | Register in `BUILTINS_STATIC` |
| `src/storage-controller/src/lib.rs:3817` | Add to `Differential` arm of `CollectionManagerKind` |
| `src/storage-controller/src/collection_mgmt.rs` | Add to 3 exhaustive match guards (lines ~693, ~1126, ~1303) |
| `src/adapter/src/coord/introspection.rs:567` | Add `SubscribeSpec` to `SUBSCRIBES` array |

**Startup cleanup task (3 changes across 2 files):**

| File | Change |
|------|--------|
| `src/adapter/src/coord.rs:346` | Add `ArrangementSizesPrune` Message variant |
| `src/adapter/src/coord/message_handler.rs` | Add handler + `prune_arrangement_sizes_on_startup` method |
| `src/adapter/src/coord.rs:3575` | Call cleanup at startup |

Follows the `storage_usage_prune` pattern (`coord.rs:4146`): snapshot the collection,
find rows where `replica_id` doesn't match any existing replica, retract them.

**Console (2 files):**

| File | Change |
|------|--------|
| `console/src/api/materialize/cluster/arrangementMemory.ts` | Remove session vars, join unified source |
| `console/src/api/materialize/cluster/largestMaintainedQueries.ts` | Same pattern |

### Known Pitfalls

| Pitfall | Impact | Mitigation |
|---------|--------|------------|
| **Dataflow memory overhead** | Subscribe adds resident memory to every replica | Monitor; query is proven (same as existing view). Deferred: per-subscribe metrics. |
| **Stale data on slow replicas** | Wrong sizes during OOM diagnosis | Intrinsic limitation. Deferred: `collection_timestamp` column. |
| **Broken cleanup on crash** | Old replica data lingers | Startup cleanup task prunes rows for non-existent replicas. |
| **Silent feature disable** | Data disappears if `ENABLE_INTROSPECTION_SUBSCRIBES = false` | Console shows "Waiting for data" skeleton instead of error. |
| **Differential churn** | Byte-level changes propagate through JOINs | Acceptable for MVP. Deferred: quantization. |
| **Multi-replica confusion** | Same object appears N times (once per replica) | Console filters by `replica_id`. Document in column comments. |

## Minimal Viable Prototype

The POC consists of:

1. **Backend:** Unified introspection source with startup cleanup (12 files modified)
2. **Console:** Updated queries removing session variables + empty-state fallback
3. **Tests:** Testdrive coverage (create object → verify in table → drop → verify cleanup)

**Validation plan:**
- `cargo check` passes (exhaustive matches catch missing wiring)
- Coordinator logs show `installing introspection subscribe (type_=ComputeObjectArrangementSizes)`
- `SELECT * FROM mz_internal.mz_object_arrangement_sizes` returns replica-tagged rows
- Scalability test: `largestMaintainedQueries` failures drop from 23/23 to 0

## Alternatives

### Alternative 1: Periodic Polling (mz_storage_usage_by_shard pattern)

Add a timer to the coordinator that iterates over clusters, runs the arrangement size
query per cluster with session variables, and writes results to a builtin table every 60s.

**Rejected because:** This runs one-off queries per cluster — the exact operation that
hangs on busy clusters today. A periodic `SELECT` against a loaded 600cc would face the
same 60s+ hangs. The introspection subscribe avoids this by installing a persistent
dataflow at replica creation time, before the cluster gets busy.

### Alternative 2: Expose via mz_cluster_prometheus_metrics (PR #35256)

Register arrangement sizes in compute's Prometheus registry and let PR #35256's scraper
surface them via `mz_cluster_prometheus_metrics`.

**Rejected because:** Arrangement sizes are not registered in compute's Prometheus registry.
Adding them would require compute-side changes to register per-object metrics (not just
process-level metrics). The introspection subscribe approach requires no compute-side changes.

### Alternative 3: Console-side caching with longer TTL

Cache arrangement size query results in the console with a 5-minute TTL, accepting stale
data to reduce query frequency.

**Rejected because:** This doesn't fix the fundamental problem — the first query still hangs.
A 5-minute TTL means the console shows no data for the first 60+ seconds on a busy cluster.

## Open questions

1. **Subscribe query complexity:** The 4-JOIN subscribe is more complex than existing
   subscribes (single-table aggregations). Should we simplify to two separate subscribes
   (one for heap, one for batcher) and aggregate in a view on top?

2. **OID allocation:** Using 17070 (next after current last = 17069). Need to confirm
   no concurrent PR has claimed this OID.

3. **Naming:** The unified source is named `mz_object_arrangement_sizes` in `mz_internal`
   schema. The existing view with the same name is in `mz_introspection` schema. Different
   schemas, so no conflict, but could cause confusion. Should we use a different name
   (e.g., `mz_object_arrangement_sizes_unified`)?
