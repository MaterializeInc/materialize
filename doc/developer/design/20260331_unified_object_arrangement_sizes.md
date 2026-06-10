# Unified Per-Object Arrangement Sizes

- Associated: Maintained Objects UI, [CNS-42](https://linear.app/materializeinc/issue/CNS-42/per-object-memory-arrangement-for-maintained-objects-in-the-console)

## The Problem

There is no way to query per-object arrangement memory across clusters without setting
session variables (`SET cluster`, `SET cluster_replica`) that target a specific replica.

This has three consequences:

- **Coupled to replica load.** The arrangement size views live in `mz_introspection` and
  execute on the target replica's compute resources. When a replica is under heavy load,
  these queries compete with its actual workload and can take 60+ seconds or time out.
- **No cross-cluster queries.** Getting arrangement sizes for all objects requires
  separate session-variable-scoped queries per cluster/replica pair.
- **Self-managed gap.** Cloud has a Prometheus endpoint that computes per-object sizes,
  but results flow to external Prometheus — not back into SQL. Self-managed deployments
  have no access to this data at all.

## Success Criteria

1. Per-object arrangement memory is queryable from `mz_catalog_server` without session
   variables — query latency is decoupled from target cluster load.
2. Each row is tagged by `replica_id`, enabling cross-replica and cross-cluster queries
   without session variable overhead.
3. A history table retains hourly snapshots for 7 days, enabling time-range queries
   (e.g., `WHERE collection_timestamp > now() - INTERVAL '24 hours'`).
4. Stale data is cleaned up automatically — the introspection subscribe infrastructure
   handles replica lifecycle at runtime via `deferred_write`, and a startup task prunes
   expired timestamps from the history table.
5. Works for both cloud and self-managed deployments with no external infrastructure
   dependencies.

## Out of Scope

- **Per-object CPU attribution.** The same introspection subscribe pattern could surface
  per-object CPU via `mz_scheduling_elapsed`, but this work focuses on arrangement
  memory only.
- **Compute-side changes.** No changes to the compute layer or its Prometheus registry.
  All new data is derived from existing introspection log sources.
- **Console UI changes.** This design covers the system catalog additions. Console query
  migration and UI work to consume the new tables is a separate effort.

## Solution Proposal

### Overview

Two new system catalog objects in `mz_internal`:

1. **`mz_object_arrangement_sizes`** — a unified introspection source (differential)
   that continuously reflects the current arrangement memory of every compute object
   across all replicas. Backed by a `SUBSCRIBE` dataflow installed on each replica at
   creation time, following the same pattern as `mz_compute_error_counts_raw_unified`
   and `mz_compute_hydration_times`.

2. **`mz_object_arrangement_size_history`** — an append-only builtin table that
   accumulates hourly snapshots of the live data with a `collection_timestamp`. A
   coordinator task periodically reads the live collection and writes timestamped rows.
   A startup pruning task enforces 7-day retention. Follows the `mz_storage_usage_by_shard`
   collection and pruning pattern.

Both are queryable from `mz_catalog_server` without session variables or cluster targeting.
The subscribe aggregates per-object arrangement sizes (heap + batcher) from the existing
`mz_arrangement_heap_size_raw` and `mz_arrangement_batcher_size_raw` introspection logs.
The introspection subscribe infrastructure automatically tags each row with `replica_id`,
writes differential updates to a storage-managed collection, and handles replica lifecycle
(creation, crashes, reconnects).

### Schema

**Live state (differential)** — one row per (replica, object), updated in place:

```sql
mz_internal.mz_object_arrangement_sizes (
    replica_id  text NOT NULL,   -- FK to mz_cluster_replicas.id
    object_id   text NOT NULL,   -- FK to mz_objects.id (index or materialized view)
    size        int8,            -- arrangement heap + batcher size in bytes (minimum 10MB due to quantization floor)
    -- Key: (replica_id, object_id)
)
```

Objects with no arrangements are absent from this table (not present with size=0).
The `replica_id` column is prepended automatically by the introspection subscribe
infrastructure. Includes both user and system compute exports.

**History (append-only)** — hourly snapshots for time-range queries:

```sql
mz_internal.mz_object_arrangement_size_history (
    replica_id            text NOT NULL,   -- FK to mz_cluster_replicas.id
    object_id             text NOT NULL,   -- FK to mz_objects.id
    size                  int8,
    collection_timestamp  timestamptz NOT NULL,  -- when this snapshot was taken
    -- Key: (replica_id, object_id, collection_timestamp)
)
```

Cluster attribution (including dropped replicas) is available by joining
`mz_cluster_replica_history` on `replica_id`.

The history table follows the `mz_storage_usage_by_shard` collection and pruning pattern:

- **Collection interval:** 1 hour (dyncfg: `arrangement_size_collection_interval`)
- **Retention period:** 7 days (dyncfg: `arrangement_size_retention_period`)

**Indexes:**

All indexes run on `mz_catalog_server` with single-column keys per `BuiltinIndex`
documentation (composite keys require all leading columns to match in Materialize).

| Index | On | Column | Primary query pattern |
|-------|----|--------|-----------------------|
| `mz_object_arrangement_sizes_ind` | Live table | `replica_id` | All objects on a replica |
| `mz_object_arrangement_size_history_object_ind` | History | `object_id` | Single object's memory trend |
| `mz_object_arrangement_size_history_ts_ind` | History | `collection_timestamp` | All objects within a time window |

**Usage examples:**

```sql
-- Current state for all user objects (live table)
SELECT object_id, replica_id, size
FROM mz_internal.mz_object_arrangement_sizes
WHERE object_id LIKE 'u%';

-- Historical memory for all objects in the last 6 hours (history table)
SELECT object_id, replica_id, size, collection_timestamp
FROM mz_internal.mz_object_arrangement_size_history
WHERE collection_timestamp > now() - INTERVAL '6 hours'
  AND object_id LIKE 'u%';
```

### Subscribe Query

The subscribe runs on each replica and aggregates arrangement memory per compute object.
The raw introspection logs (`mz_arrangement_heap_size_raw`, `mz_arrangement_batcher_size_raw`)
are differential collections where each byte of arrangement memory is represented as a
separate row keyed by `operator_id` — so `COUNT(*)` on these rows gives total bytes for
an operator. The query maps operators to dataflows via `mz_dataflow_addresses`
(`address[1]` = dataflow_id), then dataflows to export objects via
`mz_compute_exports`.

```sql
SUBSCRIBE (
    WITH
        operator_dataflows AS (
            SELECT addrs.id AS operator_id, addrs.address[1] AS dataflow_id
            FROM mz_introspection.mz_dataflow_addresses addrs
        ),
        raw_sizes AS (
            SELECT operator_id FROM mz_introspection.mz_arrangement_heap_size_raw
            UNION ALL
            SELECT operator_id FROM mz_introspection.mz_arrangement_batcher_size_raw
        )
    SELECT
        ce.export_id AS object_id,
        GREATEST(10485760, (COUNT(*) / 10485760 * 10485760))::int8 AS size
    FROM mz_introspection.mz_compute_exports AS ce
    JOIN operator_dataflows AS od ON od.dataflow_id = ce.dataflow_id
    JOIN raw_sizes AS rs ON rs.operator_id = od.operator_id
    GROUP BY ce.export_id
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1000)
)
```

The query uses `mz_dataflow_addresses` directly instead of the
`mz_dataflow_operator_dataflows` view to map operators to dataflows, avoiding unnecessary
joins. `mz_dataflow_addresses` and `mz_compute_exports` are the deduplicated (worker 0)
views over the underlying per-worker tables — operator→dataflow→export mappings are
identical across workers, so the single-worker views give the right shape without
explicit worker filters. `raw_sizes` intentionally reads the per-worker raw tables
without any worker filter so `COUNT(*)` aggregates bytes across all workers, giving
total replica memory per object. `GREATEST(10MB, rounded)` suppresses byte-level
differential churn while keeping small objects visible. 10MB was chosen as a starting
threshold — large enough to filter out allocation noise, small enough that
memory-significant objects remain distinguishable. `OPTIONS (AGGREGATE INPUT GROUP SIZE
= 1000)` is a plan hint that encourages the optimizer to pick a monotonic reduce plan;
it is not a memory bound (see Known Pitfalls).

### Implementation

#### Live table

A new `IntrospectionType::ComputeObjectArrangementSizes` variant in `mz-storage-client`
drives the wiring. The catalog defines a `BuiltinSource` with columns
(`replica_id`, `object_id`, `size`) and a `BuiltinIndex` on `replica_id`, both registered
in `BUILTINS_STATIC`. The storage controller adds the new type to the `Differential` arm
of `CollectionManagerKind` and its exhaustive match guards. The adapter adds the subscribe
query as a `SubscribeSpec` in `coord/introspection.rs`.

This follows the exact pattern of `mz_compute_error_counts_raw_unified` — new
`IntrospectionType` variant, `BuiltinSource`, storage controller wiring, `SubscribeSpec`.

#### History table

The catalog defines a `BuiltinTable` with columns (`replica_id`, `object_id`,
`size`, `collection_timestamp`) and two `BuiltinIndex` definitions
(`object_id`, `collection_timestamp`).

Two new dyncfgs control the collection behavior:
- `arrangement_size_collection_interval` (default: 1 hour)
- `arrangement_size_retention_period` (default: 7 days)

#### Periodic collection

Three new `Message` variants in `coord.rs`, following the storage usage collection
structure. Should use the direct group commit pattern from PR #35436 once landed,
rather than `catalog_transact_inner`:

1. `ArrangementSizesSchedule` — deterministic periodic timer that fires every
   collection interval, using a seeded offset to avoid thundering-herd across
   environments.
2. `ArrangementSizesSnapshot` — reads the live differential collection, writes
   timestamped rows to the history table.
3. `ArrangementSizesPrune` — retracts expired rows from the history table.

#### Startup cleanup

On startup, prune expired history rows where
`collection_timestamp < now - retention_period`. The live table does not need startup
cleanup — the introspection subscribe infrastructure handles replica lifecycle via
`deferred_write`.

### Known Pitfalls

- **Dataflow memory overhead.** Subscribe adds a persistent dataflow to every replica.
  Both the join arrangements and the reduce scale linearly with the number of operators
  on the replica — there is no per-object bound. `OPTIONS (AGGREGATE INPUT GROUP SIZE =
  1000)` is a plan hint that helps the optimizer pick a monotonic reduce plan but does
  not bound total memory.
- **Stale data on slow replicas.** If a replica is unresponsive, the live table retains
  its last-known sizes. Neither table provides direct staleness detection —
  `mz_cluster_replica_statuses` can serve as a proxy (`offline` replicas likely have
  stale data), but an `online` replica under heavy load could also lag.
- **Startup ordering.** The first history snapshot must be gated on subscribes having
  produced initial output (a new coordination mechanism, not yet implemented) to avoid
  recording empty state before subscribes hydrate.
- **Stale replica cleanup.** The introspection subscribe infrastructure handles replica
  drops and crashes at runtime via `deferred_write` — old data is retained until the
  reinstalled subscribe produces output, then retracted.
- **10MB quantization.** The `GREATEST` floor slightly overstates objects under 10MB to
  keep them visible. If this proves too coarse, the threshold can be lowered (e.g., 1MB)
  while still suppressing byte-level churn, or removed entirely at the cost of more
  frequent differential updates.
- **Hourly snapshot gaps.** The history table misses short memory spikes between
  collections. It may also miss entire replicas if they are created and dropped within
  a single collection interval — their data exists in the live table while they are
  alive but is retracted on drop and never captured in history. The collection interval
  is a dyncfg and can be lowered to reduce the window, but event-driven snapshotting
  (on replica create/drop) would be needed to fully eliminate this gap.
- **History table growth.** ~1.7M rows at scale (1000 objects × 10 replicas × 168
  snapshots). Bounded by 7-day retention with startup pruning.
- **Silent feature disable.** No data if `ENABLE_INTROSPECTION_SUBSCRIBES = false`.
  Repopulates automatically when re-enabled.

## POC

A prototype of the live table was built in
[PR #35848](https://github.com/MaterializeInc/materialize/pull/35848)
(commit `cba891c`). It validates that per-object arrangement sizes from all replicas
are queryable via `SELECT * FROM mz_internal.mz_object_arrangement_sizes` on
`mz_catalog_server` without session variables.

**What the POC does not cover (remaining work for this design):**
- History table (`mz_object_arrangement_size_history`)
- Periodic collection and pruning tasks
- System variables for collection interval and retention period
- `GREATEST` minimum floor on the quantization
- Startup pruning for expired history rows
- Testdrive coverage

## Validation Plan

Tests to be added as part of the full implementation:

- Testdrive: create MV → verify row appears in live table with correct `object_id` and
  non-zero `size` → drop MV → verify row is retracted
- Testdrive: verify `replica_id` matches `mz_cluster_replicas.id`
- Testdrive: verify history rows accumulate with correct `replica_id` and
  `collection_timestamp` after one collection interval
- Testdrive: verify startup pruning retracts history rows older than retention period

## Alternatives

### Periodic polling for live data

A coordinator timer queries each cluster with session variables every 60s and writes
results to a builtin table.

**Reasons not chosen:** This is the exact operation that hangs on busy clusters today — a
periodic `SELECT` against a loaded replica faces the same 60s+ hangs. The introspection
subscribe avoids this by installing a persistent dataflow at replica creation time. (The
periodic pattern *is* used for the history table, but there it reads from the
already-populated live collection rather than querying replicas directly.)

### Expose via mz_cluster_prometheus_metrics

Register arrangement sizes in compute's Prometheus registry and surface them via
`mz_cluster_prometheus_metrics` (PR #35256).

**Reasons not chosen:** Arrangement sizes are not currently in compute's Prometheus
registry. Once future metrics work adds them, Prometheus could become the preferred
path for this data. This design does not preclude that.

## Open questions

1. **Collection interval tuning.** The 1-hour default balances history granularity against
   table growth. Should this be shorter (e.g., 15 minutes) for environments that need
   finer-grained trending?

2. **Quantization threshold.** 10MB works for identifying memory-heavy objects. Should
   this be configurable or lowered to 1MB for environments with many small objects?
