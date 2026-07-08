---
source: src/catalog/src/durable/persist.rs
revision: 584bb9030c
---

# catalog::durable::persist

Implements `UnopenedPersistCatalogState` and `PersistCatalogState`, the persist-backed implementation of `DurableCatalogState`.
Manages a single persist shard ("catalog") using a write handle, a critical since handle for compaction, and a read/listen handle for tailing updates.
The `Mode` enum (`Readonly`, `Savepoint`, `Writable`) controls the effect of mutable operations.
Handles epoch fencing via the `FenceableToken` state machine (states: `Initializing`, `Unfenced`, `Fenced`), which detects concurrent writers by comparing `FenceToken` values on each `compare_and_append`; fencing can be triggered by a higher deploy generation or a higher epoch.
The `ApplyUpdate<T>` trait defines per-update processing and filtering; `UnopenedCatalogStateInner` implements it for the pre-open phase (caching config and setting collections), and `CatalogStateInner` implements it for the post-open phase (buffering updates for higher layers and updating metrics).
Handles catalog schema upgrades on open. `open_inner` returns `Result<Box<dyn DurableCatalogState>, CatalogError>`. On open, audit log entries are partitioned out of the snapshot and dropped (not returned); they are counted for metrics and then discarded, since audit events are served from `mz_internal.mz_catalog_raw` via the `mz_audit_events` materialized view.
`PersistHandle` tracks a `size_at_last_consolidation` field and exposes `maybe_consolidate()`, which consolidates the snapshot when it has at least doubled in size since the last consolidation; this amortizes the O(N log N) consolidation cost so total work remains O(N log N) rather than O(K * N log N) for K timestamps. The `apply_updates_and_consolidate()` method is the typical entry point for applying a single batch with immediate consolidation.
Provides `shard_id` (a deterministic mapping from environment ID to catalog shard ID via SHA-256) and `Timestamp` (a module-private alias for `mz_repr::Timestamp`, a `u64`-backed epoch-millisecond type).
