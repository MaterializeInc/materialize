---
source: src/catalog/src/durable/persist.rs
revision: 0c926b489c
---

# catalog::durable::persist

Implements `UnopenedPersistCatalogState` and `PersistCatalogState`, the persist-backed implementation of `DurableCatalogState`.
Manages a single persist shard ("catalog") using a write handle, a critical since handle for compaction, and a read/listen handle for tailing updates.
Handles epoch fencing via the `FenceableToken` state machine (states: `Initializing`, `Unfenced`, `Fenced`), which detects concurrent writers by comparing `FenceToken` values on each `compare_and_append`; fencing can be triggered by a higher deploy generation or a higher epoch.
Handles catalog schema upgrades on open and snapshotting vs. listening modes.
`PersistHandle` tracks a `size_at_last_consolidation` field and exposes `maybe_consolidate()`, which consolidates the snapshot when it has at least doubled in size since the last consolidation; this amortizes the O(N log N) consolidation cost so total work remains O(N log N) rather than O(K * N log N) for K timestamps.
Provides `shard_id` (a deterministic mapping from environment ID to catalog shard ID) and `Timestamp` (a module-private alias for `mz_repr::Timestamp`, a `u64`-backed epoch-millisecond type).
