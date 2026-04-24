---
source: src/catalog/src/durable/persist.rs
revision: 31ebed7760
---

# catalog::durable::persist

Implements `UnopenedPersistCatalogState` and `PersistCatalogState`, the persist-backed implementation of `DurableCatalogState`.
Manages a single persist shard ("catalog") using a write handle, a critical since handle for compaction, and a read/listen handle for tailing updates.
Handles epoch fencing via the `FenceableToken` state machine (states: `Initializing`, `Unfenced`, `Fenced`), which detects concurrent writers by comparing `FenceToken` values on each `compare_and_append`; fencing can be triggered by a higher deploy generation or a higher epoch.
Handles catalog schema upgrades on open and snapshotting vs. listening modes.
Provides `shard_id` (a deterministic mapping from environment ID to catalog shard ID) and `Timestamp` (a module-private alias for `mz_repr::Timestamp`, a `u64`-backed epoch-millisecond type).
