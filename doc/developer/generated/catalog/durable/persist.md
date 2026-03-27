---
source: src/catalog/src/durable/persist.rs
revision: 92f868e8b5
---

# catalog::durable::persist

Implements `UnopenedPersistCatalogState` and `PersistCatalogState`, the persist-backed implementation of `DurableCatalogState`.
Manages a single persist shard ("catalog") using a write handle, a critical since handle for compaction, and a read/listen handle for tailing updates.
Handles epoch fencing (each opener writes a monotonically increasing `Epoch` to detect concurrent writers), catalog schema upgrades on open, and snapshotting vs. listening modes.
The fencing soft-assertion in `compare_and_append` was simplified to avoid redundant clones and clarify the fence-detection logic.
Provides `shard_id` (a deterministic mapping from environment ID to catalog shard ID) and `Timestamp` (a `u64` epoch-millisecond type).
