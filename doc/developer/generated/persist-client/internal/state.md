---
source: src/persist-client/src/internal/state.rs
revision: 1f5d5df538
---

# persist-client::internal::state

Defines the core persist state data model: `State` / `TypedState` (the full shard state at a given `SeqNo`), `StateCollections` (the mutable collections within state: trace, readers, writers, schemas), `HollowBatch` / `HollowBatchPart` / `HollowRun` (metadata-only references to blob data), and the GC/rollup configuration knobs.
State is parameterized over `(K, V, T, D)` codec types and tracks the shard's since and upper frontiers, registered reader/writer leases, and the compaction trace.
All state transitions are pure functions that return a new `State` value, enabling compare-and-set semantics against consensus.
`StateCollections::add_rollup` refuses to insert a rollup at a `SeqNo` below the minimum currently-kept rollup. This guard prevents a delayed retry of an indeterminate `add_rollup` commit from re-inserting an entry that GC has already physically removed, which would produce duplicate Insert/Delete events in the diff stream.
