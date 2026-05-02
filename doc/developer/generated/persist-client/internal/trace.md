---
source: src/persist-client/src/internal/trace.rs
revision: 530390d54e
---

# persist-client::internal::trace

Implements `Trace`, persist's fork of Differential Dataflow's `Spine`: an append-only, compactable collection of `HollowBatch` pointers organized in a leveled structure.
The Spine is adapted so that compaction is asynchronous: `FueledMergeReq` events are emitted when two spine levels are ready to merge, and `apply_merge_res` later substitutes the compacted result without blocking writes.
`SpineBatch` accumulates N input `HollowBatch`es to allow N-way compaction beyond the binary merges that the original Spine supports.
