---
source: src/persist-client/src/internal/trace.rs
revision: 6ebd534824
---

# persist-client::internal::trace

Implements `Trace`, persist's fork of Differential Dataflow's `Spine`: an append-only, compactable collection of `HollowBatch` pointers organized in a leveled structure.
The Spine is adapted so that compaction is asynchronous: `FueledMergeReq` events are emitted when two spine levels are ready to merge, and `apply_merge_res` later substitutes the compacted result without blocking writes.
`SpineBatch` accumulates N input `HollowBatch`es to allow N-way compaction beyond the binary merges that the original Spine supports.
`apply_merge_res` computes the min/max replacement range using `itertools::minmax()` over an iterator of indices rather than collecting into a `BTreeSet` and calling `.iter().min()`/`.iter().max()`.
`Trace::unflatten` decodes from an untrusted blob and enforces several bounds as hard decode errors rather than panics: the total logical length of all batches is capped at `MAX_TOTAL_LEN` (to prevent overflow in spine maintenance arithmetic), the spine level count is capped at `MAX_LEVELS` (256), and each spine batch's parts must tile its id range contiguously (so that downstream `apply_merge_res_checked` assertions cannot be reached with malformed input). Legacy batches pushed into a reconstructed spine are also validated to have non-empty time ranges and to match the trace's current upper.
`SpineBatch::batch_diffs_sum` computes the total `diffs_sum` across all runs of a `HollowBatch`, but returns `None` for any run where `RunMeta::bounds_truncated()` is true: those runs' parts may physically hold updates outside the registered desc, so their write-time statistics cannot be compared against data seen through a read. `diffs_sum_for_runs` applies the same guard when computing the sum for a subset of runs identified by `RunId`.
