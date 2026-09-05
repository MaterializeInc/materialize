---
source: src/compute/src/arrangement/manager.rs
revision: 24e1cba0d4
---

# mz-compute::arrangement::manager

Defines `TraceManager`, which maps `GlobalId`s to `TraceBundle`s and drives arrangement maintenance (physical batch merging, logical compaction).
`TraceBundle` pairs an `oks` trace (successful rows as `RowRowAgent`) with an `errs` trace (dataflow errors as `ErrAgent`), optionally holding lifetime tokens for associated resources. The `oks_errs_mut` method returns mutable references to both traces simultaneously, which callers such as peek error scans require when they must borrow both at once.
`PaddedTrace` wraps any `TraceReader` and synthesizes empty data for times between its `padded_since` and the inner trace's logical compaction frontier, used during reconciliation to reset logging collections without restarting the dataflow.
