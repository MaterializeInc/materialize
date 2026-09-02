---
source: src/compute/src/render/threshold.rs
revision: 1c55de49eb
---

# mz-compute::render::threshold

Renders `ThresholdPlan` nodes, which filter out rows with negative or zero multiplicities to implement SQL `EXCEPT ALL` / `DISTINCT`-based deduplication.
`BasicThresholdPlan` arranges the collection and applies a reduce that retains only rows with positive count; the result is returned as an arranged `CollectionBundle`.
The reduce step is split across two concrete helper functions: `threshold_local` operates on a dataflow-local `RowRowAgent` arrangement, and `threshold_trace` operates on an imported `RowRowEnter` arrangement. Both call `mz_reduce_abelian` with a concrete output spine type so that the compiler can resolve the higher-ranked key equality required by that method.
