---
source: src/compute/src/render/threshold.rs
revision: 52f2de096d
---

# mz-compute::render::threshold

Renders `ThresholdPlan` nodes, which filter out rows with negative or zero multiplicities to implement SQL `EXCEPT ALL` / `DISTINCT`-based deduplication.
`BasicThresholdPlan` arranges the collection and applies a reduce that retains only rows with positive count; the result is returned as an arranged `CollectionBundle`.
