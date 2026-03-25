---
source: src/storage-types/src/read_policy.rs
revision: 9c78bf3297
---

# storage-types::read_policy

Defines `ReadPolicy<T>`, an enum that drives compaction decisions for storage and compute collections.
Variants include `ValidFrom` (fixed frontier), `LagWriteFrontier` (a closure applied to the write frontier, e.g., `step_back` or `lag_writes_by`), `Multiple` (takes the minimum of several policies), and `NoPolicy` (no compaction yet requested).
`lag_writes_by` rounds the lagged frontier down to a configurable granularity to reduce capability churn.
