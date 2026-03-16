---
source: src/compute/src/arrangement.rs
revision: 82048ca795
---

# mz-compute::arrangement

Provides the `TraceManager` and related types for managing differential dataflow arrangements within a compute worker.
`TraceManager` maintains a map from `GlobalId` to `TraceBundle` and triggers periodic physical compaction of arrangement batches.
`PaddedTrace` enables safe recycling of logging arrangement traces across client reconnects without recreating the underlying dataflow.
