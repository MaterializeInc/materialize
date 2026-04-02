---
source: src/compute-client/src/service.rs
revision: 2982634c0d
---

# mz-compute-client::service

Defines `ComputeClient`, the trait alias for a `GenericClient` over compute commands and responses, and `PartitionedComputeState`, which merges responses from multiple partitioned workers into a single unified stream.
`PartitionedComputeState` tracks frontier state per collection, merges peek and copy-to responses once all shards have replied, and sequences subscribe batches by holding updates until their timestamps are complete.
Stashed subscribe updates are stored as `Vec<UpdateCollection<T>>`; when the frontier advances, each collection is split at the frontier boundary and the prefix is shipped while the remainder is retained for future batches.
It implements the `PartitionedState` trait and is instantiated at both the controller–cluster boundary and within each cluster process to dispatch across timely workers.
