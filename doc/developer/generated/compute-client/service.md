---
source: src/compute-client/src/service.rs
revision: 217c45ad34
---

# mz-compute-client::service

Defines `ComputeClient`, the trait alias for a `GenericClient` over compute commands and responses, and `PartitionedComputeState`, which merges responses from multiple partitioned workers into a single unified stream.
`PartitionedComputeState` tracks frontier state per collection (via `TrackedFrontiers`, which maintains `MutableAntichain`s plus per-shard antichains for write, input, and output frontiers), merges peek and copy-to responses once all shards have replied, and sequences subscribe batches by holding updates until their timestamps are complete.
Stashed subscribe updates are stored as `Vec<UpdateCollection>`; when the frontier advances, each collection is split at the frontier boundary and the prefix is shipped while the remainder is retained for future batches.
It implements the `PartitionedState` trait and is instantiated at both the controller–cluster boundary and within each cluster process to dispatch across timely workers. `Hello` and `UpdateConfiguration` commands are forwarded to all shards; all other commands go only to the first shard.
