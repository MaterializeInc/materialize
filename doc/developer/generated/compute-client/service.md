---
source: src/compute-client/src/service.rs
revision: 82e054569f
---

# mz-compute-client::service

Defines `ComputeClient`, the trait alias for a `GenericClient` over compute commands and responses, and `PartitionedComputeState`, which merges responses from multiple partitioned workers into a single unified stream.
`PartitionedComputeState` tracks frontier state per collection (via `TrackedFrontiers`, which maintains `MutableAntichain`s plus per-shard antichains for write, input, and output frontiers), merges peek and copy-to responses once all shards have replied, and sequences subscribe batches by holding updates until their timestamps are complete.
Per-peek accumulation is handled by the private `PendingPeek` struct, which holds the merged `PeekResponse` so far, the total inline byte count across all shards (tracked separately from the response so that rows already discarded by an error still count against the size limit), and the set of shards that have responded. The size check runs on every absorbed response rather than once at the end. `merge_peek_responses` no longer takes a `max_result_size` argument; size enforcement is `PendingPeek::absorb`'s responsibility. When two `Error` responses must be merged, `merge_peek_errors` is called: a `RowIterationLimitExceeded` error yields to any other error kind, and when two `RowIterationLimitExceeded` errors meet the smaller limit wins so the result is order-independent. Size-limit violations produce a structured `PeekError::ResultExceedsMaxSize { max_result_size }` rather than an unstructured string.
Stashed subscribe updates are stored as `Vec<UpdateCollection>`; when the frontier advances, each collection is split at the frontier boundary and the prefix is shipped while the remainder is retained for future batches.
It implements the `PartitionedState` trait and is instantiated at both the controller–cluster boundary and within each cluster process to dispatch across timely workers. `Hello` and `UpdateConfiguration` commands are forwarded to all shards; all other commands go only to the first shard.
