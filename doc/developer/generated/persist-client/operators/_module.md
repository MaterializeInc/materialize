---
source: src/persist-client/src/operators/shard_source.rs
revision: 161628c089
---

# persist-client::operators

Provides Timely dataflow operators that integrate persist shards into streaming dataflow pipelines.
Currently contains one operator, `shard_source`, which reads a shard and feeds decoded batch parts into downstream dataflow operators while advancing the output frontier in lockstep with the shard's upper.
When a blob fetch fails in `shard_source_fetch`, the operator calls `BatchFetcher::missing_blob_diagnostics` to inspect whether the minting reader's lease is still present in state, then reports the error through the supplied `ErrorHandler` rather than panicking directly.
