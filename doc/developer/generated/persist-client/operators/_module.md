---
source: src/persist-client/src/operators/shard_source.rs
revision: 70f75e4f0e
---

# persist-client::operators

Provides Timely dataflow operators that integrate persist shards into streaming dataflow pipelines.
Currently contains one operator, `shard_source`, which reads a shard and feeds decoded batch parts into downstream dataflow operators while advancing the output frontier in lockstep with the shard's upper.
When a blob fetch fails in `shard_source_fetch`, the operator calls `BatchFetcher::missing_blob_diagnostics` to inspect whether the minting reader's lease is still present in state, then reports the error through the supplied `ErrorHandler` rather than panicking directly.
Batch parts are now fetched concurrently per worker, with the degree of concurrency controlled by the `SOURCE_FETCH_CONCURRENCY` dyncfg (default 1, preserving serial behavior).
During hydration catch-up, while the output frontier has not yet reached the `replay_upper` observed at startup, the operator coalesces frontier downgrades until `SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES` encoded bytes have been emitted, reducing the number of downstream arrangement passes during historical replay.
