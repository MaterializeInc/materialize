---
source: src/persist-client/src/operators/shard_source.rs
revision: 70f75e4f0e
---

# persist-client::operators::shard_source

Implements the Timely dataflow `shard_source` operator, which reads from a persist shard and emits `(part, frontier)` pairs to downstream operators for decoding.
Parts are distributed across workers via Exchange, and stats-based pushdown (`FilterResult`) allows the operator to skip fetching parts that contain no matching rows.
The operator handles both snapshot catch-up and continuous listening, advancing the output frontier as the shard's upper moves forward.
In `shard_source_fetch`, when `BatchFetcher::fetch_leased_part` returns a missing-blob error, the operator calls `BatchFetcher::missing_blob_diagnostics` with the `LeasedReaderId` carried by the `ExchangeableBatchPart`, then forwards a diagnostic message to the `ErrorHandler` (which either halts the process or signals a supervisor to trigger a restart).
Batch parts are fetched concurrently per worker, with the degree of concurrency controlled by the `SOURCE_FETCH_CONCURRENCY` dyncfg (default 1, preserving serial behavior); each concurrent fetch runs on its own `BatchFetcher` clone.
During hydration catch-up, while the output frontier has not yet reached the `replay_upper` observed at startup, the operator coalesces frontier downgrades until `SOURCE_HYDRATION_FRONTIER_COALESCE_BYTES` encoded bytes have been emitted, reducing the number of downstream arrangement passes during historical replay.
