---
source: src/persist-client/src/operators/shard_source.rs
revision: 161628c089
---

# persist-client::operators::shard_source

Implements the Timely dataflow `shard_source` operator, which reads from a persist shard and emits `(part, frontier)` pairs to downstream operators for decoding.
Parts are distributed across workers via Exchange, and stats-based pushdown (`FilterResult`) allows the operator to skip fetching parts that contain no matching rows.
The operator handles both snapshot catch-up and continuous listening, advancing the output frontier as the shard's upper moves forward.
In `shard_source_fetch`, when `BatchFetcher::fetch_leased_part` returns a missing-blob error, the operator calls `BatchFetcher::missing_blob_diagnostics` with the `LeasedReaderId` carried by the `ExchangeableBatchPart`, then forwards a diagnostic message to the `ErrorHandler` (which either halts the process or signals a supervisor to trigger a restart).
