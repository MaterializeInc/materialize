---
source: src/storage-operators/src/persist_source.rs
revision: 4d8deb2de7
---

# storage-operators::persist_source

Provides the `persist_source` Timely operator that reads from a persist shard and emits `(Row, Timestamp, Diff)` and error streams.
It wraps the lower-level `shard_source` from `mz_persist_client`, adding storage-specific logic for MFP pushdown, stats-based part filtering, and txn-wal integration.
`persist_source`, `persist_source_core`, and `decode_and_mfp` are generic over an error type `E` that must implement `From<DataflowError>` and `From<EvalError>` (in addition to `timely::ExchangeData + Ord + Clone + Debug`), allowing callers to choose how errors are represented on the output stream.
Defines `Subtime`, an opaque sub-timestamp token that enables finer-grained frontier progress within a single timestamp, used for granular backpressure flow control.
`Subtime` implements `differential_dataflow::lattice::Lattice` (join as max, meet as min) and `Maximum` (returning `u64::MAX`), allowing it to participate in differential dataflow lattice operations.
The `persist_source_core` function handles the inner scoped dataflow, wiring up `shard_source`, optional `txns_progress`, and the `decode_and_mfp` operator that deserializes `SourceData` and applies remaining MFP logic.
