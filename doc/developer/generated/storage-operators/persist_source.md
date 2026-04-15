---
source: src/storage-operators/src/persist_source.rs
revision: b0fa98e931
---

# storage-operators::persist_source

Provides the `persist_source` Timely operator that reads from a persist shard and emits `(Row, Timestamp, Diff)` and `(DataflowError, Timestamp, Diff)` streams.
It wraps the lower-level `shard_source` from `mz_persist_client`, adding storage-specific logic for MFP pushdown, stats-based part filtering, and txn-wal integration.
Defines `Subtime`, an opaque sub-timestamp token that enables finer-grained frontier progress within a single timestamp, used for granular backpressure flow control.
The `persist_source_core` function handles the inner scoped dataflow, wiring up `shard_source`, optional `txns_progress`, and the `decode_and_mfp` operator that deserializes `SourceData` and applies remaining MFP logic.
