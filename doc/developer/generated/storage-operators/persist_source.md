---
source: src/storage-operators/src/persist_source.rs
revision: d943a9789f
---

# storage-operators::persist_source

Provides the `persist_source` Timely operator that reads from a persist shard and emits `(SourceData, (), Timestamp, StorageDiff)` updates.
It wraps the lower-level `shard_source` from `mz_persist_client`, adding storage-specific logic for MFP pushdown, stats-based part filtering, and txn-wal integration.
