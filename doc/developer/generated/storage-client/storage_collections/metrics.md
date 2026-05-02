---
source: src/storage-client/src/storage_collections/metrics.rs
revision: 844ad57e4b
---

# storage-client::storage_collections::metrics

Provides `StorageCollectionsMetrics` with Prometheus gauges and counters tracking persist shard finalization progress (outstanding, pending-commit, started, succeeded, failed).
Also provides `ShardIdSet`, a `Mutex`-guarded `BTreeSet<ShardId>` that automatically updates a `UIntGauge` whenever the set is mutated via its `ShardIdSetGuard`.
