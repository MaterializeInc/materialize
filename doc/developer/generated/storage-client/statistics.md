---
source: src/storage-client/src/statistics.rs
revision: db271c31b1
---

# storage-client::statistics

Defines the `SourceStatisticsUpdate` and `SinkStatisticsUpdate` structs that represent per-worker/per-replica statistics snapshots for the `mz_source_statistics_raw` and `mz_sink_statistics_raw` introspection tables.
Also provides `WebhookStatistics` (backed by atomic counters) for low-overhead in-process statistics collection from webhook sources, along with the `RelationDesc` constants for these tables.
Statistics updates are consolidated per worker before being sent back to the controller as part of `StorageResponse::StatisticsUpdates`.
