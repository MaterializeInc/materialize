---
source: src/persist-client/src/internal/metrics.rs
revision: 7db395b8e4
---

# persist-client::internal::metrics

Defines the comprehensive `Metrics` struct (and numerous sub-structs) that instruments every major persist operation with Prometheus counters, gauges, and histograms.
Sub-structs cover blob/consensus operations, command evaluation, retries, batch reads/writes, compaction, GC, leasing, codecs, state updates, PubSub, MFP pushdown, consolidation, blob caching, tokio tasks, columnar encoding, schema operations, inline writes, the persist sink, and fetch semaphore usage.
`CmdsMetrics` includes an `upgrade_version` field (of type `CmdMetrics`) that instruments the `upgrade_version` state-machine command separately from the `remove_rollups` command it previously shared metrics with.
`MetricsBlob` and `MetricsConsensus` are decorator implementations of the `Blob` and `Consensus` traits that record latency and error metrics for all storage operations.
`ShardsMetrics` tracks per-shard state (since, upper, encoded size, batch/update counts) as labeled gauge vectors, with `ShardMetrics` representing an individual shard's metrics.
`MetricsSemaphore` provides a metered wrapper around `tokio::sync::Semaphore` for tracking fetch concurrency.
