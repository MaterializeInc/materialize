---
source: src/persist-client/src/internal/metrics.rs
revision: 11e6a79394
---

# persist-client::internal::metrics

Defines the comprehensive `Metrics` struct (and numerous sub-structs) that instruments every major persist operation with Prometheus counters, gauges, and histograms.
Sub-structs cover blob/consensus operations, command evaluation, retries, batch reads/writes, compaction, GC, leasing, codecs, state updates, PubSub, MFP pushdown, consolidation, blob caching, tokio tasks, columnar encoding, schema operations, inline writes, the persist sink, fetch semaphore usage, and hedged blob gets (`blob_hedge: BlobHedgeMetrics` from `mz_persist::metrics`).
`CmdsMetrics` includes a `CmdMetrics` field for each state-machine command: `init_state`, `add_rollup`, `remove_rollups`, `upgrade_version`, `register`, `compare_and_append`, `compare_and_downgrade_since`, `downgrade_since`, `expire_reader`, `expire_writer`, `merge_res`, `become_tombstone`, `compare_and_evolve_schema`, and `spine_exert`; plus bare `IntCounter` fields for `compare_and_append_noop` and `fetch_upper_count`.
`MetricsBlob` and `MetricsConsensus` are decorator implementations of the `Blob` and `Consensus` traits that record latency and error metrics for all storage operations.
`ShardsMetrics` tracks per-shard state (since, upper, encoded size, batch/update counts) as labeled gauge vectors, with `ShardMetrics` representing an individual shard's metrics.
`MetricsSemaphore` provides a metered wrapper around `tokio::sync::Semaphore` for tracking fetch concurrency.
