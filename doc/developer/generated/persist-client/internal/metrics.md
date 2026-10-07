---
source: src/persist-client/src/internal/metrics.rs
revision: 988f05416f
---

# persist-client::internal::metrics

Defines the comprehensive `Metrics` struct (and numerous sub-structs) that instruments every major persist operation with Prometheus counters, gauges, and histograms.
Sub-structs cover blob/consensus operations, command evaluation, retries, batch reads/writes, compaction, GC, leasing, codecs, state updates, PubSub, MFP pushdown, consolidation, blob caching, tokio tasks, columnar encoding, schema operations, inline writes, the persist sink, fetch semaphore usage, and hedged blob gets (`blob_hedge: BlobHedgeMetrics` from `mz_persist::metrics`).
`CmdsMetrics` includes a `CmdMetrics` field for each state-machine command: `init_state`, `add_rollup`, `remove_rollups`, `upgrade_version`, `register`, `compare_and_append`, `compare_and_downgrade_since`, `downgrade_since`, `expire_reader`, `expire_writer`, `merge_res`, `become_tombstone`, `compare_and_evolve_schema`, and `spine_exert`; plus bare `IntCounter` fields for `compare_and_append_noop` and `fetch_upper_count`.
`MetricsBlob` and `MetricsConsensus` are decorator implementations of the `Blob` and `Consensus` traits that record latency and error metrics for all storage operations.
`ShardsMetrics` tracks per-shard state (since, upper, encoded size, batch/update counts) as labeled gauge vectors, with `ShardMetrics` representing an individual shard's metrics. `ShardsAggregateMetrics` uses `AggregatedFamilies` (from `mz_ore::metrics::aggregation`) to produce bounded summary aggregates (percentile and top-k series) of the per-shard families.
A `PerShardMetricsFilter` postprocessor is registered with the metrics registry at `Metrics::new` time. On each scrape it reads the `SHARD_METRICS` (`persist_shard_metrics`) dynamic config via `ShardMetricsExport::get` and removes the raw per-shard `mz_persist_shard_*` families from the gathered output when the mode does not include `PerShard`. The filter matches exact family names from `PER_SHARD_FAMILIES` rather than a name prefix so that the shard-count and summary-aggregate series sharing the prefix are unaffected. An `mz_persist_shard_metrics_mode_invalid` counter is incremented each scrape when the config value does not parse to a recognized mode.
`MetricsSemaphore` provides a metered wrapper around `tokio::sync::Semaphore` for tracking fetch concurrency.
