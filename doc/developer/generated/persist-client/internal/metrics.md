---
source: src/persist-client/src/internal/metrics.rs
revision: f8e54dea92
---

# persist-client::internal::metrics

Defines the comprehensive `Metrics` struct (and associated sub-structs) that instruments every major persist operation with Prometheus counters, gauges, and histograms.
Also provides `MetricsBlob` and `MetricsConsensus`, decorator implementations of the `Blob` and `Consensus` traits that record latency and error metrics for all storage operations.
`ShardMetrics` tracks per-shard state (since, upper, batch counts) as labeled gauge vectors.
