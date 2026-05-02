---
source: src/timestamp-oracle/src/metrics.rs
revision: 59539c1c0e
---

# mz-timestamp-oracle::metrics

Defines `Metrics` (the top-level Prometheus registry struct), `OracleMetrics` (per-operation counters/histograms for the core oracle operations), `BatchingMetrics` (tracks batching efficiency), and `RetriesMetrics` (retry attempt counts).
`MetricsRetryStream` wraps a `RetryStream` and instruments each retry attempt with the corresponding `RetriesMetrics` counter.
All metric structs are constructed once at startup and shared via `Arc`.
