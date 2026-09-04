---
source: src/persist/src/metrics.rs
revision: db15d3b2dc
---

# persist::metrics

Defines Prometheus metric structs for the storage backends and columnar encoding.
`BlobHedgeMetrics` tracks hedged blob get activity: `fired` (hedge requests sent), `won` (hedge leg completed first), `won_seconds` (end-to-end latency histogram for won hedges), `skipped_budget`/`skipped_concurrency`/`skipped_unavailable` (reasons a hedge was suppressed), `errors` (hedge-leg errors), `warm_errors` (warm-path liveness check failures), `armed` (1 if a sibling is available), and `rtt_latency` (most recent sibling round-trip latency).
`S3BlobMetrics` tracks individual S3 API call counts and timeout events (operation, attempt, connect, and read timeouts, plus per-operation counters and an error counter vector).
`ColumnarMetrics` bundles `ArrowMetrics` (structured-column operation counts, part-build timing, and concat-byte counts) and `ParquetMetrics` (encoded size, row-group counts, per-column compressed/uncompressed sizes, and elided-null-buffer counts) for tracking columnar encoding and decoding statistics.
