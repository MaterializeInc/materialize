---
source: src/persist/src/metrics.rs
revision: b89a9e0ec5
---

# persist::metrics

Defines Prometheus metric structs for the storage backends and columnar encoding.
`S3BlobMetrics` tracks individual S3 API call counts and timeout events (operation, attempt, connect, and read timeouts, plus per-operation counters and an error counter vector).
`ColumnarMetrics` bundles `ArrowMetrics` (structured-column operation counts, part-build timing, and concat-byte counts) and `ParquetMetrics` (encoded size, row-group counts, per-column compressed/uncompressed sizes, and elided-null-buffer counts) for tracking columnar encoding and decoding statistics.
