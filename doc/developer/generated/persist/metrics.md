---
source: src/persist/src/metrics.rs
revision: 8ce656db17
---

# persist::metrics

Defines Prometheus metric structs for the storage backends: `S3BlobMetrics` tracks individual S3 API call counts and timeout events, and `ColumnarMetrics` (plus `ParquetColumnMetrics`) tracks columnar encoding statistics and lgalloc memory usage.
`S3BlobMetrics` also carries `LgBytesMetrics` for all lgalloc regions used by the S3 and Azure backends.
