---
source: src/storage/src/metrics/sink/iceberg.rs
revision: 146851842d
---

# mz-storage::metrics::sink::iceberg

Defines `IcebergSinkMetricDefs` and `IcebergSinkMetrics`, tracking data/delete files written, stashed rows, snapshot commits, commit failures and conflicts, and latency histograms for commits and writer closes, all labeled by `sink_id` and `worker_id`.
