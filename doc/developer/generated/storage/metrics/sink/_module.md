---
source: src/storage/src/metrics/sink.rs
revision: 1db0a3421e
---

# mz-storage::metrics::sink

Aggregates sink metric definition structs into `SinkMetricDefs`, which bundles `KafkaSinkMetricDefs` and `IcebergSinkMetricDefs`.
Accessed through `StorageMetrics` to obtain per-sink instantiated metric handles.
