---
source: src/storage-client/src/metrics.rs
revision: 6c590b0a3c
---

# storage-client::metrics

Provides Prometheus metrics for the storage controller, organized in a three-level hierarchy: `StorageControllerMetrics` (global), `InstanceMetrics` (per storage instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` and `ResponseMetrics` track per-message-type counters for `StorageCommand` and `StorageResponse`.
`HistoryMetrics` tracks the count of commands currently held in the controller's command history buffer.
`StorageControllerMetrics` also holds a `table_apply_seconds` histogram (`mz_storage_controller_table_apply_seconds`) that measures the latency of applying a durable table append to its data shards; this apply happens off the write critical path, after the append response has been returned to the caller.
