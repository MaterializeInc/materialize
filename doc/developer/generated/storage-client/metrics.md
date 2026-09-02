---
source: src/storage-client/src/metrics.rs
revision: 1de0314a65
---

# storage-client::metrics

Provides Prometheus metrics for the storage controller, organized in a three-level hierarchy: `StorageControllerMetrics` (global), `InstanceMetrics` (per storage instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` and `ResponseMetrics` track per-message-type counters for `StorageCommand` and `StorageResponse`.
`HistoryMetrics` tracks the count of commands currently held in the controller's command history buffer.
`StorageControllerMetrics` no longer holds a `table_apply_seconds` histogram; the `mz_storage_controller_table_apply_seconds` metric was removed along with the deferred-apply optimization.
