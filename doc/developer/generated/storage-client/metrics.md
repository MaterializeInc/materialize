---
source: src/storage-client/src/metrics.rs
revision: 00cc513fa5
---

# storage-client::metrics

Provides Prometheus metrics for the storage controller, organized in a three-level hierarchy: `StorageControllerMetrics` (global), `InstanceMetrics` (per storage instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` and `ResponseMetrics` track per-message-type counters for `StorageCommand` and `StorageResponse`.
`HistoryMetrics` tracks the count of commands currently held in the controller's command history buffer.
