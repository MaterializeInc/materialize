---
source: src/storage-client/src/metrics.rs
revision: c8a90af6a2
---

# storage-client::metrics

Provides Prometheus metrics for the storage controller, organized in a three-level hierarchy: `StorageControllerMetrics` (global), `InstanceMetrics` (per storage instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` and `ResponseMetrics` track per-message-type counters for `StorageCommand` and `StorageResponse`.
`HistoryMetrics` tracks the count of commands currently held in the controller's command history buffer.
All metric vectors carry `Rule`s (`instance_name_rule` and `replica_name_rule`) so the public scrape endpoint can attach `instance_name` and `replica_name` labels via `ClusterNameLookup` and `ReplicaNameLookup`.
