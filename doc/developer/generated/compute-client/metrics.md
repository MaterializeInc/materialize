---
source: src/compute-client/src/metrics.rs
revision: 19608a8469
---

# mz-compute-client::metrics

Defines Prometheus metrics for the compute controller, organized into three layers: `ComputeControllerMetrics` (registry-level, shared across all instances), `InstanceMetrics` (per compute instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` and `ResponseMetrics` provide keyed metric sets for each command and response variant, respectively, and `PeekMetrics` tracks peek outcomes by result type.
`HistoryMetrics` tracks the size of the command history, and `ReplicaCollectionMetrics` holds wallclock-lag metrics for individual collections.
