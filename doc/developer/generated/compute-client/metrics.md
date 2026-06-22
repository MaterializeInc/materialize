---
source: src/compute-client/src/metrics.rs
revision: 82dac06732
---

# mz-compute-client::metrics

Defines Prometheus metrics for the compute controller, organized into three layers: `ComputeControllerMetrics` (registry-level, shared across all instances), `InstanceMetrics` (per compute instance), and `ReplicaMetrics` (per replica).
`CommandMetrics` provides a keyed metric set for each `ComputeCommand` variant (including `Hello`, `CreateInstance`, `CreateDataflow`, `Schedule`, `AllowCompaction`, `Peek`, `CancelPeek`, `InitializationComplete`, `UpdateConfiguration`, and `AllowWrites`). `ResponseMetrics` (private) provides keyed metrics per `ComputeResponse` variant. `PeekMetrics` tracks peek outcomes by result type (rows, rows_stashed, error, canceled).
`HistoryMetrics` tracks command and dataflow counts in the command history. `ReplicaCollectionMetrics` holds wallclock-lag metrics for individual non-transient collections.
Metrics carry `instance_id` and/or `replica_id` labels for identifying the compute instance and replica.
