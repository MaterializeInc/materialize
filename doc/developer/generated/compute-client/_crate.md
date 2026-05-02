---
source: src/compute-client/src/lib.rs
revision: 98758a945d
---

# mz-compute-client

Provides the public API for the compute layer, including the controller, compute protocol, logging configuration, metrics, and client abstractions.

Key modules:
* `controller` — top-level `ComputeController` managing instances and replicas
* `protocol` — `ComputeCommand`/`ComputeResponse` definitions, history compaction
* `service` — `ComputeClient` trait and `PartitionedComputeState` for multi-worker response merging
* `logging` — `LoggingConfig` and `LogVariant` definitions
* `metrics` — Prometheus metrics for controller, instances, and replicas
* `as_of_selection` — as-of timestamp selection during system initialization

Key dependencies: `mz-cluster-client`, `mz-compute-types`, `mz-expr`, `mz-persist-client`, `mz-repr`, `mz-service`, `mz-storage-client`.
Downstream consumers include `mz-controller` and `mz-clusterd`.
