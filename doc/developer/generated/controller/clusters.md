---
source: src/controller/src/clusters.rs
revision: 0a7c37cd69
---

# controller::clusters

Implements cluster and replica lifecycle management on `Controller`: creating, configuring, and dropping clusters and their replicas via the orchestrator.

Defines the key configuration and location types — `ClusterConfig`, `ReplicaConfig`, `ReplicaAllocation`, `ReplicaLocation` (`Managed` / `Unmanaged`), `ManagedReplicaLocation`, and `ClusterEvent` — as well as `ReplicaServiceName`, which encodes `(ClusterId, ReplicaId, generation)` as an orchestrator service-name string.
`provision_replica` calls `NamespacedOrchestrator::ensure_service` with a fully constructed `ServiceConfig` (including Timely addresses, resource limits, labels, and availability zones) and spawns a background task that polls replica CPU/memory metrics every 60 seconds.
Both `provision_replica` and the internal `create_managed_service` accept an `enable_storage_introspection_logs: bool` parameter; when true, `--enable-storage-introspection-logs` is passed to the `clusterd` service arguments.
`update_cluster_workload_class` panics if the instance does not exist in either the `StorageController` or `ComputeController`.
Past-generation replicas are cleaned up in background via `remove_past_generation_replicas_in_background`, and orphaned current-generation replicas are removed during `remove_orphaned_replicas`.
