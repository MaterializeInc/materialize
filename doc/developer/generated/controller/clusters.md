---
source: src/controller/src/clusters.rs
revision: b1c657d28e
---

# controller::clusters

Implements cluster and replica lifecycle management on `Controller`: creating, configuring, and dropping clusters and their replicas via the orchestrator.

Defines the key configuration and location types — `ClusterConfig`, `ReplicaConfig`, `ReplicaAllocation`, `ReplicaLocation` (`Managed` / `Unmanaged`), `ManagedReplicaLocation`, and `ClusterEvent` — as well as `ReplicaServiceName`, which encodes `(ClusterId, ReplicaId, generation)` as an orchestrator service-name string.
`provision_replica` calls `NamespacedOrchestrator::ensure_service` with a fully constructed `ServiceConfig` (including Timely addresses, resource limits, labels, and availability zones) and spawns a background task that polls replica CPU/memory metrics every 60 seconds.
Past-generation replicas are cleaned up in background via `remove_past_generation_replicas_in_background`, and orphaned current-generation replicas are removed during `remove_orphaned_replicas`.
