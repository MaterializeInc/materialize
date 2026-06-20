---
source: src/controller/src/clusters.rs
revision: 80f8711523
---

# controller::clusters

Implements cluster and replica lifecycle management on `Controller`: creating, configuring, and dropping clusters and their replicas via the orchestrator.

Defines the key configuration and location types — `ClusterConfig`, `ReplicaConfig`, `ReplicaAllocation`, `ReplicaLocation` (`Managed` / `Unmanaged`), `ManagedReplicaLocation`, and `ClusterEvent` — as well as `ReplicaServiceName`, which encodes `(ClusterId, ReplicaId, generation)` as an orchestrator service-name string.
`ReplicaAllocation` carries an optional `family: Option<String>` field that identifies the size family (e.g. `"D"` for `D.1-xsmall`, `"legacy"` for legacy t-shirt sizes). The `family()` method returns this value when set, falling back to `"cc"` for modern sizes (`is_cc = true`) or `"legacy"` otherwise; the family is used as the `replica_size_family` attribute when evaluating replica-local scoped feature flags.
`ManagedReplicaLocation` stores availability zone constraints as `availability_zones: Vec<String>`: for a replica of a managed cluster this is the cluster's `AVAILABILITY ZONES` pool; for a replica of an unmanaged cluster it is the single user-pinned zone as a zero- or one-element list; an empty list means unconstrained. The field is not serialized — it is re-derived from the cluster config at concretization rather than read back from a durable record.
`provision_replica` calls `NamespacedOrchestrator::ensure_service` with a fully constructed `ServiceConfig` (including Timely addresses, resource limits, labels, and availability zones) and spawns a background task that polls replica CPU/memory metrics every 60 seconds.
Both `provision_replica` and the internal `create_managed_service` accept an `enable_storage_introspection_logs: bool` parameter; when true, `--enable-storage-introspection-logs` is passed to the `clusterd` service arguments.
`update_cluster_workload_class` panics if the instance does not exist in either the `StorageController` or `ComputeController`.
Past-generation replicas are cleaned up in background via `remove_past_generation_replicas_in_background`, and orphaned current-generation replicas are removed during `remove_orphaned_replicas`.
The `other_replicas_selector` and `replicas_selector` passed to the orchestrator include a `generation` label matching the current deploy generation, so that anti-affinity and topology-spread constraints only consider pods of the same generation; this prevents in-flight old-generation pods from blocking placement of new-generation pods during a rolling upgrade.
