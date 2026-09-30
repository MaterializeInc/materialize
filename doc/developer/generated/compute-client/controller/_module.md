---
source: src/compute-client/src/controller.rs
revision: 60a8dd3a8f
---

# mz-compute-client::controller

Provides the compute controller, which manages compute instances, their replicas, and the collections (indexes, sinks, subscribes, COPY TOs) installed on them.
`ComputeController` is the top-level entry point; it creates and drops instances (each represented by an `Instance` task via `instance_client`), routes commands and responses through the compute protocol, and exposes `update_replica_dyncfg_overrides` to set per-replica dyncfg overrides (used by the scoped feature flags layer) across all instances.
`ComputeController` maintains a `replica_dyncfg_overrides: BTreeMap<ReplicaId, ConfigUpdates>` field alongside the per-instance copies. This controller-level copy is consulted at replica-creation time to resolve replica-scoped configs (such as `COMPUTE_REPLICA_EXPIRATION_OFFSET` and `ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA`) through the new replica's overrides before the first configuration command arrives; without it, those values would be read from the environment-wide set even when a replica-specific override exists.
When adding a replica, the controller folds the current dyncfg into the `CreateInstance` command as `initial_config` so the replica seeds its worker configuration before create-time setup. A subsequent `UpdateConfiguration` still follows to carry workload class, max result size, tracing, and to sync dyncfg into persist config and metrics; the overlapping dyncfg application is idempotent.
`PeekNotification` has a `Success` variant carrying `rows` (row count after applying `offset`/`limit`) and `result_size` (bytes), an `Error(String)` variant, and a `Canceled` variant. It is constructed from a `PeekResponse` via `PeekNotification::new`, which handles both inline (`PeekResponse::Rows`) and stashed (`PeekResponse::Stashed`) responses.
`CollectionReadiness` is a three-variant enum (`Ready`, `Lagging { lag: Option<u64> }`, `Unhydrated`) returned by `CollectionReadiness::classify`. `classify` takes `hydrated: bool`, a frontier, and an optional `(reference_frontier, allowed_lag)` pair; when the lag requirement is `Some` and `frontier_within_lag` returns `false`, it returns `Lagging` with the timestamp-tick gap where computable. `collections_ready_for_replicas` on `ComputeController` dispatches to `Instance::collections_ready_on_replicas` with the caller-supplied `allowed_lag` and `reference_replicas`.
Supporting modules cover per-method error types (`error`), the instance state machine (`instance`), the external instance interface (`instance_client`), replica connection management (`replica`), sequential hydration enforcement (`sequential_hydration`), and introspection routing (`introspection`).
