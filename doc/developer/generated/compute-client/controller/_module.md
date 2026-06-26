---
source: src/compute-client/src/controller.rs
revision: 141cb2a0a5
---

# mz-compute-client::controller

Provides the compute controller, which manages compute instances, their replicas, and the collections (indexes, sinks, subscribes, COPY TOs) installed on them.
`ComputeController` is the top-level entry point; it creates and drops instances (each represented by an `Instance` task via `instance_client`), routes commands and responses through the compute protocol, and exposes `update_replica_dyncfg_overrides` to set per-replica dyncfg overrides (used by the scoped feature flags layer) across all instances.
When adding a replica, the controller folds the current dyncfg into the `CreateInstance` command as `initial_config` (via `specialize_command_for_replica`) so the replica seeds its worker configuration before create-time setup. A subsequent `UpdateConfiguration` still follows to carry workload class, max result size, tracing, and to sync dyncfg into persist config and metrics; the overlapping dyncfg application is idempotent.
Supporting modules cover per-method error types (`error`), the instance state machine (`instance`), the external instance interface (`instance_client`), replica connection management (`replica`), sequential hydration enforcement (`sequential_hydration`), and introspection routing (`introspection`).
