---
source: src/compute-client/src/controller/instance.rs
revision: 94054eb165
---

# mz-compute-client::controller::instance

Implements `Instance`, the per-compute-instance controller that manages replicas, collections, peeks, subscribes, and COPY TO operations for a single compute instance.
It drives the compute protocol by sending commands to replicas, processing their responses, maintaining collection frontier state, enforcing read policies, tracking wallclock lag, and emitting introspection updates. Supports a `read_only` mode that suppresses persistent state updates.
Key internal types include `CollectionState` (per-collection frontier and read-hold tracking), `ReplicaState` (per-replica client and metrics), `PendingPeek`, and `ActiveSubscribe`. Error types `ReplicaExists`, `ReplicaMissing`, `DataflowCreationError`, and `ReadPolicyError` cover the various failure modes. Storage metadata resolution for `MaterializedViewSinkConnection` is performed during dataflow creation; subscribe and copy-to sinks pass their connection data through unmodified.
`Instance` holds a `replica_dyncfg_overrides: BTreeMap<ReplicaId, ConfigUpdates>` field (empty by default) that stores per-replica dyncfg overrides from the scoped feature flags layer. The `send` method applies overrides via `specialize_command_for_replica`, which for `UpdateConfiguration` merges the replica's `ConfigUpdates` into the update, and for `CreateInstance` captures the current instance-wide dyncfg with the override applied on top as `initial_config`. The base (un-specialized) command is recorded in history so specialization is re-applied at replay time in `add_replica`. `update_replica_dyncfg_overrides` replaces the overrides map.
`add_replica_state` returns `Err(ReadHoldIssuerHungUp)` when storage read hold issuers have closed during process shutdown; the replica is still inserted to keep bookkeeping consistent with the controller, and `initiate_shutdown` is called to terminate the `Instance::run` loop by replacing `command_rx` with a sender-less channel. Unlike `shutdown`, `initiate_shutdown` does not assert that no replicas remain.
`InstanceClient` (in `instance_client`) provides the externally-visible interface, while this module houses the core state machine running as an async task.
