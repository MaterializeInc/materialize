---
source: src/compute-client/src/controller/instance.rs
revision: 9d0a7c3c6f
---

# mz-compute-client::controller::instance

Implements `Instance`, the per-compute-instance controller that manages replicas, collections, peeks, subscribes, and COPY TO operations for a single compute instance.
It drives the compute protocol by sending commands to replicas, processing their responses, maintaining collection frontier state, enforcing read policies, tracking wallclock lag, and emitting introspection updates. Supports a `read_only` mode that suppresses persistent state updates.
Key internal types include `CollectionState` (per-collection frontier and read-hold tracking), `ReplicaState` (per-replica client and metrics), `PendingPeek`, and `ActiveSubscribe`. Error types `ReplicaExists`, `ReplicaMissing`, `DataflowCreationError`, and `ReadPolicyError` cover the various failure modes. Storage metadata resolution for `MaterializedViewSinkConnection` is performed during dataflow creation; subscribe and copy-to sinks pass their connection data through unmodified.
`InstanceClient` (in `instance_client`) provides the externally-visible interface, while this module houses the core state machine running as an async task.
