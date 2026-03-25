---
source: src/compute-client/src/controller/instance.rs
revision: 4e5a4cb6c9
---

# mz-compute-client::controller::instance

Implements `Instance`, the per-compute-instance controller that manages replicas, collections, peeks, subscribes, and COPY TO operations for a single compute instance.
It drives the compute protocol by sending commands to replicas, processing their responses, maintaining collection frontier state, enforcing read policies, tracking wallclock lag, and emitting introspection updates.
`InstanceClient` (in `instance_client`) provides the externally-visible interface, while this module houses the core state machine running as an async task.
