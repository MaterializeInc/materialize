---
source: src/compute-client/src/controller.rs
revision: 00cc513fa5
---

# mz-compute-client::controller

Provides the compute controller, which manages compute instances, their replicas, and the collections (indexes, sinks, subscribes, COPY TOs) installed on them.
`ComputeController` is the top-level entry point; it creates and drops instances (each represented by an `Instance` task via `instance_client`) and routes commands and responses through the compute protocol.
Supporting modules cover per-method error types (`error`), the instance state machine (`instance`), the external instance interface (`instance_client`), replica connection management (`replica`), sequential hydration enforcement (`sequential_hydration`), and introspection routing (`introspection`).
