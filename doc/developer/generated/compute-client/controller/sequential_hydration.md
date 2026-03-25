---
source: src/compute-client/src/controller/sequential_hydration.rs
revision: e757b4d11b
---

# mz-compute-client::controller::sequential_hydration

Implements `SequentialHydration`, a `GenericClient` wrapper that enforces a configurable limit on the number of dataflows hydrating concurrently per replica.
It delays delivery of `Schedule` commands until a hydration slot is free, observing `Frontiers` responses to detect when a dataflow finishes hydrating and a slot opens up.
This client sits between the controller and the `PartitionedState` client so that all replica workers see `Schedule` commands in a consistent order.
