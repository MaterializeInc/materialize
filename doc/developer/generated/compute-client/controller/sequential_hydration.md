---
source: src/compute-client/src/controller/sequential_hydration.rs
revision: c723c0bc2d
---

# mz-compute-client::controller::sequential_hydration

Implements `SequentialHydration`, a synchronous interceptor that enforces a configurable limit on the number of dataflows hydrating concurrently per replica.
The replica task drives it by feeding every command it intends to send via `absorb_command` and every response it receives via `observe_response`; both methods return the commands that should actually be forwarded to the replica.
`Schedule` commands are held back until a hydration slot is free, and `Frontiers` responses are observed to detect when a dataflow finishes hydrating and a slot opens up.
This interceptor sits between the controller and the `PartitionedState` client so that all replica workers see `Schedule` commands in a consistent order, and it holds no client and spawns no task of its own.
