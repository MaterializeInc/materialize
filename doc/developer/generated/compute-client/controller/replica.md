---
source: src/compute-client/src/controller/replica.rs
revision: c58b2ebb27
---

# mz-compute-client::controller::replica

Implements `ReplicaClient`, which manages a long-running async task that maintains a connection to a compute replica.
The task continuously attempts to connect to the replica's gRPC endpoint (with retries), sends commands from an unbounded channel, and forwards responses back to the controller via an instrumented sender.
`ReplicaConfig` carries per-replica parameters (location, logging, gRPC settings, expiration offset), and `SequentialHydration` is layered over the underlying client within the task.
