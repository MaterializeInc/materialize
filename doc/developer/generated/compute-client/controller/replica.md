---
source: src/compute-client/src/controller/replica.rs
revision: c723c0bc2d
---

# mz-compute-client::controller::replica

Implements `ReplicaClient`, which manages a long-running async task that maintains a connection to a compute replica.
The task continuously attempts to connect to the replica's gRPC endpoint (with retries), sends commands from an unbounded channel, and forwards responses back to the controller via an instrumented sender.
`ReplicaConfig` carries per-replica parameters (location, logging, gRPC settings, expiration offset, and arrangement dictionary compression), and `SequentialHydration` is used as a synchronous interceptor within the message loop: the task feeds it every command it intends to send and every response it receives, and sends the commands the interceptor returns.
