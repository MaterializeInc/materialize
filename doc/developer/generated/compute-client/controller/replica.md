---
source: src/compute-client/src/controller/replica.rs
revision: 5a4a36c4fd
---

# mz-compute-client::controller::replica

Implements `ReplicaClient`, which manages a long-running async task that maintains a connection to a compute replica.
The task continuously attempts to connect to the replica's gRPC endpoint (with retries), sends commands from an unbounded channel, and forwards responses back to the controller via an instrumented sender.
`ReplicaConfig` carries per-replica parameters (location, logging, gRPC settings, expiration offset, and arrangement dictionary compression), and `SequentialHydration` is used as a synchronous interceptor within the message loop: the task feeds it every command it intends to send and every response it receives, and sends the commands the interceptor returns.
The task seeds a per-replica `ConfigSet` (`replica_dyncfg`) from the environment-wide config via `seed_replica_dyncfg`, and updates it by applying each `CreateInstance` (full snapshot) or `UpdateConfiguration` (delta) command via `apply_config_command`. This per-replica config is passed to `SequentialHydration::absorb_command` so that replica-scoped configs like `HYDRATION_CONCURRENCY` take effect at the point where they are enforced.
