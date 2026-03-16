---
source: src/storage-controller/src/instance.rs
revision: 82d92a7fad
---

# storage-controller::instance

Defines `Instance<T>`, which manages communication with all replicas of one storage instance (cluster), and `Replica<T>`, which handles the connection to a single replica over gRPC.
`Instance` maintains per-object scheduling decisions — single-replica objects (most sinks and some sources) run on the lowest-ID replica, while multi-replica objects run on all replicas — and replays the command history to newly added or reconnected replicas via `CommandHistory`.
`Replica` runs a background `ReplicaTask` that connects (with retry) and runs a bidirectional message loop forwarding commands from the controller and responses back up.
`ReplicaConfig` bundles the build info, network location, and gRPC parameters needed to connect.
