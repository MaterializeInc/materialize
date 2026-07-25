---
source: src/storage-controller/src/instance.rs
revision: bbe8b6977e
---

# storage-controller::instance

Defines `Instance`, which manages communication with all replicas of one storage instance (cluster), and `Replica`, which handles the connection to a single replica over gRPC.
`Instance` maintains per-object scheduling decisions — single-replica objects (most sinks and some sources) run on the lowest-ID replica, while multi-replica objects run on all replicas — and replays the command history to newly added or reconnected replicas via `CommandHistory`.
`Instance::active_replica_ids` is a shared helper that resolves an object ID to its scheduled replicas, returning `ActiveReplicas::Scheduled(&BTreeSet<ReplicaId>)` for objects with per-replica scheduling (ingestions and exports) or `ActiveReplicas::All` for objects without scheduling (tables, webhooks). `active_replicas` and `is_active_replica` project this result into their respective mutable-iterator and bool forms. The `collections_hydrated_on_replicas` check uses `active_replica_ids` to skip ingestions that are not scheduled on any of the target replicas: a single-replica source keeps running on its existing replica and cannot hydrate on a pending replica before cut-over, so it must not count against the target's hydration readiness.
`Replica` runs a background `ReplicaTask` that connects (with retry) and runs a bidirectional message loop forwarding commands from the controller and responses back up.
`ReplicaConfig` bundles the build info, network location, and gRPC parameters needed to connect.
