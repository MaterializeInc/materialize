---
source: src/adapter/src/coord/introspection.rs
revision: 7f6c52776d
---

# adapter::coord::introspection

Implements unified compute introspection: the process of collecting introspection data exported by individual replicas through their logging indexes and writing that data, tagged with the respective replica ID, to unified storage collections.
`install_introspection_subscribes` installs all defined introspection subscribes on a given replica (and `bootstrap_introspection_subscribes` calls it for all existing replicas during coordinator startup); `handle_introspection_subscribe_batch` processes each batch response, writing updates to the corresponding storage-managed collection and reinstalling failed subscribes on disconnect; `drop_introspection_subscribes` removes all subscribes installed on a replica before it is dropped.
Each introspection subscribe is sequenced through a multi-stage pipeline (`OptimizeMir` → `TimestampOptimizeLir` → `Finish`) using the `sequence_staged` driver. The optimizer config for introspection subscribes includes cluster-coherent scoped overrides via `Coordinator::cluster_scoped_optimizer_overrides`.
`IntrospectionSubscribe` tracks a `first_data_at: Option<Instant>` field recording when the subscribe first appended data to its target storage collection in the current process. Rows in that collection before that point may describe a previous environmentd process or a prior replica incarnation and must not be trusted. `invalidate_introspection_freshness(replica_id)` clears `first_data_at` for all subscribes targeting a given replica when a cluster event reports the replica offline or restarted. `fresh_introspection_replicas(introspection_type, margin)` returns the string replica IDs whose subscribe of the given type delivered data at least `margin` ago; consumers such as the arrangement sizes snapshot use this to exclude stale replicas.
