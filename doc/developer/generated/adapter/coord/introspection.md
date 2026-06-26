---
source: src/adapter/src/coord/introspection.rs
revision: 277b33e9c0
---

# adapter::coord::introspection

Implements unified compute introspection: the process of collecting introspection data exported by individual replicas through their logging indexes and writing that data, tagged with the respective replica ID, to unified storage collections.
`install_introspection_subscribes` installs all defined introspection subscribes on a given replica (and `bootstrap_introspection_subscribes` calls it for all existing replicas during coordinator startup); `handle_introspection_subscribe_batch` processes each batch response, writing updates to the corresponding storage-managed collection and reinstalling failed subscribes on disconnect; `drop_introspection_subscribes` removes all subscribes installed on a replica before it is dropped.
Each introspection subscribe is sequenced through a multi-stage pipeline (`OptimizeMir` → `TimestampOptimizeLir` → `Finish`) using the `sequence_staged` driver. The optimizer config for introspection subscribes includes cluster-coherent scoped overrides via `Coordinator::cluster_scoped_optimizer_overrides`.
