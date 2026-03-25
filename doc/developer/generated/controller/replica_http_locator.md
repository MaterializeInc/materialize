---
source: src/controller/src/replica_http_locator.rs
revision: 101efde02a
---

# controller::replica_http_locator

Provides `ReplicaHttpLocator`, an in-memory `RwLock`-protected map from `(ClusterId, ReplicaId)` to per-process HTTP addresses for cluster replica processes.
The controller calls `register_replica` when provisioning a managed replica and `remove_replica` when dropping one; `get_http_addr` lets `environmentd` proxy HTTP requests to a specific replica process without direct pod-level network access.
