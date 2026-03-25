---
source: src/storage-controller/src/collection_mgmt.rs
revision: 82d92a7fad
---

# storage-controller::collection_mgmt

Defines `CollectionManager<T>`, the central coordinator for all storage-managed (introspection) collections, which come in two flavors: append-only and differential.
Append-only collections accept blind writes at any timestamp; differential collections maintain an in-memory desired state and continuously reconcile persist to match it, similar to a self-correcting persist sink.
In read-only mode the manager buffers desired state for differential collections and defers all persist writes until the controller transitions out of read-only mode.
`MonotonicAppender` provides an ergonomic handle for callers to append to append-only collections without holding a reference to the full manager.
