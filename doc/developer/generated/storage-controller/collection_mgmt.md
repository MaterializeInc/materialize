---
source: src/storage-controller/src/collection_mgmt.rs
revision: 5680493e7d
---

# storage-controller::collection_mgmt

Defines `CollectionManager<T>`, the central coordinator for all storage-managed (introspection) collections, which come in two flavors: append-only and differential.
Append-only collections accept blind writes at any timestamp; differential collections maintain an in-memory desired state and continuously reconcile persist to match it, similar to a self-correcting persist sink.
In read-only mode the manager maintains the desired state for differential collections so it can immediately begin writing when leaving read-only mode, but does not write to persist.
For append-only collections in read-only mode, calls to `blind_write` panic and attempts to append via `MonotonicAppender` return `StorageError::ReadOnly`.
`MonotonicAppender` provides an ergonomic handle for callers to append to append-only collections without holding a reference to the full manager.
