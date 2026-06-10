---
source: src/compute-client/src/controller/introspection.rs
revision: 00cc513fa5
---

# mz-compute-client::controller::introspection

Implements `IntrospectionSink`, which routes introspection updates produced by the compute controller to the storage controller's `CollectionManager` for persistence.
`spawn_introspection_sink` starts an async task that receives `(IntrospectionType, Vec<(Row, Diff)>)` batches over an unbounded channel and dispatches them to the appropriate differential or append-only storage sender.
