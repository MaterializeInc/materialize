---
source: src/catalog/src/durable/debug.rs
revision: d37c5be00a
---

# catalog::durable::debug

Provides `DebugCatalogState` and the `Collection` trait for low-level inspection and manual repair of the durable catalog.
`Trace` is a full snapshot of all catalog collections, expressed as `CollectionTrace<C>` values keyed by `CollectionType`.
`CollectionType` enumerates every logical collection in the catalog (audit log, clusters, items, roles, schemas, etc.) and serves as serializable labels for trace output.
