---
source: src/storage-client/src/controller.rs
revision: 9d0a7c3c6f
---

# storage-client::controller

Defines the `StorageController` trait, which is the primary interface used by the adapter/coordinator to manage the lifecycle of storage collections, sources, sinks, and introspection collections.
Provides key supporting types: `CollectionDescription`, `DataSource`, `ExportDescription`, `ExportState`, `IntrospectionType`, `StorageMetadata`, `StorageTxn`, `MonotonicAppender`, `WallclockLag`, and `PersistEpoch`.
The `StorageTxn` trait abstracts durable metadata persistence for shard-to-collection mappings and unfinalized shard tracking.
