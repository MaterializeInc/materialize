---
source: src/storage-client/src/controller.rs
revision: 4d59d67c50
---

# storage-client::controller

Defines the `StorageController` trait, which is the primary interface used by the adapter/coordinator to manage the lifecycle of storage collections, sources, sinks, and introspection collections.
Provides key supporting types: `CollectionDescription`, `DataSource`, `ExportDescription`, `ExportState`, `IntrospectionType`, `StorageMetadata`, `StorageTxn`, `MonotonicAppender`, `WallclockLag`, `WallclockLagHistogramPeriod`, `StorageWriteOp`, `Response`, and `PersistEpoch`.
`DataSource` enumerates how collection data is produced: `Ingestion`, `IngestionExport`, `Introspection`, `Progress`, `Webhook`, `Table`, `Other`, and `Sink { desc: ExportDescription }`.
`StorageWriteOp` represents high-level write operations (`Append` and `Delete`) used to update differential introspection collections.
`WallclockLagHistogramPeriod` represents a `[start, end)` time range for wallclock lag histogram bucketing.
The `StorageTxn` trait abstracts durable metadata persistence for shard-to-collection mappings, unfinalized shard tracking, and the txn WAL shard.
