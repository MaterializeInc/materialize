---
source: src/storage-client/src/controller.rs
revision: a60edac7f1
---

# storage-client::controller

Defines the `StorageController` trait, which is the primary interface used by the adapter/coordinator to manage the lifecycle of storage collections, sources, sinks, and introspection collections.
Provides key supporting types: `CollectionDescription`, `DataSource`, `ExportDescription`, `ExportState`, `IntrospectionType`, `StorageMetadata`, `StorageTxn`, `MonotonicAppender`, `WallclockLag`, `WallclockLagHistogramPeriod`, `StorageWriteOp`, `Response`, and `PersistEpoch`.
`DataSource` enumerates how collection data is produced: `Ingestion`, `IngestionExport`, `Introspection`, `Progress`, `Webhook`, `Table`, `Other`, and `Sink { desc: ExportDescription }`.
`StorageWriteOp` represents high-level write operations (`Append` and `Delete`) used to update differential introspection collections.
`WallclockLagHistogramPeriod` represents a `[start, end)` time range for wallclock lag histogram bucketing.
The `StorageTxn` trait abstracts durable metadata persistence for shard-to-collection mappings, unfinalized shard tracking, and the txn WAL shard. `remove_unfinalized_shards` removes entries from the unfinalized shard set without finalizing the underlying Persist shards; callers are responsible for reconciling the set against active collection metadata before calling it, since stale entries can refer to active collections.
`collections_hydrated_on_replicas` skips ingestions whose scheduled replica set is disjoint from the target replicas: a single-replica source stays scheduled on its current replica and cannot hydrate on a replica it is not scheduled on, so such ingestions must not count against the target's hydration readiness. Replica health (online/offline status) is a caller concern and is checked separately outside this method.
