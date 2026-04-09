---
source: src/catalog/src/memory/objects.rs
revision: 0c2379c49c
---

# catalog::memory::objects

Defines all in-memory catalog object types, which are richer and more consumer-friendly than the durable `objects` types.
Key types include `CatalogItem` (an enum over Table, Source, Log, View, MaterializedView, Sink, Index, Type, Func, Secret, Connection, ContinualTask), `CatalogEntry` (pairs a `CatalogItem` with its metadata), `Database`, `Schema`, `Role`, `Cluster`, `ClusterReplica`, and `NetworkPolicy`.
`StateUpdate` and `StateUpdateKind` represent in-memory deltas applied during catalog replay and incremental updates.
`DataSourceDesc` and `DataSource` describe how a source or table obtains its data (ingestion, introspection, webhook, progress, etc.).
`Sink::envelope` returns `Some("append")` for the `SinkEnvelope::Append` variant in addition to `Some("debezium")` and `Some("upsert")`.
The `UpdateFrom` trait drives the incremental update pattern throughout this module.
