---
source: src/catalog/src/memory/objects.rs
revision: 278f65f05f
---

# catalog::memory::objects

Defines all in-memory catalog object types, which are richer and more consumer-friendly than the durable `objects` types.
Key types include `CatalogItem` (an enum over Table, Source, Log, View, MaterializedView, Sink, Index, Type, Func, Secret, Connection, ContinualTask), `CatalogEntry` (pairs a `CatalogItem` with its metadata), `Database`, `Schema`, `Role`, `Cluster`, `ClusterReplica`, and `NetworkPolicy`.
`View` and `MaterializedView` store their locally optimized MIR expression in `locally_optimized_expr`.
`Index`, `MaterializedView`, and `ContinualTask` additionally carry `optimized_plan` (global MIR dataflow), `physical_plan` (LIR dataflow), and `dataflow_metainfo` (optimizer notices) fields, all `#[serde(skip)]` since they are populated after catalog replay and are not part of the durable representation.
`CatalogItem` exposes `optimized_plan()`, `physical_plan()`, and `dataflow_metainfo()` accessors for the plan fields, and `plan_fields_mut()` for mutable access to all three plan fields at once on plan-bearing items (`Index`, `MaterializedView`, `ContinualTask`); it returns `None` for other item kinds. `CatalogEntry` exposes `item_mut()` for in-place mutation.
`StateUpdate` and `StateUpdateKind` represent in-memory deltas applied during catalog replay and incremental updates.
`DataSourceDesc` and `DataSource` describe how a source or table obtains its data (ingestion, introspection, webhook, progress, etc.).
`Sink::envelope` returns `Some("append")` for the `SinkEnvelope::Append` variant in addition to `Some("debezium")` and `Some("upsert")`.
The `UpdateFrom` trait drives the incremental update pattern throughout this module.
