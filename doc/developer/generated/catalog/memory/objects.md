---
source: src/catalog/src/memory/objects.rs
revision: a90f8e3027
---

# catalog::memory::objects

Defines all in-memory catalog object types, which are richer and more consumer-friendly than the durable `objects` types.
Key types include `CatalogItem` (an enum over Table, Source, Log, View, MaterializedView, Sink, Index, Type, Func, Secret, Connection), `CatalogEntry` (pairs a `CatalogItem` with its metadata), `Database`, `Schema`, `Role`, `Cluster`, `ClusterReplica`, `NetworkPolicy`, `ReconfigurationState`, and `BurstState`.
`View` and `MaterializedView` store their locally optimized MIR expression in `locally_optimized_expr`.
`Index` and `MaterializedView` additionally carry `optimized_plan` (global MIR dataflow), `physical_plan` (`DataflowDescription<LirRelationExpr>`, i.e. the LIR dataflow), and `dataflow_metainfo` (optimizer notices) fields, all `#[serde(skip)]` since they are populated after catalog replay and are not part of the durable representation.
`CatalogItem` exposes `optimized_plan()`, `physical_plan()`, and `dataflow_metainfo()` accessors for the plan fields, and `plan_fields_mut()` for mutable access to all three plan fields at once on plan-bearing items (`Index`, `MaterializedView`); it returns `None` for other item kinds. `CatalogItem::set_create_sql` overwrites the `create_sql` of a view, index, or table without replanning, used during temporary item updates to keep the byte-identical `create_sql` for retraction consolidation; it panics for item kinds that cannot be temporary. `CatalogItem::update_timestamp_interval` updates the timestamp interval for a source and returns the previous `WithOptionValue<Raw>` if one was set, or `None`; it returns `Err(())` if called on a non-source item. `CatalogEntry` exposes `item_mut()` for in-place mutation.
`ClusterVariantManaged` carries `auto_scaling_strategy` (`Option<AutoScalingStrategy>`), `reconfiguration` (`Option<ReconfigurationState>`), and `burst` (`Option<BurstState>`) in addition to the base cluster shape fields. `ReconfigurationState` is the in-memory mirror of the durable type, recording the target config, deadline, and `on_timeout` action for a graceful reconfiguration. `BurstState` is the in-memory mirror of the durable burst-state record, carrying `burst_size`, `linger_duration`, and an optional `steady_hydrated_at` timestamp.
`ClusterReplicaProcessStatus` records the process status, a cumulative `restart_count` mirrored from the orchestrator, and the time of the most recent change to either field.
`Schema::has_items` returns true if any of `self.items`, `self.types`, or `self.functions` is non-empty, so DROP SCHEMA without CASCADE is rejected when the schema contains only types or functions.
`StateUpdate` and `StateUpdateKind` represent in-memory deltas applied during catalog replay and incremental updates. `StateUpdateKind` includes `ClusterSystemConfiguration` and `ReplicaSystemConfiguration` variants carrying the corresponding durable objects. `BootstrapStateUpdateKind` mirrors `StateUpdateKind` for the bootstrap phase and includes the same two variants; it converts to/from `StateUpdateKind` via `From`/`TryFrom` implementations.
`DataSourceDesc` and `DataSource` describe how a source or table obtains its data (ingestion, introspection, webhook, progress, etc.).
`Sink::envelope` returns `Some("append")` for the `SinkEnvelope::Append` variant in addition to `Some("debezium")` and `Some("upsert")`.
The `UpdateFrom` trait drives the incremental update pattern throughout this module.
