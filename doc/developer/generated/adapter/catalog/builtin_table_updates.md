---
source: src/adapter/src/catalog/builtin_table_updates.rs
revision: 4ec14fa5c7
---

# adapter::catalog::builtin_table_updates

Generates the row-level diffs that keep the `mz_catalog` system tables (e.g. `mz_tables`, `mz_indexes`) in sync with the in-memory `CatalogState`.
For each catalog object type, a corresponding `pack_*` function converts a `CatalogEntry` or related struct into one or more `BuiltinTableUpdate` rows with `+1` or `-1` diffs; the coordinator calls these from the DDL path.
`BuiltinTableUpdate` wraps either a `Row`-level update or a `ProtoBatch` for bulk ingestion, both resolved to a `CatalogItemId` before being written.
`pack_role_auth_update` writes a row into `mz_role_auth` containing the role's password hash and the timestamp it was last updated. `mz_roles`, `mz_role_parameters`, `mz_clusters`, `mz_cluster_replicas`, and `mz_cluster_schedules` are materialized views backed by `mz_internal.mz_catalog_raw`; rows for these objects are not produced by `pack_*` functions here.
`MZ_CLUSTER_REPLICA_SIZE_INTERNAL` (a `BuiltinTable`) receives rows via `pack_cluster_replica_size_update` and retains entries for disabled sizes; the `mz_cluster_replicas` MV LEFT JOINs it to resolve the `disk` column.
`CatalogItem::Secret`, `CatalogItem::Log`, `CatalogItem::Connection`, and `CatalogItem::Index` do not produce direct table updates for `MZ_SECRETS`, `MZ_SOURCES`, `MZ_CONNECTIONS`, or `MZ_INDEXES` from `pack_*` functions; `mz_connections`, `mz_secrets`, and `mz_sources` are backed by catalog views or materialized views rather than builtin tables, so `CatalogItem::Secret` and `CatalogItem::Log` return empty update vecs and `pack_connection_update` only emits rows for the connection-type-specific subtables (e.g. `MZ_KAFKA_CONNECTIONS`, `MZ_SSH_TUNNEL_CONNECTIONS`). `ConnectionDetails::GlueSchemaRegistry` and `ConnectionDetails::Gcp` are handled in `pack_connection_update` alongside `Csr`, `Postgres`, `MySql`, and `SqlServer` as types that do not produce their own subtable row. `CatalogItem::Source` similarly does not produce rows into `MZ_SOURCES` (a materialized view over `mz_catalog_raw`); it only emits rows for source-type-specific subtables and subsource/progress metadata. `CatalogItem::Index` does not produce rows into `MZ_INDEXES` (a materialized view); `pack_index_update` does not write to `MZ_INDEXES`.
The `notice` sub-module handles the special `mz_recent_activity_log` and related notice tables separately.
