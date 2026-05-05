---
source: src/adapter/src/catalog/builtin_table_updates.rs
revision: b4d5dba782
---

# adapter::catalog::builtin_table_updates

Generates the row-level diffs that keep the `mz_catalog` system tables (e.g. `mz_tables`, `mz_indexes`, `mz_clusters`, `mz_roles`) in sync with the in-memory `CatalogState`.
For each catalog object type, a corresponding `pack_*` function converts a `CatalogEntry` or related struct into one or more `BuiltinTableUpdate` rows with `+1` or `-1` diffs; the coordinator calls these from the DDL path.
`BuiltinTableUpdate` wraps either a `Row`-level update or a `ProtoBatch` for bulk ingestion, both resolved to a `CatalogItemId` before being written.
`pack_role_update` derives `rolcanlogin` from `role.attributes.login` (defaulting to `true` for built-in super roles) and `rolsuper` from `role.attributes.superuser` (defaulting to `true` for built-in super roles, `Datum::Null` for indeterminate cloud environments, or `false` otherwise); it also emits per-variable rows into `mz_role_parameters`.
`pack_role_auth_update` writes a row into `mz_role_auth` containing the role's password hash and the timestamp it was last updated.
`CatalogItem::Secret` and `CatalogItem::Connection` do not produce direct `MZ_SECRETS` or `MZ_CONNECTIONS` table updates from `pack_*` functions; `mz_connections` and `mz_secrets` are backed by catalog views rather than builtin tables, so `CatalogItem::Secret` returns an empty update vec and `pack_connection_update` only emits rows for the connection-type-specific subtables (e.g. `MZ_KAFKA_CONNECTIONS`, `MZ_SSH_TUNNEL_CONNECTIONS`).
The `notice` sub-module handles the special `mz_recent_activity_log` and related notice tables separately.
