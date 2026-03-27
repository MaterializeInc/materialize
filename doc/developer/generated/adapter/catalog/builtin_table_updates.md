---
source: src/adapter/src/catalog/builtin_table_updates.rs
revision: aa7a1afd31
---

# adapter::catalog::builtin_table_updates

Generates the row-level diffs that keep the `mz_catalog` system tables (e.g. `mz_tables`, `mz_indexes`, `mz_clusters`, `mz_roles`) in sync with the in-memory `CatalogState`.
For each catalog object type, a corresponding `pack_*` function converts a `CatalogEntry` or related struct into one or more `BuiltinTableUpdate` rows with `+1` or `-1` diffs; the coordinator calls these from the DDL path.
`pack_role_update` reads `rolcanlogin` and `rolsuper` directly from role attributes.
The `notice` sub-module handles the special `mz_recent_activity_log` and related notice tables separately.
