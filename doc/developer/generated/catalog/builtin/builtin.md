---
source: src/catalog/src/builtin/builtin.rs
revision: 7e4fee33d0
---

# catalog::builtin::builtin

Generates builtin views that expose information about builtin catalog objects.

The public entry point is `builtins(builtin_items)`, which iterates over a slice of `Builtin<NameReference>` values and produces additional `Builtin::View` entries to be included in the static builtin list.

The module generates four catalog objects:
- The `mz_builtin_sources` builtin view (in `mz_internal`), which lists builtin and log sources that do not appear in `mz_catalog_raw` (i.e., sources not user-created). User-created sources are exposed via `mz_catalog.mz_sources`. Its SQL is constructed dynamically from `BuiltinSource` and `BuiltinLog` items. `make_mz_sources` generates the `mz_catalog.mz_sources` builtin materialized view, combining the dynamically built `mz_builtin_sources` entries with user sources parsed from `mz_catalog_raw`.
- The `mz_builtin_materialized_views` builtin view (in `mz_internal`), which reports every builtin materialized view with columns: `oid`, `schema_name`, `name`, `cluster_name`, `definition`, `privileges`, and `create_sql`. Its SQL is constructed dynamically by serializing each `BuiltinMaterializedView`'s metadata into a `VALUES` clause.
- The `mz_builtin_tables` builtin view (in `mz_internal`), which lists every builtin table with columns: `oid`, `schema_name`, `name`, and `privileges`. Its SQL is constructed dynamically from `BuiltinTable` items.
- The `mz_builtin_views` builtin view (in `mz_internal`), which lists every builtin view with columns: `oid`, `schema_name`, `name`, `definition`, `privileges`, and `create_sql`. Views from the static builtin list are listed with their real definition and create SQL; the three generated views above and `mz_builtin_views` itself are listed with a placeholder query instead of their real SQL, because embedding their full definitions would produce enormous or self-referential rows.

The `privileges` column is built by `make_privileges_sql`, which combines the view's declared access entries with the owner privilege derived from `MZ_SYSTEM_ROLE_ID`.
