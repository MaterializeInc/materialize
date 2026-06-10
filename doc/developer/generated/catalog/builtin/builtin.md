---
source: src/catalog/src/builtin/builtin.rs
revision: 9693291dd4
---

# catalog::builtin::builtin

Generates builtin views that expose information about builtin catalog objects.

The public entry point is `builtins(builtin_items)`, which iterates over a slice of `Builtin<NameReference>` values and produces additional `Builtin::View` and `Builtin::MaterializedView` entries to be included in the static builtin list.

The module generates two catalog objects:
- The `mz_builtin_materialized_views` builtin view (in `mz_internal`), which reports every builtin materialized view with columns: `oid`, `schema_name`, `name`, `cluster_name`, `definition`, `privileges`, and `create_sql`. Its SQL is constructed dynamically by serializing each `BuiltinMaterializedView`'s metadata into a `VALUES` clause.
- The `mz_builtin_sources` builtin view (in `mz_internal`), which lists builtin and log sources that do not appear in `mz_catalog_raw` (i.e., sources not user-created). User-created sources are exposed via `mz_catalog.mz_sources`. Its SQL is constructed dynamically from `BuiltinSource` and `BuiltinLog` items. `make_mz_sources` generates the `mz_catalog.mz_sources` builtin materialized view, combining the dynamically built `mz_builtin_sources` entries with user sources parsed from `mz_catalog_raw`.

The `privileges` column is built by `make_privileges_sql`, which combines the view's declared access entries with the owner privilege derived from `MZ_SYSTEM_ROLE_ID`.
