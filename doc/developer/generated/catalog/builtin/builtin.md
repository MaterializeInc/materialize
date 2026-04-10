---
source: src/catalog/src/builtin/builtin.rs
revision: 90a38f32be
---

# catalog::builtin::builtin

Generates builtin views that expose information about builtin catalog objects.

The public entry point is `builtins(builtin_items)`, which iterates over a slice of `Builtin<NameReference>` values, filters for `BuiltinMaterializedView` variants, and produces additional `Builtin::View` entries to be included in the static builtin list.

The primary output is the `mz_builtin_materialized_views` builtin view (in `mz_internal`), which reports every builtin materialized view with columns: `oid`, `schema_name`, `name`, `cluster_name`, `definition`, `privileges`, and `create_sql`. Its SQL is constructed dynamically by serializing each `BuiltinMaterializedView`'s metadata into a `VALUES` clause.
