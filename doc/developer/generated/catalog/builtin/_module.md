---
source: src/catalog/src/builtin.rs
revision: 943e082d03
---

# catalog::builtin

Defines every built-in catalog object hardcoded into Materialize: system tables (`BuiltinTable`), views (`BuiltinView`), materialized views (`BuiltinMaterializedView`), indexes (`BuiltinIndex`), types (`BuiltinType`), sources (`BuiltinSource`), logs (`BuiltinLog`), connections (`BuiltinConnection`), cluster configs, roles, and schemas.
Key types include `Builtin<T>` (a generic enum with variants Log, Table, View, MaterializedView, Type, Func, Source, Index, and Connection, each wrapping the corresponding builtin struct), `BUILTINS` (the exhaustive static list used at catalog open time), and constants like `BUILTIN_PREFIXES` and `RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL`.
`MZ_DATABASES` is a `BuiltinMaterializedView` backed by a query over the catalog.
`MZ_CONNECTIONS` and `MZ_SECRETS` are `BuiltinMaterializedView` objects backed by queries over `mz_internal.mz_catalog_raw`, and are registered in `BUILTINS_STATIC` as `Builtin::MaterializedView`.
The `builtin` submodule generates the `mz_builtin_materialized_views` view, which exposes metadata about all builtin materialized views.
The `notice` submodule contains the optimizer-notice tables.
All built-in objects are auto-installed on catalog open; changes to their definitions are detected via fingerprinting and migrated automatically.
