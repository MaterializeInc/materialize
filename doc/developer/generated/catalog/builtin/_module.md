---
source: src/catalog/src/builtin.rs
revision: f7fabf4978
---

# catalog::builtin

Defines every built-in catalog object hardcoded into Materialize: system tables (`BuiltinTable`), views (`BuiltinView`), materialized views (`BuiltinMaterializedView`), indexes (`BuiltinIndex`), types (`BuiltinType`), sources (`BuiltinSource`), continual tasks (`BuiltinContinualTask`), connections (`BuiltinConnection`), cluster configs, roles, and schemas.
Key types include `Builtin<T>` (generic wrapper carrying name, schema, OID, and object-specific data), `BUILTINS` (the exhaustive static list used at catalog open time), and constants like `BUILTIN_PREFIXES` and `RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL`.
`MZ_DATABASES` is a `BuiltinMaterializedView` backed by a query over the catalog.
The `notice` submodule contains the optimizer-notice tables.
All built-in objects are auto-installed on catalog open; changes to their definitions are detected via fingerprinting and migrated automatically.
