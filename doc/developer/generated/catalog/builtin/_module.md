---
source: src/catalog/src/builtin.rs
revision: ea77b8b38b
---

# catalog::builtin

Defines every built-in catalog object hardcoded into Materialize: system tables (`BuiltinTable`), views (`BuiltinView`), materialized views (`BuiltinMaterializedView`), indexes (`BuiltinIndex`), types (`BuiltinType`), sources (`BuiltinSource`), logs (`BuiltinLog`), connections (`BuiltinConnection`), cluster configs, roles, and schemas.
Key types include `Builtin<T>` (a generic enum with variants Log, Table, View, MaterializedView, Type, Func, Source, Index, and Connection, each wrapping the corresponding builtin struct), `BUILTINS` (the exhaustive static list used at catalog open time), and constants like `BUILTIN_PREFIXES` and `RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL`.
`MZ_DATABASES` is a `BuiltinMaterializedView` backed by a query over the catalog.
`MZ_CONNECTIONS` and `MZ_SECRETS` are `BuiltinMaterializedView` objects backed by queries over `mz_internal.mz_catalog_raw`, and are registered in `BUILTINS_STATIC` as `Builtin::MaterializedView`.
Each builtin struct carries an optional `ontology` field of type `Option<Ontology>` that marks the object as a catalog ontology entity and provides entity-level metadata.
`Ontology` captures an `entity_name`, a one-line `description`, a list of `OntologyLink` relationships, and per-column `column_semantic_types` annotations (`&'static [(&'static str, SemanticType)]`). Semantic types are stored here rather than in `RelationDesc` to avoid persist schema mismatches during zero-downtime upgrades.
`OntologyLink` pairs a link `name` with a `LinkProperties` variant: `ForeignKey` (column-to-column reference with `Cardinality`, optional `source_id_type`, `requires_mapping`, `nullable`, and `note`), `Union` (superset view with optional `discriminator_column`/`discriminator_value`), `MapsTo` (ID-type conversion with optional `via` table and `from_type`/`to_type`), `DependsOn` (direct dependency), and `Measures` (metric measurement). `LinkProperties` serializes to JSONB via serde with `#[serde(tag = "kind")]`.
`Cardinality` is `OneToOne` or `ManyToOne`.
The `builtin` submodule generates the `mz_builtin_materialized_views` view, which exposes metadata about all builtin materialized views.
The `notice` submodule contains the optimizer-notice tables.
The `ontology` submodule contains the static ontology entity definitions used to populate `mz_internal.mz_ontology_*` views.
All built-in objects are auto-installed on catalog open; changes to their definitions are detected via fingerprinting and migrated automatically.
