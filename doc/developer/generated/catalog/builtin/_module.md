---
source: src/catalog/src/builtin.rs
revision: 584bb9030c
---

# catalog::builtin

Defines every built-in catalog object hardcoded into Materialize: system tables (`BuiltinTable`), views (`BuiltinView`), materialized views (`BuiltinMaterializedView`), indexes (`BuiltinIndex`), types (`BuiltinType`), sources (`BuiltinSource`), logs (`BuiltinLog`), connections (`BuiltinConnection`), cluster configs, roles, and schemas.
Key types include `Builtin<T>` (a generic enum with variants Log, Table, View, MaterializedView, Type, Func, Source, Index, and Connection, each wrapping the corresponding builtin struct), `BUILTINS` (the exhaustive static list used at catalog open time), and constants like `BUILTIN_PREFIXES` and `RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL`.
`MZ_DATABASES`, `MZ_CLUSTERS`, `MZ_CLUSTER_REPLICAS`, and `MZ_CLUSTER_SCHEDULES` are all `BuiltinMaterializedView` objects backed by queries over the catalog. `MZ_CLUSTER_REPLICA_SIZE_INTERNAL` is a `BuiltinTable` that retains size-map entries including disabled sizes, allowing the `MZ_CLUSTER_REPLICAS` materialized view to resolve the `disk` column via LEFT JOIN even for replicas whose sizes have been disabled.
`MZ_CONNECTIONS`, `MZ_SECRETS`, `MZ_SOURCES`, `MZ_DEFAULT_PRIVILEGES`, and `MZ_SYSTEM_PRIVILEGES` are `BuiltinMaterializedView` objects backed by queries over `mz_internal.mz_catalog_raw`. `MZ_SOURCES` is generated dynamically via `make_mz_sources()` in the `builtin` submodule rather than declared as a static, since its SQL must enumerate all builtin and log sources.
`MZ_INDEXES` is also a `BuiltinMaterializedView` generated dynamically via `make_mz_indexes()` in the `mz_catalog` submodule; it inlines the full set of builtin indexes and logs as `VALUES` so that its SQL fingerprint changes whenever a builtin index or log is added or removed, triggering an automatic `MigrationStep::replacement`.
Each builtin struct carries an optional `ontology` field of type `Option<Ontology>` that marks the object as a catalog ontology entity and provides entity-level metadata.
`Ontology` captures an `entity_name`, a one-line `description`, a list of `OntologyLink` relationships, and per-column `column_semantic_types` annotations (`&'static [(&'static str, SemanticType)]`). Semantic types are stored here rather than in `RelationDesc` to avoid persist schema mismatches during zero-downtime upgrades.
`OntologyLink` pairs a link `name` with a `LinkProperties` variant: `ForeignKey` (column-to-column reference with `Cardinality`, optional `source_id_type`, `requires_mapping`, `nullable`, and `note`), `Union` (superset view with optional `discriminator_column`/`discriminator_value`), `MapsTo` (ID-type conversion with required `source_column`/`target_column` and optional `via` table and `from_type`/`to_type`), `DependsOn` (dependency via a graph-edge table with required `source_column`/`target_column` and optional `extra_key_columns` for composite join keys such as `(id, worker_id)`), and `Measures` (metric measurement). `LinkProperties` serializes to JSONB via serde with `#[serde(tag = "kind")]`.
`Cardinality` is `OneToOne` or `ManyToOne`.
Builtin object definitions are split across per-schema submodules: `pg_catalog`, `mz_catalog`, `mz_internal`, `mz_introspection`, and `information_schema`; each re-exports its contents into the parent namespace.
The `builtin` submodule generates the `mz_builtin_materialized_views` view, which exposes metadata about all builtin materialized views.
The `notice` submodule contains the optimizer-notice tables.
The `ontology` submodule contains the static ontology entity definitions used to populate `mz_internal.mz_ontology_*` views.
All built-in objects are auto-installed on catalog open; changes to their definitions are detected via fingerprinting and migrated automatically.
