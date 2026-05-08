---
source: src/catalog/src/builtin/ontology.rs
revision: ea77b8b38b
---

# catalog::builtin::ontology

Generates the four `mz_internal.mz_ontology_*` views derived from `Ontology` annotations on builtin catalog objects.

`generate_views` is the public entry point.
It iterates over all builtins, selects those with an `ontology: Some(...)` field (tables, views, materialized views, and sources), and produces four `Builtin::View` values that are leaked to `'static` lifetime via `leak` — one per view, bounded to startup.

The four generated views are:

- `mz_ontology_entity_types`: one row per annotated relation with columns `name`, `relation`, `properties` (primary and alternate keys as JSONB), and `description`. Backed by a static VALUES list built at startup.
- `mz_ontology_semantic_types`: small reference table of all known semantic types with columns `name`, `sql_type`, and `description`. Rows come from the `SEMANTIC_TYPE_DEFS` constant.
- `mz_ontology_properties`: one row per column of every annotated relation, with columns `entity_type`, `column_name`, `semantic_type`, and `description`. Uses a live catalog join through `mz_schemas` → `mz_objects` → `mz_columns` so column additions are reflected automatically, with semantic-type annotations and column comments LEFT JOINed from static VALUES lists and `mz_comments` respectively.
- `mz_ontology_link_types`: one row per `OntologyLink` on each annotated relation, with columns `name`, `source_entity`, `target_entity`, `properties` (JSONB), and `description`. Backed by a static VALUES list.

Internal helpers `values_view` and `values_sql` construct SQL `VALUES (...)` fragments with proper single-quote escaping via `esc`; `sql_type_name` maps `SqlScalarType` variants to their SQL cast targets; and `pk_lit` serializes a relation's key set as a JSONB object with `primary_key` and optional `alternate_keys` fields.

`SEMANTIC_TYPE_DEFS` is a `pub(super)` constant slice of `(SemanticType, sql_type, description)` triples covering all recognized semantic types such as `CatalogItemId`, `GlobalId`, `ClusterId`, `ReplicaId`, `MzTimestamp`, `ByteCount`, `RecordCount`, and others.
