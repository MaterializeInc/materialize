---
source: src/catalog/src/builtin/ontology.rs
---

# catalog::builtin::ontology

Generates four built-in views in `mz_internal` that describe the structure and
relationships of the Materialize system catalog. Designed to help LLMs,
diagnostic tools, and developers discover the right tables, join paths, and ID
types when writing catalog queries.

## Views

| View | Columns | Purpose |
|---|---|---|
| `mz_internal.mz_ontology_entity_types` | `name, relation, properties, description` | What kinds of things exist. `properties` jsonb has `{"primary_key": ["id"]}`. |
| `mz_internal.mz_ontology_semantic_types` | `name, sql_type, description` | Typed ID domains and other semantic column types (CatalogItemId, GlobalId, ByteCount, etc.) |
| `mz_internal.mz_ontology_properties` | `entity_type, column_name, semantic_type, description` | Maps every column to its semantic type and describes what it means. |
| `mz_internal.mz_ontology_link_types` | `name, source_entity, target_entity, properties, description` | Named relationships between entity types. |

The views are generated at startup by `generate_views()`, which enumerates all
builtins that have `ontology: Some(...)` annotations and extracts metadata from
their `RelationDesc`, column comments, and semantic type annotations.

## How it works

1. **Entity types** — one row per builtin with an `Ontology` annotation. The
   `relation` column is `schema.table_name`, `properties` contains primary key
   info extracted from `RelationDesc::typ().keys`.

2. **Semantic types** — a static reference table of 20 ID/value domains
   (e.g., `CatalogItemId`, `GlobalId`, `ReplicaId`, `ByteCount`).

3. **Properties** — one row per column per annotated entity. Joins against
   `mz_columns` at runtime to discover column names and types. Semantic type
   annotations come from `RelationDesc::get_semantic_type()`. Column
   descriptions come from `mz_comments`.

4. **Link types** — one row per `OntologyLink` on each annotated entity.
   The `properties` JSONB column contains structured relationship metadata
   (kind, source_column, target_column, cardinality, source_id_type, etc.).

## Link type properties

The `properties` jsonb in `mz_ontology_link_types` uses a `"kind"` field:

- `"foreign_key"` — column-level join with `source_column`, `target_column`, `cardinality`
- `"measures"` — a measurement/metric relationship
- `"depends_on"` — a dependency relationship
- `"maps_to"` — an ID mapping (e.g., CatalogItemId to GlobalId)
- `"union"` — a UNION view includes another entity type

Common keys in the properties JSONB:

| Key | Description |
|---|---|
| `kind` | Relationship kind: `foreign_key`, `measures`, `depends_on`, `maps_to`, or `union`. |
| `source_column` | Column name on the source entity used for the join. |
| `target_column` | Column name on the target entity used for the join. |
| `cardinality` | Join cardinality: `many_to_one`, `one_to_one`, `many_to_many`. |
| `nullable` | `true` if the FK column can be NULL (optional relationship). |
| `source_id_type` | Semantic ID type of the source column (e.g., `CatalogItemId`, `GlobalId`). |
| `requires_mapping` | Mapping table needed to bridge ID namespaces (e.g., `mz_internal.mz_object_global_ids`). |
| `from_type` | Source semantic ID type for `maps_to` links (e.g., `CatalogItemId`). |
| `to_type` | Target semantic ID type for `maps_to` links (e.g., `GlobalId`). |
| `via` | Intermediate table or view used to perform a mapping or indirect join. |
| `metric` | Name of the metric or statistic measured by a `measures` link (e.g., `cpu_time_ns`, `materialization_lag`). |
| `discriminator_column` | Column on the `union` view that identifies the member type (e.g., `type`). |
| `discriminator_value` | Value in `discriminator_column` that selects the specific member entity. |
| `note` | Free-text clarification for unusual join semantics or caveats. |

## For LLMs

If connected to a Materialize instance, query these views **before** writing
catalog queries. They help find the right tables, correct join paths, and avoid
the GlobalId/CatalogItemId trap.

### Key queries

**Find all entities related to X:**
```sql
SELECT l.name, l.source_entity, l.target_entity,
       l.properties->>'source_id_type' AS id_type
FROM mz_internal.mz_ontology_link_types l
WHERE l.source_entity = 'X' OR l.target_entity = 'X';
```

**Discover columns and types for entity Z:**
```sql
SELECT p.column_name, p.semantic_type, p.description
FROM mz_internal.mz_ontology_properties p
WHERE p.entity_type = 'Z'
ORDER BY p.column_name;
```

**Look up the actual table name for an entity:**
```sql
SELECT name, relation FROM mz_internal.mz_ontology_entity_types WHERE name = 'mv';
-- mv -> mz_catalog.mz_materialized_views
```

### GlobalId vs CatalogItemId

Many `object_id` columns in `mz_internal` and `mz_introspection` use
**GlobalId**, not **CatalogItemId**. Both are `text`, both look like `u42`,
but they are different ID namespaces. A direct join to `mz_objects.id`
(CatalogItemId) will silently return wrong results after ALTER operations.

Check `mz_ontology_properties.semantic_type` before writing joins. If the
types differ, bridge through `mz_internal.mz_object_global_ids`.

## Stats

- ~117 entity types (mz_catalog + mz_internal + mz_introspection)
- 20 semantic types
- ~450 column properties
- ~150 named relationships

## Related files

- `src/catalog/src/builtin.rs` — `Ontology` and `OntologyLink` struct definitions, per-builtin annotations
- `src/repr/src/relation.rs` — `semantic_types` field on `RelationDesc`
- `src/storage-client/src/healthcheck.rs` — semantic type annotations on status history tables
- `misc/ontology/` — SQL files for loading the same data as user-space tables
