# mz_ontology: Catalog Ontology Schema

The `mz_ontology` schema contains four system tables that describe the structure
and relationships of the Materialize system catalog. It is designed to help
LLMs, diagnostic tools, and developers discover the right tables, join paths,
and ID types when writing catalog queries.

> **Unstable.** This schema is gated behind the `unsafe_enable_unstable_dependencies`
> system parameter. To access the tables:
>
> ```sql
> ALTER SYSTEM SET unsafe_enable_unstable_dependencies = true;
> ```

## Tables

| Table | Columns | Purpose |
|---|---|---|
| `mz_ontology_entity_types` | `name, relation, properties, description` | What kinds of things exist. `properties` jsonb has `{"primary_key": ["id"]}`. |
| `mz_ontology_semantic_types` | `name, sql_type, description` | Typed ID domains and other semantic column types (CatalogItemId, GlobalId, ByteCount, etc.) |
| `mz_ontology_properties` | `entity_type, column_name, semantic_type, description` | Maps every column to its semantic type and describes what it means. |
| `mz_ontology_link_types` | `name, source_entity, target_entity, properties, description` | Named relationships between entity types. |

### Link type kinds

The `properties` jsonb in `mz_ontology_link_types` uses a `"kind"` field:

- `"foreign_key"` — column-level join with `source_column`, `target_column`, `cardinality`
- `"measures"` — a measurement/metric relationship
- `"depends_on"` — a dependency relationship
- `"maps_to"` — an ID mapping (e.g., CatalogItemId ↔ GlobalId)
- `"union"` — a UNION view includes another entity type

### Properties key vocabulary

The `properties` jsonb in `mz_ontology_link_types` is freeform but uses the
following common keys:

| Key | Description |
|---|---|
| `kind` | **(required)** Relationship kind: `foreign_key`, `measures`, `depends_on`, `maps_to`, or `union`. |
| `source_column` | Column name on the source entity used for the join. |
| `target_column` | Column name on the target entity used for the join. |
| `cardinality` | Join cardinality: `many_to_one`, `one_to_one`, `many_to_many`. |
| `nullable` | `true` if the FK column can be NULL (i.e., the relationship is optional). |
| `source_id_type` | Semantic ID type of the source column (e.g., `CatalogItemId`, `GlobalId`). Helps detect cross-namespace joins that need a mapping table. |
| `requires_mapping` | Fully-qualified mapping table (e.g., `mz_internal.mz_object_global_ids`) needed to bridge ID namespaces. |
| `metric` | For `measures` kind: the metric name (e.g., `wallclock_lag`, `arrangement_size`). |
| `discriminator_column` | For `union` kind: the column used to distinguish member types. |
| `discriminator_value` | For `union` kind: the value in `discriminator_column` that selects this member. |
| `via` | For `maps_to` kind: the mapping table used to perform the translation. |
| `from_type` / `to_type` | For `maps_to` kind: the source and target semantic ID types. |
| `note` | Freeform clarification (e.g., explaining that IDs are local operator IDs, not GlobalIds). |

The `properties` jsonb in `mz_ontology_entity_types` uses:

| Key | Description |
|---|---|
| `primary_key` | JSON array of column names forming the primary key (e.g., `["id"]`). |

## Stats

- ~110 entity types (mz_catalog + mz_internal + mz_introspection)
- 20 semantic types
- ~380 column properties
- ~110 named relationships

## For Claude / LLMs

If you are connected to a Materialize instance, use the ontology tables
**before** writing catalog queries. They will help you find the right tables,
correct join paths, and avoid the GlobalId/CatalogItemId trap.

### Three queries to run first

**1. "What relates to X?"** — find all entities connected to what you're
investigating:

```sql
SELECT l.name, l.source_entity, l.target_entity,
       l.properties->>'source_id_type' AS id_type,
       l.description
FROM mz_ontology.mz_ontology_link_types l
WHERE l.source_entity = 'X' OR l.target_entity = 'X';
```

**2. "What measures/tracks Y?"** — find diagnostic and measurement tables:

```sql
SELECT l.name, l.source_entity, e.relation,
       l.properties->>'source_id_type' AS id_type,
       l.properties->>'requires_mapping' AS needs_mapping
FROM mz_ontology.mz_ontology_link_types l
JOIN mz_ontology.mz_ontology_entity_types e ON e.name = l.source_entity
WHERE l.properties->>'kind' = 'measures'
   OR l.name LIKE '%keyword%';
```

**3. "What columns does Z have?"** — discover available data and types:

```sql
SELECT p.column_name, p.semantic_type, p.description
FROM mz_ontology.mz_ontology_properties p
WHERE p.entity_type = 'Z'
ORDER BY p.column_name;
```

### Critical: GlobalId vs CatalogItemId

Many `object_id` columns in `mz_internal` and `mz_introspection` use
**GlobalId**, not **CatalogItemId**. Both are `text`, both look like `u42`,
but they are different ID namespaces. A direct join to `mz_objects.id`
(CatalogItemId) will silently return wrong results after ALTER operations.

**Before writing a join**, check whether the source and target columns have
the same semantic type in `mz_ontology_properties`:

```sql
SELECT sp.semantic_type AS src_type, tp.semantic_type AS tgt_type
FROM mz_ontology.mz_ontology_properties sp, mz_ontology.mz_ontology_properties tp
WHERE sp.entity_type = 'SOURCE_ENTITY' AND sp.column_name = 'SOURCE_COL'
  AND tp.entity_type = 'TARGET_ENTITY' AND tp.column_name = 'TARGET_COL';
```

If they differ (e.g., `GlobalId` vs `CatalogItemId`), you need the mapping
table `mz_internal.mz_object_global_ids`:

```sql
JOIN mz_internal.mz_object_global_ids g ON table.object_id = g.global_id
JOIN mz_catalog.mz_objects o ON g.id = o.id
```

### Critical: Always use `entity_types.relation` for table names

The `name` column in `mz_ontology_entity_types` is a short alias (e.g.,
`replica`, `source`, `mv`). It is **NOT** a valid SQL table name. The
`relation` column contains the fully-qualified table name (e.g.,
`mz_catalog.mz_cluster_replicas`).

**When writing diagnostic SQL queries, ALWAYS look up the table name from
`mz_ontology_entity_types.relation`.** Never guess or abbreviate table
names — many don't match what you'd expect (e.g., `replica` →
`mz_catalog.mz_cluster_replicas`, not `mz_replicas`).

```sql
SELECT name, relation, description
FROM mz_ontology.mz_ontology_entity_types
WHERE name = 'mv';
-- mv → mz_catalog.mz_materialized_views
```

### Primary keys

```sql
SELECT name, properties->>'primary_key' AS pk
FROM mz_ontology.mz_ontology_entity_types
WHERE name = 'cluster';
-- ["id"]
```

## Implementation

The ontology data is defined as static Rust data in
`src/adapter/src/catalog/builtin_table_updates/ontology.rs` and packed into
builtin table updates during coordinator startup. The four tables are registered
as builtin tables in `src/catalog/src/builtin.rs` under the `mz_ontology` schema.
