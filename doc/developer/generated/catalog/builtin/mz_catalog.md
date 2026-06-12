---
source: src/catalog/src/builtin/mz_catalog.rs
revision: 5b42d5e422
---

# catalog::builtin::mz_catalog

Defines all built-in catalog objects for the `mz_catalog` SQL schema.

This module exports 78 public items: types, tables, materialized views, views, sources, and indexes.

**Types** — Materialize-specific pseudo-types and their array forms: `TYPE_LIST`, `TYPE_MAP`, `TYPE_ANYCOMPATIBLELIST`, `TYPE_ANYCOMPATIBLEMAP`; unsigned integer types `TYPE_UINT2/4/8` and their arrays; `TYPE_MZ_TIMESTAMP`, `TYPE_MZ_TIMESTAMP_ARRAY`; `TYPE_MZ_ACL_ITEM`, `TYPE_MZ_ACL_ITEM_ARRAY`.

**Tables** — `BuiltinTable` statics covering connector-specific metadata: `MZ_ICEBERG_SINKS`, `MZ_KAFKA_SINKS`, `MZ_KAFKA_CONNECTIONS`, `MZ_KAFKA_SOURCES`, and more.

**Materialized views** — Core catalog entities backed by queries over `mz_internal.mz_catalog_raw`: `MZ_DATABASES`, `MZ_SCHEMAS`, `MZ_CONNECTIONS`, `MZ_SECRETS`, `MZ_TABLES`, `MZ_COLUMNS`, `MZ_VIEWS`, `MZ_SOURCES`, `MZ_SINKS`, `MZ_CLUSTER_REPLICAS`, `MZ_ROLES`, `MZ_ROLE_PARAMETERS`, `MZ_OBJECTS`, `MZ_ALL_OBJECTS`, and others. These carry `Ontology` annotations with entity names, descriptions, and `OntologyLink` relationships for the catalog graph. `MZ_INDEXES` is also a `BuiltinMaterializedView` but is generated dynamically via the `pub(super) make_mz_indexes` function rather than declared as a static, so it is not counted among the module's exported statics.

**Views** — Additional SQL views for derived catalog information such as `MZ_TIMEZONE_ABBREVIATIONS` and `MZ_TIMEZONE_NAMES`.

**Sources** — `BuiltinSource` items backed by `DataSourceDesc::Catalog` or `IntrospectionType`.

**Indexes** — `BuiltinIndex` constants for accelerating catalog queries: `MZ_DATABASES_IND`, `MZ_SCHEMAS_IND`, `MZ_CONNECTIONS_IND`, `MZ_TABLES_IND`, `MZ_TYPES_IND`, `MZ_OBJECTS_IND`, `MZ_COLUMNS_IND`, `MZ_SECRETS_IND`, `MZ_VIEWS_IND`, `MZ_CLUSTERS_IND`, `MZ_INDEXES_IND`, `MZ_ROLES_IND`, `MZ_SOURCES_IND`, `MZ_SINKS_IND`, `MZ_MATERIALIZED_VIEWS_IND`, `MZ_CLUSTER_REPLICAS_IND`, `MZ_CLUSTER_REPLICA_SIZES_IND`, `MZ_KAFKA_SOURCES_IND`, and more.
