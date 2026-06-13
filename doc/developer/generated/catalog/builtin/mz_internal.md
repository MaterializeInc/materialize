---
source: src/catalog/src/builtin/mz_internal.rs
revision: 554aab2461
---

# catalog::builtin::mz_internal

Defines all built-in catalog objects for the `mz_internal` SQL schema.

This is the largest builtin submodule, exporting 186 public items: sources, tables, materialized views, views, indexes, and connections.

**Sources** (`BuiltinSource`) — Backed by `DataSourceDesc::Catalog` or `IntrospectionType`. Key items include `MZ_CATALOG_RAW` (the raw persist-backed catalog source, system-only access), and storage statistics/status history sources such as `MZ_SOURCE_STATISTICS_RAW`, `MZ_SINK_STATISTICS_RAW`, `MZ_SOURCE_STATUS_HISTORY`, `MZ_SINK_STATUS_HISTORY`, `MZ_STATEMENT_EXECUTION_HISTORY`, `MZ_SESSION_HISTORY`, `MZ_SQL_TEXT`, `MZ_PREPARED_STATEMENT_HISTORY`, and replica metrics/status history sources.

**Tables** (`BuiltinTable`) — Connector-specific and internal metadata tables such as `MZ_POSTGRES_SOURCES`, `MZ_MYSQL_SOURCES`, and others tracking sink/source details.

**Materialized views** (`BuiltinMaterializedView`) — Derived catalog views backed by queries over `mz_catalog_raw` and other sources: aggregated statistics, lag histograms, and other derived metrics.

**Views** (`BuiltinView`) — 80 entries covering the full `mz_internal` SQL surface: cluster introspection, compute operator metrics, peek durations, arrangement sizes, source/sink status, wall-clock lag, statement execution history, privilege management, dependency graph views, and more. Views carry inline SQL queries over `mz_catalog`, `mz_introspection`, and `mz_internal` tables. Access levels vary: most grant `PUBLIC_SELECT`; sensitive views use `MONITOR_SELECT`, `MONITOR_REDACTED_SELECT`, `SUPPORT_SELECT`, or `ANALYTICS_SELECT`. `MZ_MCP_DATA_PRODUCTS` and `MZ_MCP_DATA_PRODUCT_DETAILS` are MCP-agent-facing views listing data products and their column/key details that are exempted from `restrict_to_user_objects` blocking. `MZ_MCP_DATA_PRODUCT_DETAILS` includes a `hydration` column (JSONB) reporting readiness across the cluster's replicas, with fields `hydrated` (bool), `replica_count` (int), and `hydrated_replica_count` (int).
`MZ_BUILTIN_SOURCES` is a view in `mz_internal` listing builtin and log sources that do not appear in `mz_catalog_raw`; user sources are exposed via `mz_catalog.mz_sources`.

**Connections** (`BuiltinConnection`) — System-level connection definitions.

**Indexes** (`BuiltinIndex`) — 55 index constants accelerating queries on frequently accessed `mz_internal` views and tables.

The `mz_show_my_*` privilege views (`mz_show_my_system_privileges`, `mz_show_my_cluster_privileges`, `mz_show_my_database_privileges`, `mz_show_my_schema_privileges`, `mz_show_all_my_privileges`, `mz_show_my_default_privileges`) filter using `grantee = ANY(mz_internal.mz_session_role_memberships())` instead of `pg_has_role(grantee, 'USAGE')` to avoid loading the full role graph in restricted sessions.

Many items carry `Ontology` annotations with `OntologyLink` relationships (foreign keys, union views, dependency edges) for the catalog ontology graph.
