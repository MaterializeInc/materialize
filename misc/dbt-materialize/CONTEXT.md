# misc/dbt-materialize

**Purpose:** Official dbt adapter for Materialize. Distributed as
`dbt-materialize` on PyPI; extends `dbt-postgres` to surface
Materialize-specific relation types and deployment workflows.

**Adapter pattern:** Inherits `PostgresAdapter` / `PostgresConnectionManager`
via the standard dbt adapter extension seam. Minimal Python — most
Materialize-specific logic lives in Jinja2 SQL macros.

**Python modules (`dbt/adapters/materialize/`):**
- `connections.py` — overrides `psycopg2` connect to inject Mz session params
  (`auto_route_catalog_queries`, `welcome_message=off`); enforces version gate
  (`>=0.68.0`).
- `impl.py` (`MaterializeAdapter`) — extends `PostgresAdapter`; adds
  `MaterializeIndexConfig`, `MaterializeRefreshIntervalConfig`,
  `MaterializeConfig` (cluster, refresh_interval, retain_history);
  overrides `list_relations_without_caching`, `get_column_schema_from_query`,
  `generate_final_cluster_name` for blue-green.
- `relation.py` (`MaterializeRelation`, `MaterializeRelationType`) — extends
  `PostgresRelation` with Mz-specific types: `Source`, `SourceTable`, `Sink`,
  `MaterializedView` (and legacy alias `MaterializedViewLegacy`).

**Jinja2 macros (`dbt/include/materialize/macros/`):**
- `materializations/` — custom materializations: `materialized_view.sql`,
  `source.sql`, `source_table.sql`, `sink.sql`, `materializedview.sql`
  (legacy), plus overrides for `view`, `table`, `incremental`, `test`, `unit`.
- `deploy/` — blue-green deployment: `deploy_init.sql`, `deploy_promote.sql`
  (atomic schema+cluster swap via `_dbt_deploy` suffix convention),
  `deploy_cleanup.sql`.
- `ci/` — `create_cluster.sql`, `drop_cluster.sql` for CI ephemeral clusters.
- `adapters.sql`, `catalog.sql`, `columns.sql`, `freshness.sql`,
  `relations.sql` — standard adapter overrides.

**Tests (`tests/adapter/`):** pytest suite (~27 test modules) run via mzcompose.
Covers basic, incremental, clusters, custom materializations, deploy, constraints,
relation types.

**Bubbles up to root:** misc/dbt-materialize is the external-client Adapter seam
for dbt users. The blue-green deploy pattern (`deploy_init` / `deploy_promote` /
`deploy_cleanup`) is a first-class user-facing workflow built on MZ's atomic
schema/cluster rename.
