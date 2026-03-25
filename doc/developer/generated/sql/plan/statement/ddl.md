---
source: src/sql/src/plan/statement/ddl.rs
revision: daff501451
---

# mz-sql::plan::statement::ddl

The largest statement-planning file: implements `describe_*` and `plan_*` for all DDL statements — `CREATE`/`ALTER`/`DROP` for sources, sinks, views, materialized views, tables, indexes, connections, secrets, clusters, roles, schemas, databases, types, network policies, and more.
Delegates connection-specific logic to the `connection` submodule.
Exports `PgConfigOptionExtracted` and `PlannedRoleAttributes` for use by other modules.
