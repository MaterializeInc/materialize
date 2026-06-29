---
source: src/sql/src/plan/statement/show.rs
revision: 3a5d044167
---

# mz-sql::plan::statement::show

Plans `SHOW` statements that query catalog state: `SHOW CREATE TABLE/VIEW/SOURCE/SINK/INDEX/CONNECTION/MATERIALIZED VIEW/TYPE/CLUSTER`, `SHOW OBJECTS`, `SHOW COLUMNS`, and related.
Most `SHOW` variants are rewritten to internal `SELECT` queries against `mz_internal.mz_show_*` system views via the `ShowSelect` helper, unifying them with the standard query-planning path.
`ShowSelect::new` propagates the resolved catalog IDs from the rewritten query (including the `mz_show_*` view IDs) into `scx.sql_impl_resolved_ids`, so that `check_restrict_to_user_objects` in the RBAC layer sees those system-view accesses when evaluating the merged `resolved_ids`.
The `ensure_no_from` helper enforces that `FROM` is absent for `SHOW` commands that do not accept it (e.g., `SHOW ROLES`, `SHOW CLUSTERS`, `SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW PRIVILEGES`), returning `PlanError::Internal` if the parser somehow produced a `FROM` clause.
`plan_show_create_type` uses the fully-qualified type name (schema + item) in the output row rather than just the bare item name, and rejects system types with an error.
`humanize_sql_for_show_create` strips the `Version` option from `CREATE SINK` SQL output, since that option does not roundtrip (a SQL string containing `VERSION` cannot be used as-is to recreate the sink). It also strips the internal `DETAILS` option from `CREATE TABLE ... FROM SOURCE` statements, retaining only `TEXT COLUMNS`, `EXCLUDE COLUMNS`, `PARTITION BY`, and `RETAIN HISTORY`; the omitted options are not user-typeable but remain accessible in the raw `create_sql` and `redacted_create_sql` columns of catalog builtins. In addition, column definitions populated during purification (the `Defined` variant of `TableFromSourceColumns`) and constraints are stripped from `CREATE TABLE ... FROM SOURCE` output, since `CREATE TABLE ... FROM SOURCE` rejects column definitions as input; a user-typed `Named` column list is left intact. The full column schema remains available via `SHOW COLUMNS`, `mz_columns`, and the raw `create_sql` column of `mz_catalog.mz_tables`.
Role names embedded in `pg_has_role` filter expressions are passed through `escaped_string_literal` to ensure correct SQL quoting.
