---
source: src/sql/src/plan/statement/show.rs
revision: 234d77f6b5
---

# mz-sql::plan::statement::show

Plans `SHOW` statements that query catalog state: `SHOW CREATE TABLE/VIEW/SOURCE/SINK/INDEX/CONNECTION/MATERIALIZED VIEW/TYPE/CLUSTER`, `SHOW OBJECTS`, `SHOW COLUMNS`, and related.
Most `SHOW` variants are rewritten to internal `SELECT` queries against `mz_internal.mz_show_*` system views via the `ShowSelect` helper, unifying them with the standard query-planning path.
`ShowSelect::new` propagates the resolved catalog IDs from the rewritten query (including the `mz_show_*` view IDs) into `scx.sql_impl_resolved_ids`, so that `check_restrict_to_user_objects` in the RBAC layer sees those system-view accesses when evaluating the merged `resolved_ids`.
The `ensure_no_from` helper enforces that `FROM` is absent for `SHOW` commands that do not accept it (e.g., `SHOW ROLES`, `SHOW CLUSTERS`, `SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW PRIVILEGES`), returning `PlanError::Internal` if the parser somehow produced a `FROM` clause.
`plan_show_create_type` uses the fully-qualified type name (schema + item) in the output row rather than just the bare item name, and rejects system types with an error.
