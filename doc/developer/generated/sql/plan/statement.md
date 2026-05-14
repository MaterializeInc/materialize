---
source: src/sql/src/plan/statement.rs
revision: 3df8ae2fd8
---

# mz-sql::plan::statement

Entry point for statement planning: `describe` and `plan` dispatch an `Aug`-annotated `Statement` to the appropriate submodule handler, producing a `StatementDesc` or `Plan` respectively.
Defines `StatementContext` (the shared planning context holding catalog reference, session vars, and collected resolved IDs), `StatementDesc` (the output schema and parameter types for a prepared statement), and connection-resolution helpers.
`StatementContext` carries `sql_impl_resolved_ids: Arc<Mutex<ResolvedIds>>`: resolved catalog IDs accumulated during planning of SQL-implemented built-in bodies (via `sql_impl`) and `ShowSelect` queries are stored here, then merged into the caller's `resolved_ids` before the RBAC check so that `check_restrict_to_user_objects` sees them.
Connection tunnel construction (`build_tunnel_definition`) lives in `plan::statement::ddl::connection`, not in `StatementContext`.
Submodules `acl`, `ddl`, `dml`, `raise`, `scl`, `show`, `tcl`, and `validate` implement the per-statement-category logic.
