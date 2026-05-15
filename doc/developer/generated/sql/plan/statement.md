---
source: src/sql/src/plan/statement.rs
revision: 92df2739a1
---

# mz-sql::plan::statement

Entry point for statement planning: `describe` and `plan` dispatch an `Aug`-annotated `Statement` to the appropriate submodule handler, producing a `StatementDesc` or `(Plan, ResolvedIds)` respectively.
The `plan` function takes `resolved_ids: &ResolvedIds` (the statement's main dependency set) and returns a tuple of the plan and a separate `ResolvedIds` containing IDs discovered inside SQL-implemented function bodies; the caller is responsible for passing these to `check_plan` for the `restrict_to_user_objects` RBAC check.
Defines `StatementContext` (the shared planning context holding catalog reference, session vars, and collected resolved IDs), `StatementDesc` (the output schema and parameter types for a prepared statement), and connection-resolution helpers.
`StatementContext` carries `sql_impl_resolved_ids: Arc<Mutex<ResolvedIds>>`: resolved catalog IDs accumulated during planning of SQL-implemented built-in bodies (via `sql_impl`) and `ShowSelect` queries; these are kept separate from the statement's `resolved_ids` because they are implementation details of the functions, not real dependencies of the statement.
Connection tunnel construction (`build_tunnel_definition`) lives in `plan::statement::ddl::connection`, not in `StatementContext`.
Submodules `acl`, `ddl`, `dml`, `raise`, `scl`, `show`, `tcl`, and `validate` implement the per-statement-category logic.
