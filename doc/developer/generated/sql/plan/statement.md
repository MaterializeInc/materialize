---
source: src/sql/src/plan/statement.rs
revision: 9d0a7c3c6f
---

# mz-sql::plan::statement

Entry point for statement planning: `describe` and `plan` dispatch an `Aug`-annotated `Statement` to the appropriate submodule handler, producing a `StatementDesc` or `Plan` respectively.
Defines `StatementContext` (the shared planning context holding catalog reference, session vars, and collected resolved IDs), `StatementDesc` (the output schema and parameter types for a prepared statement), and connection-resolution helpers.
Connection tunnel construction (`build_tunnel_definition`) lives in `plan::statement::ddl::connection`, not in `StatementContext`.
Submodules `acl`, `ddl`, `dml`, `raise`, `scl`, `show`, `tcl`, and `validate` implement the per-statement-category logic.
