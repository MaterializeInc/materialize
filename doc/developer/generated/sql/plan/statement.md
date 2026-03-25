---
source: src/sql/src/plan/statement.rs
revision: 52af3ba2a1
---

# mz-sql::plan::statement

Entry point for statement planning: `handle_statement` dispatches an `Aug`-annotated `Statement` to the appropriate submodule handler, producing a `Plan`.
Defines `StatementContext` (the shared planning context holding catalog reference, session vars, and collected resolved IDs), `StatementDesc` (the output schema and parameter types for a prepared statement), and `Connection` (helper enum for connection resolution).
Submodules `acl`, `ddl`, `dml`, `raise`, `scl`, `show`, `tcl`, and `validate` implement the per-statement-category logic.
