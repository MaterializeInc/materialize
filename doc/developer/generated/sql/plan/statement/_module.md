---
source: src/sql/src/plan/statement.rs
revision: 3df8ae2fd8
---

# mz-sql::plan::statement

Houses all statement-level planning logic, organized by SQL language category.
The root file defines `StatementContext`, `StatementDesc`, and the main dispatch function; children cover DDL (`ddl`+`ddl/connection`), DML (`dml`), ACL (`acl`), SCL (`scl`), TCL (`tcl`), `SHOW` (`show`), `RAISE` (`raise`), and `VALIDATE CONNECTION` (`validate`).
`StatementContext` carries `sql_impl_resolved_ids: Arc<Mutex<ResolvedIds>>` to accumulate catalog IDs resolved inside SQL-implemented built-in bodies and `ShowSelect` queries; these are merged into the caller's `resolved_ids` before the RBAC check.
