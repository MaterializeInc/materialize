---
source: src/sql/src/plan/statement.rs
revision: 52af3ba2a1
---

# mz-sql::plan::statement

Houses all statement-level planning logic, organized by SQL language category.
The root file defines `StatementContext`, `StatementDesc`, and the main dispatch function; children cover DDL (`ddl`+`ddl/connection`), DML (`dml`), ACL (`acl`), SCL (`scl`), TCL (`tcl`), `SHOW` (`show`), `RAISE` (`raise`), and `VALIDATE CONNECTION` (`validate`).
