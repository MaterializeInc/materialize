---
source: src/sql/src/plan/statement.rs
revision: 44d6b9ac6a
---

# mz-sql::plan::statement

Houses all statement-level planning logic, organized by SQL language category.
The root file defines `StatementContext`, `StatementDesc`, and the main dispatch function; children cover DDL (`ddl`+`ddl/connection`), DML (`dml`), ACL (`acl`), SCL (`scl`), TCL (`tcl`), `SHOW` (`show`), `RAISE` (`raise`), and `VALIDATE CONNECTION` (`validate`).
