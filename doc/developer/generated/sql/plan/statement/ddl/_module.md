---
source: src/sql/src/plan/statement/ddl.rs
revision: 7809257b51
---

# mz-sql::plan::statement::ddl

Plans all DDL statements; the root file covers the full breadth of catalog-modifying statements, while the `connection` submodule focuses on connection-type-specific option parsing and validation.
