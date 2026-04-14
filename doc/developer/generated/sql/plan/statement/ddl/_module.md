---
source: src/sql/src/plan/statement/ddl.rs
revision: e28be51f38
---

# mz-sql::plan::statement::ddl

Plans all DDL statements; the root file covers the full breadth of catalog-modifying statements, while the `connection` submodule focuses on connection-type-specific option parsing and validation.
Iceberg sinks support `MODE UPSERT` and `MODE APPEND`; append mode prohibits a KEY and rejects source columns that conflict with system-managed append columns.
