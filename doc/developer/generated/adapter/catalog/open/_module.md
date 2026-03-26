---
source: src/adapter/src/catalog/open.rs
revision: a50264ee3d
---

# adapter::catalog::open

Houses the catalog open sequence and its sub-step `builtin_schema_migration`.
The parent file implements `Catalog::open`; the `builtin_schema_migration` child handles schema migrations for builtin storage collections whose persist shard schemas change between versions; and `builtin_schema_migration_tests` provides integration test coverage.
The open sequence supports builtin materialized views (column comments, descriptor validation) and emits audit log events when removing pending cluster replicas.
