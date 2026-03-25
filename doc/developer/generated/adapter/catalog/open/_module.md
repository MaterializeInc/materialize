---
source: src/adapter/src/catalog/open.rs
revision: 2c413b395c
---

# adapter::catalog::open

Houses the catalog open sequence and its sub-step `builtin_schema_migration`.
The parent file implements `Catalog::open`; the `builtin_schema_migration` child re-plans user objects whose built-in dependencies changed; and `builtin_schema_migration_tests` provides integration test coverage.
