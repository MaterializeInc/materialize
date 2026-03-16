---
source: src/adapter/src/catalog/open/builtin_schema_migration.rs
revision: 2c413b395c
---

# adapter::catalog::open::builtin_schema_migration

Implements the logic for migrating existing user-created objects that depend on built-in catalog objects whose definitions have changed between versions.
When a built-in table, view, or source is altered (e.g. columns added or types changed), this module re-plans all dependent user objects so they reference the updated built-in, emitting `Op::ReplaceItem` catalog operations to update stored SQL and optimizer hints.
