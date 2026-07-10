---
source: src/adapter/src/catalog/open.rs
revision: dbd2c3fc06
---

# adapter::catalog::open

Houses the catalog open sequence and its sub-step `builtin_schema_migration`.
The parent file implements `Catalog::open`; the `builtin_schema_migration` child handles schema migrations for builtin storage collections whose persist shard schemas change between versions; and `builtin_schema_migration_tests` provides integration test coverage.
The open sequence supports builtin materialized views (column comments, descriptor validation) and emits audit log events when creating or removing builtin clusters, when creating or removing builtin cluster replicas, and when removing pending cluster replicas. Builtin replica IDs are allocated via the transaction-level system allocator (`txn.allocate_system_replica_id()`), which is single-source inside the open transaction because there is no coordinator at that point. When migrating a builtin, `add_new_remove_old_builtin_items_migration` drops comments under all relation-style `CommentObjectId` variants (Table, View, MaterializedView, Source) for the affected id, so that stale comment rows are cleaned up when a builtin's type changes.
