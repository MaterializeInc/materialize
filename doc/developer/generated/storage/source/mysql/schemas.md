---
source: src/storage/src/source/mysql/schemas.rs
revision: 20268ad87d
---

# mz-storage::source::mysql::schemas

Provides `verify_schemas`, an async helper that queries the current MySQL schema for a set of tables and compares each against the expected `SourceOutputInfo` descriptor, returning a list of outputs with incompatible schema changes.
Before comparing descriptors, it queries `@@binlog_row_metadata` to determine whether MySQL is configured with full row metadata, and passes that flag to `determine_compatibility` to control whether columns are matched by name or positionally.
Used by both snapshot and replication operators to surface definite schema-change errors.
