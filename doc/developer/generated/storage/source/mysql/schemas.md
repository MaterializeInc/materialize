---
source: src/storage/src/source/mysql/schemas.rs
revision: 47a81d6e64
---

# mz-storage::source::mysql::schemas

Provides `verify_schemas`, an async helper that queries the current MySQL schema for a set of tables and compares each against the expected `SourceOutputInfo` descriptor, returning a list of outputs with incompatible schema changes.
Descriptors are compared by calling `determine_compatibility`, which matches columns by ordinal position when `binlog_full_metadata` is false, or by name when it is true.
Used by both snapshot and replication operators to surface definite schema-change errors.
