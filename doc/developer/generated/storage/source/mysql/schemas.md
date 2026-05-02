---
source: src/storage/src/source/mysql/schemas.rs
revision: fc8d9dc1e4
---

# mz-storage::source::mysql::schemas

Provides `verify_schemas`, an async helper that queries the current MySQL schema for a set of tables and compares each against the expected `SourceOutputInfo` descriptor, returning a list of outputs with incompatible schema changes.
Descriptors are compared by calling `determine_compatibility`, which matches columns positionally.
Used by both snapshot and replication operators to surface definite schema-change errors.
