---
source: src/storage/src/source/mysql/schemas.rs
revision: e757b4d11b
---

# mz-storage::source::mysql::schemas

Provides `verify_schemas`, an async helper that queries the current MySQL schema for a set of tables and compares each against the expected `SourceOutputInfo` descriptor, returning a list of outputs with incompatible schema changes.
Used by both snapshot and replication operators to surface definite schema-change errors.
