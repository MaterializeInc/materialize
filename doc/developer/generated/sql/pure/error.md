---
source: src/sql/src/pure/error.rs
revision: 16d611fb45
---

# mz-sql::pure::error

Defines source-specific purification error types: `PgSourcePurificationError`, `MySqlSourcePurificationError`, `SqlServerSourcePurificationError`, `KafkaSourcePurificationError`, `LoadGeneratorSourcePurificationError`, `KafkaSinkPurificationError`, `IcebergSinkPurificationError`, and `CsrPurificationError`.
Each variant carries structured context (missing schemas, unrecognized types, invalid references, etc.) and implements `thiserror::Error` for human-readable messages consumed by `PlanError`.
`MySqlSourcePurificationError` includes `UnsupportedBinlogMetadataSetting { setting }` (raised when the upstream `binlog_row_metadata` variable is not `FULL`) and `UnsupportedMySqlVersion { version }` (raised when the server version is below 8.0.1), both required for the `CREATE TABLE FROM SOURCE` syntax.
