---
source: src/sql/src/pure/error.rs
revision: c19caa29fe
---

# mz-sql::pure::error

Defines source-specific purification error types: `PgSourcePurificationError`, `MySqlSourcePurificationError`, `SqlServerSourcePurificationError`, `KafkaSourcePurificationError`, `LoadGeneratorSourcePurificationError`, `KafkaSinkPurificationError`, `IcebergSinkPurificationError`, and `CsrPurificationError`.
Each variant carries structured context (missing schemas, unrecognized types, invalid references, etc.) and implements `thiserror::Error` for human-readable messages consumed by `PlanError`.
