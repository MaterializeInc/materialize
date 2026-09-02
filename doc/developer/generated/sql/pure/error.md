---
source: src/sql/src/pure/error.rs
revision: 460108c80d
---

# mz-sql::pure::error

Defines source-specific purification error types: `PgSourcePurificationError`, `MySqlSourcePurificationError`, `SqlServerSourcePurificationError`, `KafkaSourcePurificationError`, `LoadGeneratorSourcePurificationError`, `KafkaSinkPurificationError`, `IcebergSinkPurificationError`, `CsrPurificationError`, and `GluePurificationError`.
Each variant carries structured context (missing schemas, unrecognized types, invalid references, etc.) and implements `thiserror::Error` for human-readable messages consumed by `PlanError`.
`MySqlSourcePurificationError` includes `UnsupportedBinlogMetadataSetting { setting }` (raised when the upstream `binlog_row_metadata` variable is not `FULL`) and `UnsupportedMySqlVersion { version }` (raised when the server version is below 8.0.1), both required for the `CREATE TABLE FROM SOURCE` syntax.
`GluePurificationError` covers errors during AWS Glue Schema Registry purification: `NotGlueConnection` (connection is not a `GlueSchemaRegistry` connection), `MissingSchemaName` (the `SCHEMA NAME` option is absent), `LoadSdkConfigError` (AWS SDK config load failure), `SchemaLookupError` (Glue API call failed), `EmptyDefinition` (schema version has no definition), and `UnsupportedDataFormat` (schema uses a format other than Avro).
