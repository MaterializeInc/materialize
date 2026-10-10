---
source: src/sql/src/pure/error.rs
revision: 0c7c2a15c1
---

# mz-sql::pure::error

Defines source-specific purification error types: `PgSourcePurificationError`, `MySqlSourcePurificationError`, `SqlServerSourcePurificationError`, `KafkaSourcePurificationError`, `LoadGeneratorSourcePurificationError`, `KafkaSinkPurificationError`, `IcebergSinkPurificationError`, `CsrPurificationError`, and `GluePurificationError`.
Each variant carries structured context (missing schemas, unrecognized types, invalid references, etc.) and implements `thiserror::Error` for human-readable messages consumed by `PlanError`.
`MySqlSourcePurificationError` includes `UnsupportedBinlogMetadataSetting { setting }` (raised when the upstream `binlog_row_metadata` variable is not `FULL`) and `UnsupportedMySqlVersion { version }` (raised when the server version is below 8.0.1), both required for the `CREATE TABLE FROM SOURCE` syntax. It also includes `ConstraintsNotFound { table, constraints }` (raised when constraint names listed in `EXCLUDE CONSTRAINTS` do not match any key on the upstream MySQL table; the hint notes that the primary key's index is named `PRIMARY` and matching is exact and case-sensitive).
`PgSourcePurificationError` includes a `ConstraintsNotFound { table, constraints }` variant (raised when constraint names listed in `EXCLUDE CONSTRAINTS` do not match any `PRIMARY KEY` or `UNIQUE` constraint on the upstream table; the hint reports the expected exact case-sensitive names).
`IcebergSinkPurificationError` includes `CatalogError` (catalog connection failed), `AwsSdkContextError` (AWS SDK config load failed), and `S3TablesRegionMismatch { s3_tables_region, environment_region }` (the S3 Tables connection is configured for a different AWS region than the Materialize environment; the hint suggests creating a new AWS connection with the correct region).
`SqlServerSourcePurificationError` covers errors specific to SQL Server source purification, including `NotSqlServerConnection`, `UserSpecifiedDetails`, `UnnecessaryOptionsWithoutReferences`, `RequiresExternalReferences`, `DanglingColumns`, `MultiplePrimaryKeys`, `UnsupportedColumn`, `AllColumnsExcluded`, `NoTables`, `ProgrammingError`, `NoStartLsn`, and `CdcMissingColumns`.
`GluePurificationError` covers errors during AWS Glue Schema Registry purification: `NotGlueConnection` (connection is not a `GlueSchemaRegistry` connection), `MissingSchemaName` (the `SCHEMA NAME` option is absent), `LoadSdkConfigError` (AWS SDK config load failure), `SchemaLookupError` (Glue API call failed), `EmptyDefinition` (schema version has no definition), and `UnsupportedDataFormat` (schema uses a format other than Avro).
