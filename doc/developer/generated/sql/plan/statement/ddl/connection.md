---
source: src/sql/src/plan/statement/ddl/connection.rs
revision: 627776ad0b
---

# mz-sql::plan::statement::ddl::connection

Contains DDL planning logic specific to `CONNECTION` objects: parsing `CREATE CONNECTION` option blocks into typed connection structs (`KafkaConnection`, `PostgresConnection`, `MySqlConnection`, `CsrConnection`, `SshConnection`, `AwsConnection`, `IcebergCatalogConnection`, etc.) and building `AlterConnectionPlan` for `ALTER CONNECTION`.
Uses `generate_extracted_config!` for `ConnectionOption` extraction and validates connection-type-specific constraints.
