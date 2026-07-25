---
source: src/sql/src/plan/statement/ddl/connection.rs
revision: 31fb870f20
---

# mz-sql::plan::statement::ddl::connection

Contains DDL planning logic specific to `CONNECTION` objects: parsing `CREATE CONNECTION` option blocks into typed connection structs (`KafkaConnection`, `PostgresConnection`, `MySqlConnection`, `CsrConnection`, `SshConnection`, `AwsConnection`, `IcebergCatalogConnection`, `GlueSchemaRegistryConnection`, `GcpConnection`, etc.) and building `AlterConnectionPlan` for `ALTER CONNECTION`.
Uses `generate_extracted_config!` for `ConnectionOption` extraction and validates connection-type-specific constraints.
The `build_tunnel_definition` function (also used by CSR, Postgres, MySQL, and SQL Server connection planning) constructs a `Tunnel<ReferencedConnection>` from optional SSH tunnel, AWS PrivateLink, and `KafkaMatchingBrokerRule` inputs: no options yields `Tunnel::Direct`; an SSH tunnel yields `Tunnel::Ssh`; a default `aws_privatelink` yields `Tunnel::AwsPrivatelink`; and a non-empty list of matching rules yields `Tunnel::AwsPrivatelinks` (rule-based PrivateLink, supported for Kafka only).
Kafka connection planning calls `get_brokers_and_rules` to split `BROKERS` entries into static brokers and `MATCHING` rules, then passes the rules to `build_tunnel_definition`; use of matching rules requires the `enable_kafka_broker_matching_rules` feature flag and at least one static broker address.
`CreateConnectionType::GlueSchemaRegistry` is planned by resolving the `AwsConnection` reference and the `Registry` name string; it rejects an empty registry name.
`CreateConnectionType::Gcp` is planned by extracting the `SERVICE ACCOUNT KEY` secret option (required) and building a `GcpConnection { credentials_json }`.
For `IcebergCatalogType::Rest`, the catalog auth is expressed via `IcebergCatalogAuth`: `OAuth { credential, scope }` when a `CREDENTIAL` is supplied, or `Gcp(GcpConnectionReference)` when a `GCP CONNECTION` is supplied (mutually exclusive; at least one is required). GCP auth is only valid with the BigLake Iceberg REST Catalog URI (`https://biglake.googleapis.com/iceberg/v1/restcatalog`). `IcebergCatalogType::S3TablesRest` does not support a GCP connection.
The `get_gcp_connection_reference` helper resolves a `GCP CONNECTION` option to a `GcpConnectionReference<ReferencedConnection>`, erroring if the referenced item is not a GCP connection.
