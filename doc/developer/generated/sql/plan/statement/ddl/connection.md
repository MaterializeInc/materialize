---
source: src/sql/src/plan/statement/ddl/connection.rs
revision: 44d6b9ac6a
---

# mz-sql::plan::statement::ddl::connection

Contains DDL planning logic specific to `CONNECTION` objects: parsing `CREATE CONNECTION` option blocks into typed connection structs (`KafkaConnection`, `PostgresConnection`, `MySqlConnection`, `CsrConnection`, `SshConnection`, `AwsConnection`, `IcebergCatalogConnection`, etc.) and building `AlterConnectionPlan` for `ALTER CONNECTION`.
Uses `generate_extracted_config!` for `ConnectionOption` extraction and validates connection-type-specific constraints.
The `build_tunnel_definition` function (also used by CSR, Postgres, MySQL, and SQL Server connection planning) constructs a `Tunnel<ReferencedConnection>` from optional SSH tunnel, AWS PrivateLink, and `KafkaMatchingBrokerRule` inputs: no options yields `Tunnel::Direct`; an SSH tunnel yields `Tunnel::Ssh`; a default `aws_privatelink` yields `Tunnel::AwsPrivatelink`; and a non-empty list of matching rules yields `Tunnel::AwsPrivatelinks` (rule-based PrivateLink, supported for Kafka only).
Kafka connection planning calls `get_brokers_and_rules` to split `BROKERS` entries into static brokers and `MATCHING` rules, then passes the rules to `build_tunnel_definition`; use of matching rules requires the `enable_kafka_broker_matching_rules` feature flag and at least one static broker address.
