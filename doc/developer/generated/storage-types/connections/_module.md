---
source: src/storage-types/src/connections.rs
revision: 4d1c464c78
---

# storage-types::connections

Defines all connection types used to connect storage sources and sinks to external systems, along with the runtime infrastructure needed to instantiate them.
Key types include `Connection` (an enum over Kafka, CSR, Postgres, SSH, AWS, AWS PrivateLink, MySQL, SQL Server, and Iceberg Catalog connections), `ConnectionContext` (holds global runtime state such as SSH tunnel managers and secret readers), and `KafkaConnection`/`PostgresConnection`/`IcebergCatalogConnection` etc.
`IcebergCatalogConnection` holds an `IcebergCatalogImpl` (either `Rest` or `S3TablesRest`) and a URI; it implements `connect()` to return a live `Arc<dyn Catalog>`.
`AwsSdkCredentialLoader` is a private type that wraps an AWS SDK credentials provider and implements the iceberg `AwsCredentialLoad` trait, enabling refreshable assume-role credential chains for Iceberg FileIO/OpenDAL.
The submodules `aws`, `inline`, and `string_or_secret` provide supporting abstractions for AWS credential loading, reference/inlined connection polymorphism, and secret-backed string values respectively.
Connection types implement `AlterCompatible` to constrain which fields may change across an `ALTER CONNECTION`.
