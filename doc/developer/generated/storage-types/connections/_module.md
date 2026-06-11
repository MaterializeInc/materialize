---
source: src/storage-types/src/connections.rs
revision: 5173c50671
---

# storage-types::connections

Defines all connection types used to connect storage sources and sinks to external systems, along with the runtime infrastructure needed to instantiate them.
Key types include `Connection` (an enum over Kafka, CSR, Postgres, SSH, AWS, AWS PrivateLink, GCP, MySQL, SQL Server, Glue Schema Registry, and Iceberg Catalog connections), `ConnectionContext` (holds global runtime state such as SSH tunnel managers and secret readers), and `KafkaConnection`/`PostgresConnection`/`IcebergCatalogConnection` etc.
`IcebergCatalogConnection` holds an `IcebergCatalogImpl` (either `Rest` or `S3TablesRest`) and a URI; it implements `connect()` to return a live `Arc<dyn Catalog>`.
`AwsSdkCredentialLoader` is a private type that wraps an AWS SDK credentials provider and implements the iceberg `AwsCredentialLoad` trait, enabling refreshable assume-role credential chains for Iceberg FileIO/OpenDAL; its `load_credential` method accepts a `reqwest::Client`.
`Sigv4Authenticator` is a private type that implements `RequestAuthenticator` for the `iceberg-catalog-rest` crate, signing each outgoing REST catalog request with AWS SigV4 using a `SharedCredentialsProvider` (so credentials are refreshed per-request). It carries `provider`, `region`, and `signing_name` fields (e.g. `"s3tables"` for S3 Tables).
`IcebergCatalogAuth` is an enum with variants `OAuth { credential, scope }` (standard Iceberg REST OAuth2 flow) and `Gcp(GcpConnectionReference)` (GCP service-account-based auth for BigLake/Lakehouse catalogs). `RestIcebergCatalog` holds an `auth: IcebergCatalogAuth` field and an optional `warehouse`.
The submodules `aws`, `gcp`, `inline`, and `string_or_secret` provide supporting abstractions for AWS credential loading, GCP credential loading, reference/inlined connection polymorphism, and secret-backed string values respectively.
Connection types implement `AlterCompatible` to constrain which fields may change across an `ALTER CONNECTION`.

`Tunnel<C>` is an enum with variants `Direct`, `Ssh(SshTunnel<C>)`, `AwsPrivatelink(AwsPrivatelink)`, and `AwsPrivatelinks(AwsPrivatelinks)`. The `AwsPrivatelinks` variant routes broker connections through an ordered list of pattern-based PrivateLink rules.

`AwsPrivatelinks` holds `rules: Vec<AwsPrivatelinkRule>`. Exact-match rules (no wildcards) serve as bootstrap brokers; wildcard rules are applied dynamically to discovered brokers.

`AwsPrivatelinkRule` holds `pattern: ConnectionRulePattern` and `to: AwsPrivatelink`, mapping a broker address pattern to a specific PrivateLink connection.

`KafkaConnection::create_with_context` handles the `Tunnel::AwsPrivatelinks` variant by validating that brokers are non-empty and setting the default tunnel to `TunnelConfig::Rules(HostMappingRules)`. When iterating per-broker tunnels, encountering `Tunnel::AwsPrivatelinks` on an individually predefined broker panics with `unreachable!()`, as rule-based routing does not apply there.

Private helpers on `KafkaConnection`: `from_default_aws_privatelink(pl: &AwsPrivatelink) -> BrokerRewrite` builds a rewrite for the bootstrap (default) PrivateLink connection without availability zone support; `from_aws_privatelink(pl: &AwsPrivatelink) -> BrokerRewrite` builds a rewrite for a specific broker PrivateLink connection with availability zone support; `from_aws_privatelink_rule(rule: &AwsPrivatelinkRule) -> (ConnectionRulePattern, BrokerRewrite)` converts a rule to a `(pattern, rewrite)` pair; `from_aws_privatelinks(pl: &AwsPrivatelinks) -> HostMappingRules` converts the full set of rules to a `HostMappingRules` value.
