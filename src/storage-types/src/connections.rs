// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection types.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use itertools::Itertools;
use mz_ccsr::tls::{Certificate, Identity};
use mz_cloud_resources::{AwsExternalIdPrefix, CloudResourceReader, vpc_endpoint_host};
use mz_dyncfg::ConfigSet;
use mz_kafka_util::client::{
    BrokerAddr, BrokerRewrite, MzClientContext, MzKafkaError, TunnelConfig, TunnelingClientContext,
};
use mz_ore::assert_none;
use mz_ore::error::ErrorExt;
use mz_ore::future::{InTask, OreFutureExt};
use mz_ore::netio::resolve_address;
use mz_ore::num::NonNeg;
use mz_postgres_util::tunnel::PostgresFlavor;
use mz_proto::tokio_postgres::any_ssl_mode;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::url::any_url;
use mz_repr::{CatalogItemId, GlobalId};
use mz_secrets::SecretsReader;
use mz_ssh_util::keys::SshKeyPair;
use mz_ssh_util::tunnel::SshTunnelConfig;
use mz_ssh_util::tunnel_manager::{ManagedSshTunnelHandle, SshTunnelManager};
use mz_tls_util::Pkcs12Archive;
use mz_tracing::CloneableEnvFilter;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use rdkafka::ClientContext;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::net;
use tokio::runtime::Handle;
use tokio_postgres::config::SslMode;
use tracing::{debug, warn};
use url::Url;

use crate::AlterCompatible;
use crate::configuration::StorageConfiguration;
use crate::connections::aws::{
    AwsConnection, AwsConnectionReference, AwsConnectionValidationError,
};
use crate::connections::string_or_secret::StringOrSecret;
use crate::controller::AlterError;
use crate::dyncfgs::{
    ENFORCE_EXTERNAL_ADDRESSES, KAFKA_CLIENT_ID_ENRICHMENT_RULES,
    KAFKA_DEFAULT_AWS_PRIVATELINK_ENDPOINT_IDENTIFICATION_ALGORITHM,
};
use crate::errors::{ContextCreationError, CsrConnectError};

pub mod aws;
pub mod inline;
pub mod string_or_secret;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.connections.rs"));

/// An extension trait for [`SecretsReader`]
#[async_trait::async_trait]
trait SecretsReaderExt {
    /// `SecretsReader::read`, but optionally run in a task.
    async fn read_in_task_if(
        &self,
        in_task: InTask,
        id: CatalogItemId,
    ) -> Result<Vec<u8>, anyhow::Error>;

    /// `SecretsReader::read_string`, but optionally run in a task.
    async fn read_string_in_task_if(
        &self,
        in_task: InTask,
        id: CatalogItemId,
    ) -> Result<String, anyhow::Error>;
}

#[async_trait::async_trait]
impl SecretsReaderExt for Arc<dyn SecretsReader> {
    async fn read_in_task_if(
        &self,
        in_task: InTask,
        id: CatalogItemId,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let sr = Arc::clone(self);
        async move { sr.read(id).await }
            .run_in_task_if(in_task, || "secrets_reader_read".to_string())
            .await
    }
    async fn read_string_in_task_if(
        &self,
        in_task: InTask,
        id: CatalogItemId,
    ) -> Result<String, anyhow::Error> {
        let sr = Arc::clone(self);
        async move { sr.read_string(id).await }
            .run_in_task_if(in_task, || "secrets_reader_read".to_string())
            .await
    }
}

/// Extra context to pass through when instantiating a connection for a source
/// or sink.
///
/// Should be kept cheaply cloneable.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// An opaque identifier for the environment in which this process is
    /// running.
    ///
    /// The storage layer is intentionally unaware of the structure within this
    /// identifier. Higher layers of the stack can make use of that structure,
    /// but the storage layer should be oblivious to it.
    pub environment_id: String,
    /// The level for librdkafka's logs.
    pub librdkafka_log_level: tracing::Level,
    /// A prefix for an external ID to use for all AWS AssumeRole operations.
    pub aws_external_id_prefix: Option<AwsExternalIdPrefix>,
    /// The ARN for a Materialize-controlled role to assume before assuming
    /// a customer's requested role for an AWS connection.
    pub aws_connection_role_arn: Option<String>,
    /// A secrets reader.
    pub secrets_reader: Arc<dyn SecretsReader>,
    /// A cloud resource reader, if supported in this configuration.
    pub cloud_resource_reader: Option<Arc<dyn CloudResourceReader>>,
    /// A manager for SSH tunnels.
    pub ssh_tunnel_manager: SshTunnelManager,
}

impl ConnectionContext {
    /// Constructs a new connection context from command line arguments.
    ///
    /// **WARNING:** it is critical for security that the `aws_external_id` be
    /// provided by the operator of the Materialize service (i.e., via a CLI
    /// argument or environment variable) and not the end user of Materialize
    /// (e.g., via a configuration option in a SQL statement). See
    /// [`AwsExternalIdPrefix`] for details.
    pub fn from_cli_args(
        environment_id: String,
        startup_log_level: &CloneableEnvFilter,
        aws_external_id_prefix: Option<AwsExternalIdPrefix>,
        aws_connection_role_arn: Option<String>,
        secrets_reader: Arc<dyn SecretsReader>,
        cloud_resource_reader: Option<Arc<dyn CloudResourceReader>>,
    ) -> ConnectionContext {
        ConnectionContext {
            environment_id,
            librdkafka_log_level: mz_ore::tracing::crate_level(
                &startup_log_level.clone().into(),
                "librdkafka",
            ),
            aws_external_id_prefix,
            aws_connection_role_arn,
            secrets_reader,
            cloud_resource_reader,
            ssh_tunnel_manager: SshTunnelManager::default(),
        }
    }

    /// Constructs a new connection context for usage in tests.
    pub fn for_tests(secrets_reader: Arc<dyn SecretsReader>) -> ConnectionContext {
        ConnectionContext {
            environment_id: "test-environment-id".into(),
            librdkafka_log_level: tracing::Level::INFO,
            aws_external_id_prefix: Some(
                AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable(
                    "test-aws-external-id-prefix",
                )
                .expect("infallible"),
            ),
            aws_connection_role_arn: Some(
                "arn:aws:iam::123456789000:role/MaterializeConnection".into(),
            ),
            secrets_reader,
            cloud_resource_reader: None,
            ssh_tunnel_manager: SshTunnelManager::default(),
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Connection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaConnection<C>),
    Csr(CsrConnection<C>),
    Postgres(PostgresConnection<C>),
    Ssh(SshConnection),
    Aws(AwsConnection),
    AwsPrivatelink(AwsPrivatelinkConnection),
    MySql(MySqlConnection<C>),
    SqlServer(SqlServerConnectionDetails<C>),
}

impl<R: ConnectionResolver> IntoInlineConnection<Connection, R>
    for Connection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> Connection {
        match self {
            Connection::Kafka(kafka) => Connection::Kafka(kafka.into_inline_connection(r)),
            Connection::Csr(csr) => Connection::Csr(csr.into_inline_connection(r)),
            Connection::Postgres(pg) => Connection::Postgres(pg.into_inline_connection(r)),
            Connection::Ssh(ssh) => Connection::Ssh(ssh),
            Connection::Aws(aws) => Connection::Aws(aws),
            Connection::AwsPrivatelink(awspl) => Connection::AwsPrivatelink(awspl),
            Connection::MySql(mysql) => Connection::MySql(mysql.into_inline_connection(r)),
            Connection::SqlServer(sql_server) => {
                Connection::SqlServer(sql_server.into_inline_connection(r))
            }
        }
    }
}

impl<C: ConnectionAccess> Connection<C> {
    /// Whether this connection should be validated by default on creation.
    pub fn validate_by_default(&self) -> bool {
        match self {
            Connection::Kafka(conn) => conn.validate_by_default(),
            Connection::Csr(conn) => conn.validate_by_default(),
            Connection::Postgres(conn) => conn.validate_by_default(),
            Connection::Ssh(conn) => conn.validate_by_default(),
            Connection::Aws(conn) => conn.validate_by_default(),
            Connection::AwsPrivatelink(conn) => conn.validate_by_default(),
            Connection::MySql(conn) => conn.validate_by_default(),
            Connection::SqlServer(conn) => conn.validate_by_default(),
        }
    }
}

impl Connection<InlinedConnection> {
    /// Validates this connection by attempting to connect to the upstream system.
    pub async fn validate(
        &self,
        id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), ConnectionValidationError> {
        match self {
            Connection::Kafka(conn) => conn.validate(id, storage_configuration).await?,
            Connection::Csr(conn) => conn.validate(id, storage_configuration).await?,
            Connection::Postgres(conn) => conn.validate(id, storage_configuration).await?,
            Connection::Ssh(conn) => conn.validate(id, storage_configuration).await?,
            Connection::Aws(conn) => conn.validate(id, storage_configuration).await?,
            Connection::AwsPrivatelink(conn) => conn.validate(id, storage_configuration).await?,
            Connection::MySql(conn) => conn.validate(id, storage_configuration).await?,
            Connection::SqlServer(conn) => conn.validate(id, storage_configuration).await?,
        }
        Ok(())
    }

    pub fn unwrap_kafka(self) -> <InlinedConnection as ConnectionAccess>::Kafka {
        match self {
            Self::Kafka(conn) => conn,
            o => unreachable!("{o:?} is not a Kafka connection"),
        }
    }

    pub fn unwrap_pg(self) -> <InlinedConnection as ConnectionAccess>::Pg {
        match self {
            Self::Postgres(conn) => conn,
            o => unreachable!("{o:?} is not a Postgres connection"),
        }
    }

    pub fn unwrap_mysql(self) -> <InlinedConnection as ConnectionAccess>::MySql {
        match self {
            Self::MySql(conn) => conn,
            o => unreachable!("{o:?} is not a MySQL connection"),
        }
    }

    pub fn unwrap_sql_server(self) -> <InlinedConnection as ConnectionAccess>::SqlServer {
        match self {
            Self::SqlServer(conn) => conn,
            o => unreachable!("{o:?} is not a SQL Server connection"),
        }
    }

    pub fn unwrap_aws(self) -> <InlinedConnection as ConnectionAccess>::Aws {
        match self {
            Self::Aws(conn) => conn,
            o => unreachable!("{o:?} is not an AWS connection"),
        }
    }

    pub fn unwrap_ssh(self) -> <InlinedConnection as ConnectionAccess>::Ssh {
        match self {
            Self::Ssh(conn) => conn,
            o => unreachable!("{o:?} is not an SSH connection"),
        }
    }

    pub fn unwrap_csr(self) -> <InlinedConnection as ConnectionAccess>::Csr {
        match self {
            Self::Csr(conn) => conn,
            o => unreachable!("{o:?} is not a Kafka connection"),
        }
    }
}

/// An error returned by [`Connection::validate`].
#[derive(thiserror::Error, Debug)]
pub enum ConnectionValidationError {
    #[error(transparent)]
    Aws(#[from] AwsConnectionValidationError),
    #[error("{}", .0.display_with_causes())]
    Other(#[from] anyhow::Error),
}

impl ConnectionValidationError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            ConnectionValidationError::Aws(e) => e.detail(),
            ConnectionValidationError::Other(_) => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            ConnectionValidationError::Aws(e) => e.hint(),
            ConnectionValidationError::Other(_) => None,
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for Connection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        match (self, other) {
            (Self::Aws(s), Self::Aws(o)) => s.alter_compatible(id, o),
            (Self::AwsPrivatelink(s), Self::AwsPrivatelink(o)) => s.alter_compatible(id, o),
            (Self::Ssh(s), Self::Ssh(o)) => s.alter_compatible(id, o),
            (Self::Csr(s), Self::Csr(o)) => s.alter_compatible(id, o),
            (Self::Kafka(s), Self::Kafka(o)) => s.alter_compatible(id, o),
            (Self::Postgres(s), Self::Postgres(o)) => s.alter_compatible(id, o),
            (Self::MySql(s), Self::MySql(o)) => s.alter_compatible(id, o),
            _ => {
                tracing::warn!(
                    "Connection incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );
                Err(AlterError { id })
            }
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AwsPrivatelinkConnection {
    pub service_name: String,
    pub availability_zones: Vec<String>,
}

impl AlterCompatible for AwsPrivatelinkConnection {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        // Every element of the AwsPrivatelinkConnection connection is configurable.
        Ok(())
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaTlsConfig {
    pub identity: Option<TlsIdentity>,
    pub root_cert: Option<StringOrSecret>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct KafkaSaslConfig<C: ConnectionAccess = InlinedConnection> {
    pub mechanism: String,
    pub username: StringOrSecret,
    pub password: Option<CatalogItemId>,
    pub aws: Option<AwsConnectionReference<C>>,
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSaslConfig, R>
    for KafkaSaslConfig<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSaslConfig {
        KafkaSaslConfig {
            mechanism: self.mechanism,
            username: self.username,
            password: self.password,
            aws: self.aws.map(|aws| aws.into_inline_connection(&r)),
        }
    }
}

/// Specifies a Kafka broker in a [`KafkaConnection`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaBroker<C: ConnectionAccess = InlinedConnection> {
    /// The address of the Kafka broker.
    pub address: String,
    /// An optional tunnel to use when connecting to the broker.
    pub tunnel: Tunnel<C>,
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaBroker, R>
    for KafkaBroker<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaBroker {
        let KafkaBroker { address, tunnel } = self;
        KafkaBroker {
            address,
            tunnel: tunnel.into_inline_connection(r),
        }
    }
}

impl RustType<ProtoKafkaBroker> for KafkaBroker {
    fn into_proto(&self) -> ProtoKafkaBroker {
        ProtoKafkaBroker {
            address: self.address.into_proto(),
            tunnel: Some(self.tunnel.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaBroker) -> Result<Self, TryFromProtoError> {
        Ok(KafkaBroker {
            address: proto.address.into_rust()?,
            tunnel: proto
                .tunnel
                .into_rust_if_some("ProtoKafkaConnection::tunnel")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct KafkaTopicOptions {
    /// The replication factor for the topic.
    /// If `None`, the broker default will be used.
    pub replication_factor: Option<NonNeg<i32>>,
    /// The number of partitions to create.
    /// If `None`, the broker default will be used.
    pub partition_count: Option<NonNeg<i32>>,
    /// The initial configuration parameters for the topic.
    pub topic_config: BTreeMap<String, String>,
}

impl RustType<ProtoKafkaTopicOptions> for KafkaTopicOptions {
    fn into_proto(&self) -> ProtoKafkaTopicOptions {
        ProtoKafkaTopicOptions {
            replication_factor: self.replication_factor.map(|f| *f),
            partition_count: self.partition_count.map(|f| *f),
            topic_config: self.topic_config.clone(),
        }
    }

    fn from_proto(proto: ProtoKafkaTopicOptions) -> Result<Self, TryFromProtoError> {
        Ok(KafkaTopicOptions {
            replication_factor: proto.replication_factor.map(NonNeg::try_from).transpose()?,
            partition_count: proto.partition_count.map(NonNeg::try_from).transpose()?,
            topic_config: proto.topic_config,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaConnection<C: ConnectionAccess = InlinedConnection> {
    pub brokers: Vec<KafkaBroker<C>>,
    /// A tunnel through which to route traffic,
    /// that can be overridden for individual brokers
    /// in `brokers`.
    pub default_tunnel: Tunnel<C>,
    pub progress_topic: Option<String>,
    pub progress_topic_options: KafkaTopicOptions,
    pub options: BTreeMap<String, StringOrSecret>,
    pub tls: Option<KafkaTlsConfig>,
    pub sasl: Option<KafkaSaslConfig<C>>,
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaConnection, R>
    for KafkaConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaConnection {
        let KafkaConnection {
            brokers,
            progress_topic,
            progress_topic_options,
            default_tunnel,
            options,
            tls,
            sasl,
        } = self;

        let brokers = brokers
            .into_iter()
            .map(|broker| broker.into_inline_connection(&r))
            .collect();

        KafkaConnection {
            brokers,
            progress_topic,
            progress_topic_options,
            default_tunnel: default_tunnel.into_inline_connection(&r),
            options,
            tls,
            sasl: sasl.map(|sasl| sasl.into_inline_connection(&r)),
        }
    }
}

impl<C: ConnectionAccess> KafkaConnection<C> {
    /// Returns the name of the progress topic to use for the connection.
    ///
    /// The caller is responsible for providing the connection ID as it is not
    /// known to `KafkaConnection`.
    pub fn progress_topic(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
    ) -> Cow<str> {
        if let Some(progress_topic) = &self.progress_topic {
            Cow::Borrowed(progress_topic)
        } else {
            Cow::Owned(format!(
                "_materialize-progress-{}-{}",
                connection_context.environment_id, connection_id,
            ))
        }
    }

    fn validate_by_default(&self) -> bool {
        true
    }
}

impl KafkaConnection {
    /// Generates a string that can be used as the base for a configuration ID
    /// (e.g., `client.id`, `group.id`, `transactional.id`) for a Kafka source
    /// or sink.
    pub fn id_base(
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        object_id: GlobalId,
    ) -> String {
        format!(
            "materialize-{}-{}-{}",
            connection_context.environment_id, connection_id, object_id,
        )
    }

    /// Enriches the provided `client_id` according to any enrichment rules in
    /// the `kafka_client_id_enrichment_rules` configuration parameter.
    pub fn enrich_client_id(&self, configs: &ConfigSet, client_id: &mut String) {
        #[derive(Debug, Deserialize)]
        struct EnrichmentRule {
            #[serde(deserialize_with = "deserialize_regex")]
            pattern: Regex,
            payload: String,
        }

        fn deserialize_regex<'de, D>(deserializer: D) -> Result<Regex, D::Error>
        where
            D: Deserializer<'de>,
        {
            let buf = String::deserialize(deserializer)?;
            Regex::new(&buf).map_err(serde::de::Error::custom)
        }

        let rules = KAFKA_CLIENT_ID_ENRICHMENT_RULES.get(configs);
        let rules = match serde_json::from_value::<Vec<EnrichmentRule>>(rules) {
            Ok(rules) => rules,
            Err(e) => {
                warn!(%e, "failed to decode kafka_client_id_enrichment_rules");
                return;
            }
        };

        // Check every rule against every broker. Rules are matched in the order
        // that they are specified. It is usually a configuration error if
        // multiple rules match the same list of Kafka brokers, but we
        // nonetheless want to provide well defined semantics.
        debug!(?self.brokers, "evaluating client ID enrichment rules");
        for rule in rules {
            let is_match = self
                .brokers
                .iter()
                .any(|b| rule.pattern.is_match(&b.address));
            debug!(?rule, is_match, "evaluated client ID enrichment rule");
            if is_match {
                client_id.push('-');
                client_id.push_str(&rule.payload);
            }
        }
    }

    /// Creates a Kafka client for the connection.
    pub async fn create_with_context<C, T>(
        &self,
        storage_configuration: &StorageConfiguration,
        context: C,
        extra_options: &BTreeMap<&str, String>,
        in_task: InTask,
    ) -> Result<T, ContextCreationError>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<TunnelingClientContext<C>>,
    {
        let mut options = self.options.clone();

        // Ensure that Kafka topics are *not* automatically created when
        // consuming, producing, or fetching metadata for a topic. This ensures
        // that we don't accidentally create topics with the wrong number of
        // partitions.
        options.insert("allow.auto.create.topics".into(), "false".into());

        let brokers = match &self.default_tunnel {
            Tunnel::AwsPrivatelink(t) => {
                assert!(&self.brokers.is_empty());

                let algo = KAFKA_DEFAULT_AWS_PRIVATELINK_ENDPOINT_IDENTIFICATION_ALGORITHM
                    .get(storage_configuration.config_set());
                options.insert("ssl.endpoint.identification.algorithm".into(), algo.into());

                // When using a default privatelink tunnel broker/brokers cannot be specified
                // instead the tunnel connection_id and port are used for the initial connection.
                format!(
                    "{}:{}",
                    vpc_endpoint_host(
                        t.connection_id,
                        None, // Default tunnel does not support availability zones.
                    ),
                    t.port.unwrap_or(9092)
                )
            }
            _ => self.brokers.iter().map(|b| &b.address).join(","),
        };
        options.insert("bootstrap.servers".into(), brokers.into());
        let security_protocol = match (self.tls.is_some(), self.sasl.is_some()) {
            (false, false) => "PLAINTEXT",
            (true, false) => "SSL",
            (false, true) => "SASL_PLAINTEXT",
            (true, true) => "SASL_SSL",
        };
        options.insert("security.protocol".into(), security_protocol.into());
        if let Some(tls) = &self.tls {
            if let Some(root_cert) = &tls.root_cert {
                options.insert("ssl.ca.pem".into(), root_cert.clone());
            }
            if let Some(identity) = &tls.identity {
                options.insert("ssl.key.pem".into(), StringOrSecret::Secret(identity.key));
                options.insert("ssl.certificate.pem".into(), identity.cert.clone());
            }
        }
        if let Some(sasl) = &self.sasl {
            options.insert("sasl.mechanisms".into(), (&sasl.mechanism).into());
            options.insert("sasl.username".into(), sasl.username.clone());
            if let Some(password) = sasl.password {
                options.insert("sasl.password".into(), StringOrSecret::Secret(password));
            }
        }

        let mut config = mz_kafka_util::client::create_new_client_config(
            storage_configuration
                .connection_context
                .librdkafka_log_level,
            storage_configuration.parameters.kafka_timeout_config,
        );
        for (k, v) in options {
            config.set(
                k,
                v.get_string(
                    in_task,
                    &storage_configuration.connection_context.secrets_reader,
                )
                .await
                .context("reading kafka secret")?,
            );
        }
        for (k, v) in extra_options {
            config.set(*k, v);
        }

        let aws_config = match self.sasl.as_ref().and_then(|sasl| sasl.aws.as_ref()) {
            None => None,
            Some(aws) => Some(
                aws.connection
                    .load_sdk_config(
                        &storage_configuration.connection_context,
                        aws.connection_id,
                        in_task,
                    )
                    .await?,
            ),
        };

        // TODO(roshan): Implement enforcement of external address validation once
        // rdkafka client has been updated to support providing multiple resolved
        // addresses for brokers
        let mut context = TunnelingClientContext::new(
            context,
            Handle::current(),
            storage_configuration
                .connection_context
                .ssh_tunnel_manager
                .clone(),
            storage_configuration.parameters.ssh_timeout_config,
            aws_config,
            in_task,
        );

        match &self.default_tunnel {
            Tunnel::Direct => {
                // By default, don't offer a default override for broker address lookup.
            }
            Tunnel::AwsPrivatelink(pl) => {
                context.set_default_tunnel(TunnelConfig::StaticHost(vpc_endpoint_host(
                    pl.connection_id,
                    None, // Default tunnel does not support availability zones.
                )));
            }
            Tunnel::Ssh(ssh_tunnel) => {
                let secret = storage_configuration
                    .connection_context
                    .secrets_reader
                    .read_in_task_if(in_task, ssh_tunnel.connection_id)
                    .await?;
                let key_pair = SshKeyPair::from_bytes(&secret)?;

                // Ensure any ssh-bastion address we connect to is resolved to an external address.
                let resolved = resolve_address(
                    &ssh_tunnel.connection.host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                context.set_default_tunnel(TunnelConfig::Ssh(SshTunnelConfig {
                    host: resolved
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<BTreeSet<_>>(),
                    port: ssh_tunnel.connection.port,
                    user: ssh_tunnel.connection.user.clone(),
                    key_pair,
                }));
            }
        }

        for broker in &self.brokers {
            let mut addr_parts = broker.address.splitn(2, ':');
            let addr = BrokerAddr {
                host: addr_parts
                    .next()
                    .context("BROKER is not address:port")?
                    .into(),
                port: addr_parts
                    .next()
                    .unwrap_or("9092")
                    .parse()
                    .context("parsing BROKER port")?,
            };
            match &broker.tunnel {
                Tunnel::Direct => {
                    // By default, don't override broker address lookup.
                    //
                    // N.B.
                    //
                    // We _could_ pre-setup the default ssh tunnel for all known brokers here, but
                    // we avoid doing because:
                    // - Its not necessary.
                    // - Not doing so makes it easier to test the `FailedDefaultSshTunnel` path
                    // in the `TunnelingClientContext`.
                }
                Tunnel::AwsPrivatelink(aws_privatelink) => {
                    let host = mz_cloud_resources::vpc_endpoint_host(
                        aws_privatelink.connection_id,
                        aws_privatelink.availability_zone.as_deref(),
                    );
                    let port = aws_privatelink.port;
                    context.add_broker_rewrite(
                        addr,
                        BrokerRewrite {
                            host: host.clone(),
                            port,
                        },
                    );
                }
                Tunnel::Ssh(ssh_tunnel) => {
                    // Ensure any SSH bastion address we connect to is resolved to an external address.
                    let ssh_host_resolved = resolve_address(
                        &ssh_tunnel.connection.host,
                        ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                    )
                    .await?;
                    context
                        .add_ssh_tunnel(
                            addr,
                            SshTunnelConfig {
                                host: ssh_host_resolved
                                    .iter()
                                    .map(|a| a.to_string())
                                    .collect::<BTreeSet<_>>(),
                                port: ssh_tunnel.connection.port,
                                user: ssh_tunnel.connection.user.clone(),
                                key_pair: SshKeyPair::from_bytes(
                                    &storage_configuration
                                        .connection_context
                                        .secrets_reader
                                        .read_in_task_if(in_task, ssh_tunnel.connection_id)
                                        .await?,
                                )?,
                            },
                        )
                        .await
                        .map_err(ContextCreationError::Ssh)?;
                }
            }
        }

        Ok(config.create_with_context(context)?)
    }

    async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let (context, error_rx) = MzClientContext::with_errors();
        let consumer: BaseConsumer<_> = self
            .create_with_context(
                storage_configuration,
                context,
                &BTreeMap::new(),
                // We are in a normal tokio context during validation, already.
                InTask::No,
            )
            .await?;
        let consumer = Arc::new(consumer);

        let timeout = storage_configuration
            .parameters
            .kafka_timeout_config
            .fetch_metadata_timeout;

        // librdkafka doesn't expose an API for determining whether a connection to
        // the Kafka cluster has been successfully established. So we make a
        // metadata request, though we don't care about the results, so that we can
        // report any errors making that request. If the request succeeds, we know
        // we were able to contact at least one broker, and that's a good proxy for
        // being able to contact all the brokers in the cluster.
        //
        // The downside of this approach is it produces a generic error message like
        // "metadata fetch error" with no additional details. The real networking
        // error is buried in the librdkafka logs, which are not visible to users.
        let result = mz_ore::task::spawn_blocking(|| "kafka_get_metadata", {
            let consumer = Arc::clone(&consumer);
            move || consumer.fetch_metadata(None, timeout)
        })
        .await?;
        match result {
            Ok(_) => Ok(()),
            // The error returned by `fetch_metadata` does not provide any details which makes for
            // a crappy user facing error message. For this reason we attempt to grab a better
            // error message from the client context, which should contain any error logs emitted
            // by librdkafka, and fallback to the generic error if there is nothing there.
            Err(err) => {
                // Multiple errors might have been logged during this validation but some are more
                // relevant than others. Specifically, we prefer non-internal errors over internal
                // errors since those give much more useful information to the users.
                let main_err = error_rx.try_iter().reduce(|cur, new| match cur {
                    MzKafkaError::Internal(_) => new,
                    _ => cur,
                });

                // Don't drop the consumer until after we've drained the errors
                // channel. Dropping the consumer can introduce spurious errors.
                // See database-issues#7432.
                drop(consumer);

                match main_err {
                    Some(err) => Err(err.into()),
                    None => Err(err.into()),
                }
            }
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for KafkaConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let KafkaConnection {
            brokers: _,
            default_tunnel: _,
            progress_topic,
            progress_topic_options,
            options: _,
            tls: _,
            sasl: _,
        } = self;

        let compatibility_checks = [
            (progress_topic == &other.progress_topic, "progress_topic"),
            (
                progress_topic_options == &other.progress_topic_options,
                "progress_topic_options",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "KafkaConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl RustType<ProtoKafkaConnectionTlsConfig> for KafkaTlsConfig {
    fn into_proto(&self) -> ProtoKafkaConnectionTlsConfig {
        ProtoKafkaConnectionTlsConfig {
            identity: self.identity.into_proto(),
            root_cert: self.root_cert.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaConnectionTlsConfig) -> Result<Self, TryFromProtoError> {
        Ok(KafkaTlsConfig {
            root_cert: proto.root_cert.into_rust()?,
            identity: proto.identity.into_rust()?,
        })
    }
}

impl RustType<ProtoKafkaConnectionSaslConfig> for KafkaSaslConfig {
    fn into_proto(&self) -> ProtoKafkaConnectionSaslConfig {
        ProtoKafkaConnectionSaslConfig {
            mechanism: self.mechanism.into_proto(),
            username: Some(self.username.into_proto()),
            password: self.password.into_proto(),
            aws: self.aws.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaConnectionSaslConfig) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSaslConfig {
            mechanism: proto.mechanism,
            username: proto
                .username
                .into_rust_if_some("ProtoKafkaConnectionSaslConfig::username")?,
            password: proto.password.into_rust()?,
            aws: proto.aws.into_rust()?,
        })
    }
}

impl RustType<ProtoKafkaConnection> for KafkaConnection {
    fn into_proto(&self) -> ProtoKafkaConnection {
        ProtoKafkaConnection {
            brokers: self.brokers.into_proto(),
            default_tunnel: Some(self.default_tunnel.into_proto()),
            progress_topic: self.progress_topic.into_proto(),
            progress_topic_options: Some(self.progress_topic_options.into_proto()),
            options: self
                .options
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
            tls: self.tls.into_proto(),
            sasl: self.sasl.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaConnection) -> Result<Self, TryFromProtoError> {
        Ok(KafkaConnection {
            brokers: proto.brokers.into_rust()?,
            default_tunnel: proto
                .default_tunnel
                .into_rust_if_some("ProtoKafkaConnection::default_tunnel")?,
            progress_topic: proto.progress_topic,
            progress_topic_options: match proto.progress_topic_options {
                Some(progress_topic_options) => progress_topic_options.into_rust()?,
                None => Default::default(),
            },
            options: proto
                .options
                .into_iter()
                .map(|(k, v)| StringOrSecret::from_proto(v).map(|v| (k, v)))
                .collect::<Result<_, _>>()?,
            tls: proto.tls.into_rust()?,
            sasl: proto.sasl.into_rust()?,
        })
    }
}

/// A connection to a Confluent Schema Registry.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct CsrConnection<C: ConnectionAccess = InlinedConnection> {
    /// The URL of the schema registry.
    #[proptest(strategy = "any_url()")]
    pub url: Url,
    /// Trusted root TLS certificate in PEM format.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication with the schema
    /// registry.
    pub tls_identity: Option<TlsIdentity>,
    /// Optional HTTP authentication credentials for the schema registry.
    pub http_auth: Option<CsrConnectionHttpAuth>,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
}

impl<R: ConnectionResolver> IntoInlineConnection<CsrConnection, R>
    for CsrConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> CsrConnection {
        let CsrConnection {
            url,
            tls_root_cert,
            tls_identity,
            http_auth,
            tunnel,
        } = self;
        CsrConnection {
            url,
            tls_root_cert,
            tls_identity,
            http_auth,
            tunnel: tunnel.into_inline_connection(r),
        }
    }
}

impl<C: ConnectionAccess> CsrConnection<C> {
    fn validate_by_default(&self) -> bool {
        true
    }
}

impl CsrConnection {
    /// Constructs a schema registry client from the connection.
    pub async fn connect(
        &self,
        storage_configuration: &StorageConfiguration,
        in_task: InTask,
    ) -> Result<mz_ccsr::Client, CsrConnectError> {
        let mut client_config = mz_ccsr::ClientConfig::new(self.url.clone());
        if let Some(root_cert) = &self.tls_root_cert {
            let root_cert = root_cert
                .get_string(
                    in_task,
                    &storage_configuration.connection_context.secrets_reader,
                )
                .await?;
            let root_cert = Certificate::from_pem(root_cert.as_bytes())?;
            client_config = client_config.add_root_certificate(root_cert);
        }

        if let Some(tls_identity) = &self.tls_identity {
            let key = &storage_configuration
                .connection_context
                .secrets_reader
                .read_string_in_task_if(in_task, tls_identity.key)
                .await?;
            let cert = tls_identity
                .cert
                .get_string(
                    in_task,
                    &storage_configuration.connection_context.secrets_reader,
                )
                .await?;
            let ident = Identity::from_pem(key.as_bytes(), cert.as_bytes())?;
            client_config = client_config.identity(ident);
        }

        if let Some(http_auth) = &self.http_auth {
            let username = http_auth
                .username
                .get_string(
                    in_task,
                    &storage_configuration.connection_context.secrets_reader,
                )
                .await?;
            let password = match http_auth.password {
                None => None,
                Some(password) => Some(
                    storage_configuration
                        .connection_context
                        .secrets_reader
                        .read_string_in_task_if(in_task, password)
                        .await?,
                ),
            };
            client_config = client_config.auth(username, password);
        }

        // `net::lookup_host` requires a port but the port will be ignored when
        // passed to `resolve_to_addrs`. We use a dummy port that will be easy
        // to spot in the logs to make it obvious if some component downstream
        // incorrectly starts using this port.
        const DUMMY_PORT: u16 = 11111;

        // TODO: use types to enforce that the URL has a string hostname.
        let host = self
            .url
            .host_str()
            .ok_or_else(|| anyhow!("url missing host"))?;
        match &self.tunnel {
            Tunnel::Direct => {
                // Ensure any host we connect to is resolved to an external address.
                let resolved = resolve_address(
                    host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                client_config = client_config.resolve_to_addrs(
                    host,
                    &resolved
                        .iter()
                        .map(|addr| SocketAddr::new(*addr, DUMMY_PORT))
                        .collect::<Vec<_>>(),
                )
            }
            Tunnel::Ssh(ssh_tunnel) => {
                let ssh_tunnel = ssh_tunnel
                    .connect(
                        storage_configuration,
                        host,
                        // Default to the default http port, but this
                        // could default to 8081...
                        self.url.port().unwrap_or(80),
                        in_task,
                    )
                    .await
                    .map_err(CsrConnectError::Ssh)?;

                // Carefully inject the SSH tunnel into the client
                // configuration. This is delicate because we need TLS
                // verification to continue to use the remote hostname rather
                // than the tunnel hostname.

                client_config = client_config
                    // `resolve_to_addrs` allows us to rewrite the hostname
                    // at the DNS level, which means the TCP connection is
                    // correctly routed through the tunnel, but TLS verification
                    // is still performed against the remote hostname.
                    // Unfortunately the port here is ignored...
                    .resolve_to_addrs(
                        host,
                        &[SocketAddr::new(ssh_tunnel.local_addr().ip(), DUMMY_PORT)],
                    )
                    // ...so we also dynamically rewrite the URL to use the
                    // current port for the SSH tunnel.
                    //
                    // WARNING: this is brittle, because we only dynamically
                    // update the client configuration with the tunnel *port*,
                    // and not the hostname This works fine in practice, because
                    // only the SSH tunnel port will change if the tunnel fails
                    // and has to be restarted (the hostname is always
                    // 127.0.0.1)--but this is an an implementation detail of
                    // the SSH tunnel code that we're relying on.
                    .dynamic_url({
                        let remote_url = self.url.clone();
                        move || {
                            let mut url = remote_url.clone();
                            url.set_port(Some(ssh_tunnel.local_addr().port()))
                                .expect("cannot fail");
                            url
                        }
                    });
            }
            Tunnel::AwsPrivatelink(connection) => {
                assert_none!(connection.port);

                let privatelink_host = mz_cloud_resources::vpc_endpoint_host(
                    connection.connection_id,
                    connection.availability_zone.as_deref(),
                );
                let addrs: Vec<_> = net::lookup_host((privatelink_host, DUMMY_PORT))
                    .await
                    .context("resolving PrivateLink host")?
                    .collect();
                client_config = client_config.resolve_to_addrs(host, &addrs)
            }
        }

        Ok(client_config.build()?)
    }

    async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let client = self
            .connect(
                storage_configuration,
                // We are in a normal tokio context during validation, already.
                InTask::No,
            )
            .await?;
        client.list_subjects().await?;
        Ok(())
    }
}

impl RustType<ProtoCsrConnection> for CsrConnection {
    fn into_proto(&self) -> ProtoCsrConnection {
        ProtoCsrConnection {
            url: Some(self.url.into_proto()),
            tls_root_cert: self.tls_root_cert.into_proto(),
            tls_identity: self.tls_identity.into_proto(),
            http_auth: self.http_auth.into_proto(),
            tunnel: Some(self.tunnel.into_proto()),
        }
    }

    fn from_proto(proto: ProtoCsrConnection) -> Result<Self, TryFromProtoError> {
        Ok(CsrConnection {
            url: proto.url.into_rust_if_some("ProtoCsrConnection::url")?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
            tls_identity: proto.tls_identity.into_rust()?,
            http_auth: proto.http_auth.into_rust()?,
            tunnel: proto
                .tunnel
                .into_rust_if_some("ProtoCsrConnection::tunnel")?,
        })
    }
}

impl<C: ConnectionAccess> AlterCompatible for CsrConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let CsrConnection {
            tunnel,
            // All non-tunnel fields may change
            url: _,
            tls_root_cert: _,
            tls_identity: _,
            http_auth: _,
        } = self;

        let compatibility_checks = [(tunnel.alter_compatible(id, &other.tunnel).is_ok(), "tunnel")];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "CsrConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

/// A TLS key pair used for client identity.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TlsIdentity {
    /// The client's TLS public certificate in PEM format.
    pub cert: StringOrSecret,
    /// The ID of the secret containing the client's TLS private key in PEM
    /// format.
    pub key: CatalogItemId,
}

impl RustType<ProtoTlsIdentity> for TlsIdentity {
    fn into_proto(&self) -> ProtoTlsIdentity {
        ProtoTlsIdentity {
            cert: Some(self.cert.into_proto()),
            key: Some(self.key.into_proto()),
        }
    }

    fn from_proto(proto: ProtoTlsIdentity) -> Result<Self, TryFromProtoError> {
        Ok(TlsIdentity {
            cert: proto.cert.into_rust_if_some("ProtoTlsIdentity::cert")?,
            key: proto.key.into_rust_if_some("ProtoTlsIdentity::key")?,
        })
    }
}

/// HTTP authentication credentials in a [`CsrConnection`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CsrConnectionHttpAuth {
    /// The username.
    pub username: StringOrSecret,
    /// The ID of the secret containing the password, if any.
    pub password: Option<CatalogItemId>,
}

impl RustType<ProtoCsrConnectionHttpAuth> for CsrConnectionHttpAuth {
    fn into_proto(&self) -> ProtoCsrConnectionHttpAuth {
        ProtoCsrConnectionHttpAuth {
            username: Some(self.username.into_proto()),
            password: self.password.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCsrConnectionHttpAuth) -> Result<Self, TryFromProtoError> {
        Ok(CsrConnectionHttpAuth {
            username: proto
                .username
                .into_rust_if_some("ProtoCsrConnectionHttpAuth::username")?,
            password: proto.password.into_rust()?,
        })
    }
}

/// A connection to a PostgreSQL server.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct PostgresConnection<C: ConnectionAccess = InlinedConnection> {
    /// The hostname of the server.
    pub host: String,
    /// The port of the server.
    pub port: u16,
    /// The name of the database to connect to.
    pub database: String,
    /// The username to authenticate as.
    pub user: StringOrSecret,
    /// An optional password for authentication.
    pub password: Option<CatalogItemId>,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
    /// Whether to use TLS for encryption, authentication, or both.
    #[proptest(strategy = "any_ssl_mode()")]
    pub tls_mode: SslMode,
    /// An optional root TLS certificate in PEM format, to verify the server's
    /// identity.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication.
    pub tls_identity: Option<TlsIdentity>,
    /// The kind of postgres server we are connecting to. This can be vanilla, for a normal
    /// postgres server or some other system that is pg compatible, like Yugabyte, Aurora, etc.
    pub flavor: PostgresFlavor,
}

impl<R: ConnectionResolver> IntoInlineConnection<PostgresConnection, R>
    for PostgresConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> PostgresConnection {
        let PostgresConnection {
            host,
            port,
            database,
            user,
            password,
            tunnel,
            tls_mode,
            tls_root_cert,
            tls_identity,
            flavor,
        } = self;

        PostgresConnection {
            host,
            port,
            database,
            user,
            password,
            tunnel: tunnel.into_inline_connection(r),
            tls_mode,
            tls_root_cert,
            tls_identity,
            flavor,
        }
    }
}

impl<C: ConnectionAccess> PostgresConnection<C> {
    fn validate_by_default(&self) -> bool {
        true
    }
}

impl PostgresConnection<InlinedConnection> {
    pub async fn config(
        &self,
        secrets_reader: &Arc<dyn mz_secrets::SecretsReader>,
        storage_configuration: &StorageConfiguration,
        in_task: InTask,
    ) -> Result<mz_postgres_util::Config, anyhow::Error> {
        let params = &storage_configuration.parameters;

        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user.get_string(in_task, secrets_reader).await?)
            .ssl_mode(self.tls_mode);
        if let Some(password) = self.password {
            let password = secrets_reader
                .read_string_in_task_if(in_task, password)
                .await?;
            config.password(password);
        }
        if let Some(tls_root_cert) = &self.tls_root_cert {
            let tls_root_cert = tls_root_cert.get_string(in_task, secrets_reader).await?;
            config.ssl_root_cert(tls_root_cert.as_bytes());
        }
        if let Some(tls_identity) = &self.tls_identity {
            let cert = tls_identity
                .cert
                .get_string(in_task, secrets_reader)
                .await?;
            let key = secrets_reader
                .read_string_in_task_if(in_task, tls_identity.key)
                .await?;
            config.ssl_cert(cert.as_bytes()).ssl_key(key.as_bytes());
        }

        if let Some(connect_timeout) = params.pg_source_connect_timeout {
            config.connect_timeout(connect_timeout);
        }
        if let Some(keepalives_retries) = params.pg_source_tcp_keepalives_retries {
            config.keepalives_retries(keepalives_retries);
        }
        if let Some(keepalives_idle) = params.pg_source_tcp_keepalives_idle {
            config.keepalives_idle(keepalives_idle);
        }
        if let Some(keepalives_interval) = params.pg_source_tcp_keepalives_interval {
            config.keepalives_interval(keepalives_interval);
        }
        if let Some(tcp_user_timeout) = params.pg_source_tcp_user_timeout {
            config.tcp_user_timeout(tcp_user_timeout);
        }

        let mut options = vec![];
        if let Some(wal_sender_timeout) = params.pg_source_wal_sender_timeout {
            options.push(format!(
                "--wal_sender_timeout={}",
                wal_sender_timeout.as_millis()
            ));
        };
        if params.pg_source_tcp_configure_server {
            if let Some(keepalives_retries) = params.pg_source_tcp_keepalives_retries {
                options.push(format!("--tcp_keepalives_count={}", keepalives_retries));
            }
            if let Some(keepalives_idle) = params.pg_source_tcp_keepalives_idle {
                options.push(format!(
                    "--tcp_keepalives_idle={}",
                    keepalives_idle.as_secs()
                ));
            }
            if let Some(keepalives_interval) = params.pg_source_tcp_keepalives_interval {
                options.push(format!(
                    "--tcp_keepalives_interval={}",
                    keepalives_interval.as_secs()
                ));
            }
            if let Some(tcp_user_timeout) = params.pg_source_tcp_user_timeout {
                options.push(format!(
                    "--tcp_user_timeout={}",
                    tcp_user_timeout.as_millis()
                ));
            }
        }
        config.options(options.join(" ").as_str());

        let tunnel = match &self.tunnel {
            Tunnel::Direct => {
                // Ensure any host we connect to is resolved to an external address.
                let resolved = resolve_address(
                    &self.host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                mz_postgres_util::TunnelConfig::Direct {
                    resolved_ips: Some(resolved),
                }
            }
            Tunnel::Ssh(SshTunnel {
                connection_id,
                connection,
            }) => {
                let secret = secrets_reader
                    .read_in_task_if(in_task, *connection_id)
                    .await?;
                let key_pair = SshKeyPair::from_bytes(&secret)?;
                // Ensure any ssh-bastion host we connect to is resolved to an external address.
                let resolved = resolve_address(
                    &connection.host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                mz_postgres_util::TunnelConfig::Ssh {
                    config: SshTunnelConfig {
                        host: resolved
                            .iter()
                            .map(|a| a.to_string())
                            .collect::<BTreeSet<_>>(),
                        port: connection.port,
                        user: connection.user.clone(),
                        key_pair,
                    },
                }
            }
            Tunnel::AwsPrivatelink(connection) => {
                assert_none!(connection.port);
                mz_postgres_util::TunnelConfig::AwsPrivatelink {
                    connection_id: connection.connection_id,
                }
            }
        };

        Ok(mz_postgres_util::Config::new(
            config,
            tunnel,
            params.ssh_timeout_config,
            in_task,
        )?)
    }

    async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let config = self
            .config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                // We are in a normal tokio context during validation, already.
                InTask::No,
            )
            .await?;
        let client = config
            .connect(
                "connection validation",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;
        use PostgresFlavor::*;
        match (client.server_flavor(), &self.flavor) {
            (Vanilla, Yugabyte) => bail!("Expected to find PostgreSQL server, found Yugabyte."),
            (Yugabyte, Vanilla) => bail!("Expected to find Yugabyte server, found PostgreSQL."),
            (Vanilla, Vanilla) | (Yugabyte, Yugabyte) => {}
        }
        Ok(())
    }
}

impl<C: ConnectionAccess> AlterCompatible for PostgresConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let PostgresConnection {
            tunnel,
            flavor,
            // All non-tunnel options may change arbitrarily
            host: _,
            port: _,
            database: _,
            user: _,
            password: _,
            tls_mode: _,
            tls_root_cert: _,
            tls_identity: _,
        } = self;

        let compatibility_checks = [
            (tunnel.alter_compatible(id, &other.tunnel).is_ok(), "tunnel"),
            (flavor == &other.flavor, "flavor"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "PostgresConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

impl RustType<ProtoPostgresConnection> for PostgresConnection {
    fn into_proto(&self) -> ProtoPostgresConnection {
        ProtoPostgresConnection {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            database: self.database.into_proto(),
            user: Some(self.user.into_proto()),
            password: self.password.into_proto(),
            tls_mode: Some(self.tls_mode.into_proto()),
            tls_root_cert: self.tls_root_cert.into_proto(),
            tls_identity: self.tls_identity.into_proto(),
            tunnel: Some(self.tunnel.into_proto()),
            flavor: Some(self.flavor.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPostgresConnection) -> Result<Self, TryFromProtoError> {
        Ok(PostgresConnection {
            host: proto.host,
            port: proto.port.into_rust()?,
            database: proto.database,
            user: proto
                .user
                .into_rust_if_some("ProtoPostgresConnection::user")?,
            password: proto.password.into_rust()?,
            tunnel: proto
                .tunnel
                .into_rust_if_some("ProtoPostgresConnection::tunnel")?,
            tls_mode: proto
                .tls_mode
                .into_rust_if_some("ProtoPostgresConnection::tls_mode")?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
            tls_identity: proto.tls_identity.into_rust()?,
            flavor: proto
                .flavor
                .into_rust_if_some("ProtoPostgresConnection::flavor")?,
        })
    }
}

/// Specifies how to tunnel a connection.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Tunnel<C: ConnectionAccess = InlinedConnection> {
    /// No tunneling.
    Direct,
    /// Via the specified SSH tunnel connection.
    Ssh(SshTunnel<C>),
    /// Via the specified AWS PrivateLink connection.
    AwsPrivatelink(AwsPrivatelink),
}

impl<R: ConnectionResolver> IntoInlineConnection<Tunnel, R> for Tunnel<ReferencedConnection> {
    fn into_inline_connection(self, r: R) -> Tunnel {
        match self {
            Tunnel::Direct => Tunnel::Direct,
            Tunnel::Ssh(ssh) => Tunnel::Ssh(ssh.into_inline_connection(r)),
            Tunnel::AwsPrivatelink(awspl) => Tunnel::AwsPrivatelink(awspl),
        }
    }
}

impl RustType<ProtoTunnel> for Tunnel<InlinedConnection> {
    fn into_proto(&self) -> ProtoTunnel {
        use proto_tunnel::Tunnel as ProtoTunnelField;
        ProtoTunnel {
            tunnel: Some(match &self {
                Tunnel::Direct => ProtoTunnelField::Direct(()),
                Tunnel::Ssh(ssh) => ProtoTunnelField::Ssh(ssh.into_proto()),
                Tunnel::AwsPrivatelink(aws) => ProtoTunnelField::AwsPrivatelink(aws.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoTunnel) -> Result<Self, TryFromProtoError> {
        use proto_tunnel::Tunnel as ProtoTunnelField;
        Ok(match proto.tunnel {
            None => return Err(TryFromProtoError::missing_field("ProtoTunnel::tunnel")),
            Some(ProtoTunnelField::Direct(())) => Tunnel::Direct,
            Some(ProtoTunnelField::Ssh(ssh)) => Tunnel::Ssh(ssh.into_rust()?),
            Some(ProtoTunnelField::AwsPrivatelink(aws)) => Tunnel::AwsPrivatelink(aws.into_rust()?),
        })
    }
}

impl<C: ConnectionAccess> AlterCompatible for Tunnel<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let compatible = match (self, other) {
            (Self::Ssh(s), Self::Ssh(o)) => s.alter_compatible(id, o).is_ok(),
            (s, o) => s == o,
        };

        if !compatible {
            tracing::warn!(
                "Tunnel incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );

            return Err(AlterError { id });
        }

        Ok(())
    }
}

/// Specifies which MySQL SSL Mode to use:
/// <https://dev.mysql.com/doc/refman/8.0/en/connection-options.html#option_general_ssl-mode>
/// This is not available as an enum in the mysql-async crate, so we define our own.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum MySqlSslMode {
    Disabled,
    Required,
    VerifyCa,
    VerifyIdentity,
}

impl RustType<i32> for MySqlSslMode {
    fn into_proto(&self) -> i32 {
        match self {
            MySqlSslMode::Disabled => ProtoMySqlSslMode::Disabled.into(),
            MySqlSslMode::Required => ProtoMySqlSslMode::Required.into(),
            MySqlSslMode::VerifyCa => ProtoMySqlSslMode::VerifyCa.into(),
            MySqlSslMode::VerifyIdentity => ProtoMySqlSslMode::VerifyIdentity.into(),
        }
    }

    fn from_proto(proto: i32) -> Result<Self, TryFromProtoError> {
        Ok(match ProtoMySqlSslMode::try_from(proto) {
            Ok(ProtoMySqlSslMode::Disabled) => MySqlSslMode::Disabled,
            Ok(ProtoMySqlSslMode::Required) => MySqlSslMode::Required,
            Ok(ProtoMySqlSslMode::VerifyCa) => MySqlSslMode::VerifyCa,
            Ok(ProtoMySqlSslMode::VerifyIdentity) => MySqlSslMode::VerifyIdentity,
            Err(_) => {
                return Err(TryFromProtoError::UnknownEnumVariant(
                    "tls_mode".to_string(),
                ));
            }
        })
    }
}

pub fn any_mysql_ssl_mode() -> impl Strategy<Value = MySqlSslMode> {
    proptest::sample::select(vec![
        MySqlSslMode::Disabled,
        MySqlSslMode::Required,
        MySqlSslMode::VerifyCa,
        MySqlSslMode::VerifyIdentity,
    ])
}

/// A connection to a MySQL server.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct MySqlConnection<C: ConnectionAccess = InlinedConnection> {
    /// The hostname of the server.
    pub host: String,
    /// The port of the server.
    pub port: u16,
    /// The username to authenticate as.
    pub user: StringOrSecret,
    /// An optional password for authentication.
    pub password: Option<CatalogItemId>,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
    /// Whether to use TLS for encryption, verify the server's certificate, and identity.
    #[proptest(strategy = "any_mysql_ssl_mode()")]
    pub tls_mode: MySqlSslMode,
    /// An optional root TLS certificate in PEM format, to verify the server's
    /// identity.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication.
    pub tls_identity: Option<TlsIdentity>,
    /// Reference to the AWS connection information to be used for IAM authenitcation and
    /// assuming AWS roles.
    pub aws_connection: Option<AwsConnectionReference<C>>,
}

impl<R: ConnectionResolver> IntoInlineConnection<MySqlConnection, R>
    for MySqlConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> MySqlConnection {
        let MySqlConnection {
            host,
            port,
            user,
            password,
            tunnel,
            tls_mode,
            tls_root_cert,
            tls_identity,
            aws_connection,
        } = self;

        MySqlConnection {
            host,
            port,
            user,
            password,
            tunnel: tunnel.into_inline_connection(&r),
            tls_mode,
            tls_root_cert,
            tls_identity,
            aws_connection: aws_connection.map(|aws| aws.into_inline_connection(&r)),
        }
    }
}

impl<C: ConnectionAccess> MySqlConnection<C> {
    fn validate_by_default(&self) -> bool {
        true
    }
}

impl MySqlConnection<InlinedConnection> {
    pub async fn config(
        &self,
        secrets_reader: &Arc<dyn mz_secrets::SecretsReader>,
        storage_configuration: &StorageConfiguration,
        in_task: InTask,
    ) -> Result<mz_mysql_util::Config, anyhow::Error> {
        // TODO(roshan): Set appropriate connection timeouts
        let mut opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user.get_string(in_task, secrets_reader).await?));

        if let Some(password) = self.password {
            let password = secrets_reader
                .read_string_in_task_if(in_task, password)
                .await?;
            opts = opts.pass(Some(password));
        }

        // Our `MySqlSslMode` enum matches the official MySQL Client `--ssl-mode` parameter values
        // which uses opt-in security features (SSL, CA verification, & Identity verification).
        // The mysql_async crate `SslOpts` struct uses an opt-out mechanism for each of these, so
        // we need to appropriately disable features to match the intent of each enum value.
        let mut ssl_opts = match self.tls_mode {
            MySqlSslMode::Disabled => None,
            MySqlSslMode::Required => Some(
                mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
            MySqlSslMode::VerifyCa => {
                Some(mysql_async::SslOpts::default().with_danger_skip_domain_validation(true))
            }
            MySqlSslMode::VerifyIdentity => Some(mysql_async::SslOpts::default()),
        };

        if matches!(
            self.tls_mode,
            MySqlSslMode::VerifyCa | MySqlSslMode::VerifyIdentity
        ) {
            if let Some(tls_root_cert) = &self.tls_root_cert {
                let tls_root_cert = tls_root_cert.get_string(in_task, secrets_reader).await?;
                ssl_opts = ssl_opts.map(|opts| {
                    opts.with_root_certs(vec![tls_root_cert.as_bytes().to_vec().into()])
                });
            }
        }

        if let Some(identity) = &self.tls_identity {
            let key = secrets_reader
                .read_string_in_task_if(in_task, identity.key)
                .await?;
            let cert = identity.cert.get_string(in_task, secrets_reader).await?;
            let Pkcs12Archive { der, pass } =
                mz_tls_util::pkcs12der_from_pem(key.as_bytes(), cert.as_bytes())?;

            // Add client identity to SSLOpts
            ssl_opts = ssl_opts.map(|opts| {
                opts.with_client_identity(Some(
                    mysql_async::ClientIdentity::new(der.into()).with_password(pass),
                ))
            });
        }

        opts = opts.ssl_opts(ssl_opts);

        let tunnel = match &self.tunnel {
            Tunnel::Direct => {
                // Ensure any host we connect to is resolved to an external address.
                let resolved = resolve_address(
                    &self.host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                mz_mysql_util::TunnelConfig::Direct {
                    resolved_ips: Some(resolved),
                }
            }
            Tunnel::Ssh(SshTunnel {
                connection_id,
                connection,
            }) => {
                let secret = secrets_reader
                    .read_in_task_if(in_task, *connection_id)
                    .await?;
                let key_pair = SshKeyPair::from_bytes(&secret)?;
                // Ensure any ssh-bastion host we connect to is resolved to an external address.
                let resolved = resolve_address(
                    &connection.host,
                    ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
                )
                .await?;
                mz_mysql_util::TunnelConfig::Ssh {
                    config: SshTunnelConfig {
                        host: resolved
                            .iter()
                            .map(|a| a.to_string())
                            .collect::<BTreeSet<_>>(),
                        port: connection.port,
                        user: connection.user.clone(),
                        key_pair,
                    },
                }
            }
            Tunnel::AwsPrivatelink(connection) => {
                assert_none!(connection.port);
                mz_mysql_util::TunnelConfig::AwsPrivatelink {
                    connection_id: connection.connection_id,
                }
            }
        };

        let aws_config = match self.aws_connection.as_ref() {
            None => None,
            Some(aws_ref) => Some(
                aws_ref
                    .connection
                    .load_sdk_config(
                        &storage_configuration.connection_context,
                        aws_ref.connection_id,
                        in_task,
                    )
                    .await?,
            ),
        };

        Ok(mz_mysql_util::Config::new(
            opts,
            tunnel,
            storage_configuration.parameters.ssh_timeout_config,
            in_task,
            storage_configuration
                .parameters
                .mysql_source_timeouts
                .clone(),
            aws_config,
        )?)
    }

    async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let config = self
            .config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                // We are in a normal tokio context during validation, already.
                InTask::No,
            )
            .await?;
        let conn = config
            .connect(
                "connection validation",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;
        conn.disconnect().await?;
        Ok(())
    }
}

impl RustType<ProtoMySqlConnection> for MySqlConnection {
    fn into_proto(&self) -> ProtoMySqlConnection {
        ProtoMySqlConnection {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            user: Some(self.user.into_proto()),
            password: self.password.into_proto(),
            tls_mode: self.tls_mode.into_proto(),
            tls_root_cert: self.tls_root_cert.into_proto(),
            tls_identity: self.tls_identity.into_proto(),
            tunnel: Some(self.tunnel.into_proto()),
            aws_connection: self.aws_connection.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMySqlConnection) -> Result<Self, TryFromProtoError> {
        Ok(MySqlConnection {
            host: proto.host,
            port: proto.port.into_rust()?,
            user: proto.user.into_rust_if_some("ProtoMySqlConnection::user")?,
            password: proto.password.into_rust()?,
            tunnel: proto
                .tunnel
                .into_rust_if_some("ProtoMySqlConnection::tunnel")?,
            tls_mode: proto.tls_mode.into_rust()?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
            tls_identity: proto.tls_identity.into_rust()?,
            aws_connection: proto.aws_connection.into_rust()?,
        })
    }
}

impl<C: ConnectionAccess> AlterCompatible for MySqlConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let MySqlConnection {
            tunnel,
            // All non-tunnel options may change arbitrarily
            host: _,
            port: _,
            user: _,
            password: _,
            tls_mode: _,
            tls_root_cert: _,
            tls_identity: _,
            aws_connection: _,
        } = self;

        let compatibility_checks = [(tunnel.alter_compatible(id, &other.tunnel).is_ok(), "tunnel")];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "MySqlConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

/// Details how to connect to an instance of Microsoft SQL Server.
///
/// For specifics of connecting to SQL Server for purposes of creating a
/// Materialize Source, see [`SqlServerSource`] which wraps this type.
///
/// [`SqlServerSource`]: crate::sources::SqlServerSource
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SqlServerConnectionDetails<C: ConnectionAccess = InlinedConnection> {
    /// The hostname of the server.
    pub host: String,
    /// The port of the server.
    pub port: u16,
    /// Database we should connect to.
    pub database: String,
    /// The username to authenticate as.
    pub user: StringOrSecret,
    /// Password used for authentication.
    pub password: CatalogItemId,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
    /// Level of encryption to use for the connection.
    pub encryption: mz_sql_server_util::config::EncryptionLevel,
}

impl<C: ConnectionAccess> SqlServerConnectionDetails<C> {
    fn validate_by_default(&self) -> bool {
        true
    }
}

impl SqlServerConnectionDetails<InlinedConnection> {
    /// Attempts to open a connection to the upstream SQL Server instance.
    async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let config = self
            .resolve_config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                InTask::No,
            )
            .await?;
        tracing::debug!(?config, "Validating SQL Server connection");

        // Just connecting is enough to validate, no need to send any queries.
        let _client = mz_sql_server_util::Client::connect(config).await?;

        Ok(())
    }

    /// Resolve all of the connection details (e.g. read from the [`SecretsReader`])
    /// so the returned [`Config`] can be used to open a connection with the
    /// upstream system.
    ///
    /// The provided [`InTask`] argument determines whether any I/O is run in an
    /// [`mz_ore::task`] (i.e. a different thread) or directly in the returned
    /// future. The main goal here is to prevent running I/O in timely threads.
    ///
    /// [`Config`]: mz_sql_server_util::Config
    pub async fn resolve_config(
        &self,
        secrets_reader: &Arc<dyn mz_secrets::SecretsReader>,
        storage_configuration: &StorageConfiguration,
        in_task: InTask,
    ) -> Result<mz_sql_server_util::Config, anyhow::Error> {
        let dyncfg = storage_configuration.config_set();
        let mut inner_config = tiberius::Config::new();

        // Setup default connection params.
        inner_config.host(&self.host);
        inner_config.port(self.port);
        inner_config.database(self.database.clone());
        // TODO(sql_server1): Figure out the right settings for encryption.
        inner_config.encryption(self.encryption.into());
        inner_config.application_name("materialize");

        // Read our auth settings from
        let user = self
            .user
            .get_string(in_task, secrets_reader)
            .await
            .context("username")?;
        let password = secrets_reader
            .read_string_in_task_if(in_task, self.password)
            .await
            .context("password")?;
        // TODO(sql_server3): Support other methods of authentication besides
        // username and password.
        inner_config.authentication(tiberius::AuthMethod::sql_server(user, password));

        // TODO(sql_server2): Fork the tiberius library and add support for
        // specifying a cert bundle from a binary blob.
        //
        // See: <https://github.com/prisma/tiberius/pull/290>
        inner_config.trust_cert();

        // Prevent users from probing our internal network ports by trying to
        // connect to localhost, or another non-external IP.
        let enfoce_external_addresses = ENFORCE_EXTERNAL_ADDRESSES.get(dyncfg);

        let tunnel = match &self.tunnel {
            Tunnel::Direct => mz_sql_server_util::config::TunnelConfig::Direct,
            Tunnel::Ssh(SshTunnel {
                connection_id,
                connection: ssh_connection,
            }) => {
                let secret = secrets_reader
                    .read_in_task_if(in_task, *connection_id)
                    .await
                    .context("ssh secret")?;
                let key_pair = SshKeyPair::from_bytes(&secret).context("ssh key pair")?;
                // Ensure any SSH-bastion host we connect to is resolved to an
                // external address.
                let addresses = resolve_address(&ssh_connection.host, enfoce_external_addresses)
                    .await
                    .context("ssh tunnel")?;

                let config = SshTunnelConfig {
                    host: addresses.into_iter().map(|a| a.to_string()).collect(),
                    port: ssh_connection.port,
                    user: ssh_connection.user.clone(),
                    key_pair,
                };
                mz_sql_server_util::config::TunnelConfig::Ssh {
                    config,
                    manager: storage_configuration
                        .connection_context
                        .ssh_tunnel_manager
                        .clone(),
                    timeout: storage_configuration.parameters.ssh_timeout_config.clone(),
                    host: self.host.clone(),
                    port: self.port,
                }
            }
            Tunnel::AwsPrivatelink(private_link_connection) => {
                assert_none!(private_link_connection.port);
                mz_sql_server_util::config::TunnelConfig::AwsPrivatelink {
                    connection_id: private_link_connection.connection_id,
                }
            }
        };

        Ok(mz_sql_server_util::Config::new(
            inner_config,
            tunnel,
            in_task,
        ))
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<SqlServerConnectionDetails, R>
    for SqlServerConnectionDetails<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SqlServerConnectionDetails {
        let SqlServerConnectionDetails {
            host,
            port,
            database,
            user,
            password,
            tunnel,
            encryption,
        } = self;

        SqlServerConnectionDetails {
            host,
            port,
            database,
            user,
            password,
            tunnel: tunnel.into_inline_connection(&r),
            encryption,
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for SqlServerConnectionDetails<C> {
    fn alter_compatible(
        &self,
        id: mz_repr::GlobalId,
        other: &Self,
    ) -> Result<(), crate::controller::AlterError> {
        let SqlServerConnectionDetails {
            tunnel,
            // TODO(sql_server2): Figure out how these variables are allowed to change.
            host: _,
            port: _,
            database: _,
            user: _,
            password: _,
            encryption: _,
        } = self;

        let compatibility_checks = [(tunnel.alter_compatible(id, &other.tunnel).is_ok(), "tunnel")];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SqlServerConnectionDetails incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }
        Ok(())
    }
}

impl RustType<ProtoSqlServerConnectionDetails> for SqlServerConnectionDetails {
    fn into_proto(&self) -> ProtoSqlServerConnectionDetails {
        ProtoSqlServerConnectionDetails {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            database: self.database.into_proto(),
            user: Some(self.user.into_proto()),
            password: Some(self.password.into_proto()),
            tunnel: Some(self.tunnel.into_proto()),
            encryption: self.encryption.into_proto().into(),
        }
    }

    fn from_proto(proto: ProtoSqlServerConnectionDetails) -> Result<Self, TryFromProtoError> {
        Ok(SqlServerConnectionDetails {
            host: proto.host,
            port: proto.port.into_rust()?,
            database: proto.database.into_rust()?,
            user: proto
                .user
                .into_rust_if_some("ProtoSqlServerConnectionDetails::user")?,
            password: proto
                .password
                .into_rust_if_some("ProtoSqlServerConnectionDetails::password")?,
            tunnel: proto
                .tunnel
                .into_rust_if_some("ProtoSqlServerConnectionDetails::tunnel")?,
            encryption: ProtoSqlServerEncryptionLevel::try_from(proto.encryption)?.into_rust()?,
        })
    }
}

impl RustType<ProtoSqlServerEncryptionLevel> for mz_sql_server_util::config::EncryptionLevel {
    fn into_proto(&self) -> ProtoSqlServerEncryptionLevel {
        match self {
            Self::None => ProtoSqlServerEncryptionLevel::SqlServerNone,
            Self::Login => ProtoSqlServerEncryptionLevel::SqlServerLogin,
            Self::Preferred => ProtoSqlServerEncryptionLevel::SqlServerPreferred,
            Self::Required => ProtoSqlServerEncryptionLevel::SqlServerRequired,
        }
    }

    fn from_proto(proto: ProtoSqlServerEncryptionLevel) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            ProtoSqlServerEncryptionLevel::SqlServerNone => {
                mz_sql_server_util::config::EncryptionLevel::None
            }
            ProtoSqlServerEncryptionLevel::SqlServerLogin => {
                mz_sql_server_util::config::EncryptionLevel::Login
            }
            ProtoSqlServerEncryptionLevel::SqlServerPreferred => {
                mz_sql_server_util::config::EncryptionLevel::Preferred
            }
            ProtoSqlServerEncryptionLevel::SqlServerRequired => {
                mz_sql_server_util::config::EncryptionLevel::Required
            }
        })
    }
}

/// A connection to an SSH tunnel.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SshConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
}

use self::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};

impl RustType<ProtoSshConnection> for SshConnection {
    fn into_proto(&self) -> ProtoSshConnection {
        ProtoSshConnection {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            user: self.user.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSshConnection) -> Result<Self, TryFromProtoError> {
        Ok(SshConnection {
            host: proto.host,
            port: proto.port.into_rust()?,
            user: proto.user,
        })
    }
}

impl AlterCompatible for SshConnection {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        // Every element of the SSH connection is configurable.
        Ok(())
    }
}

/// Specifies an AWS PrivateLink service for a [`Tunnel`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AwsPrivatelink {
    /// The ID of the connection to the AWS PrivateLink service.
    pub connection_id: CatalogItemId,
    // The availability zone to use when connecting to the AWS PrivateLink service.
    pub availability_zone: Option<String>,
    /// The port to use when connecting to the AWS PrivateLink service, if
    /// different from the port in [`KafkaBroker::address`].
    pub port: Option<u16>,
}

impl RustType<ProtoAwsPrivatelink> for AwsPrivatelink {
    fn into_proto(&self) -> ProtoAwsPrivatelink {
        ProtoAwsPrivatelink {
            connection_id: Some(self.connection_id.into_proto()),
            availability_zone: self.availability_zone.into_proto(),
            port: self.port.into_proto(),
        }
    }

    fn from_proto(proto: ProtoAwsPrivatelink) -> Result<Self, TryFromProtoError> {
        Ok(AwsPrivatelink {
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoAwsPrivatelink::connection_id")?,
            availability_zone: proto.availability_zone.into_rust()?,
            port: proto.port.into_rust()?,
        })
    }
}

impl AlterCompatible for AwsPrivatelink {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let AwsPrivatelink {
            connection_id,
            availability_zone: _,
            port: _,
        } = self;

        let compatibility_checks = [(connection_id == &other.connection_id, "connection_id")];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "AwsPrivatelink incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

/// Specifies an SSH tunnel connection.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SshTunnel<C: ConnectionAccess = InlinedConnection> {
    /// id of the ssh connection
    pub connection_id: CatalogItemId,
    /// ssh connection object
    pub connection: C::Ssh,
}

impl<R: ConnectionResolver> IntoInlineConnection<SshTunnel, R> for SshTunnel<ReferencedConnection> {
    fn into_inline_connection(self, r: R) -> SshTunnel {
        let SshTunnel {
            connection,
            connection_id,
        } = self;

        SshTunnel {
            connection: r.resolve_connection(connection).unwrap_ssh(),
            connection_id,
        }
    }
}

impl RustType<ProtoSshTunnel> for SshTunnel<InlinedConnection> {
    fn into_proto(&self) -> ProtoSshTunnel {
        ProtoSshTunnel {
            connection_id: Some(self.connection_id.into_proto()),
            connection: Some(self.connection.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSshTunnel) -> Result<Self, TryFromProtoError> {
        Ok(SshTunnel {
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoSshTunnel::connection_id")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoSshTunnel::connection")?,
        })
    }
}

impl SshTunnel<InlinedConnection> {
    /// Like [`SshTunnelConfig::connect`], but the SSH key is loaded from a
    /// secret.
    async fn connect(
        &self,
        storage_configuration: &StorageConfiguration,
        remote_host: &str,
        remote_port: u16,
        in_task: InTask,
    ) -> Result<ManagedSshTunnelHandle, anyhow::Error> {
        // Ensure any ssh-bastion host we connect to is resolved to an external address.
        let resolved = resolve_address(
            &self.connection.host,
            ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
        )
        .await?;
        storage_configuration
            .connection_context
            .ssh_tunnel_manager
            .connect(
                SshTunnelConfig {
                    host: resolved
                        .iter()
                        .map(|a| a.to_string())
                        .collect::<BTreeSet<_>>(),
                    port: self.connection.port,
                    user: self.connection.user.clone(),
                    key_pair: SshKeyPair::from_bytes(
                        &storage_configuration
                            .connection_context
                            .secrets_reader
                            .read_in_task_if(in_task, self.connection_id)
                            .await?,
                    )?,
                },
                remote_host,
                remote_port,
                storage_configuration.parameters.ssh_timeout_config,
                in_task,
            )
            .await
    }
}

impl<C: ConnectionAccess> AlterCompatible for SshTunnel<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let SshTunnel {
            connection_id,
            connection,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SshTunnel incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl SshConnection {
    #[allow(clippy::unused_async)]
    async fn validate(
        &self,
        id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let secret = storage_configuration
            .connection_context
            .secrets_reader
            .read_in_task_if(
                // We are in a normal tokio context during validation, already.
                InTask::No,
                id,
            )
            .await?;
        let key_pair = SshKeyPair::from_bytes(&secret)?;

        // Ensure any ssh-bastion host we connect to is resolved to an external address.
        let resolved = resolve_address(
            &self.host,
            ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set()),
        )
        .await?;

        let config = SshTunnelConfig {
            host: resolved
                .iter()
                .map(|a| a.to_string())
                .collect::<BTreeSet<_>>(),
            port: self.port,
            user: self.user.clone(),
            key_pair,
        };
        // Note that we do NOT use the `SshTunnelManager` here, as we want to validate that we
        // can actually create a new connection to the ssh bastion, without tunneling.
        config
            .validate(storage_configuration.parameters.ssh_timeout_config)
            .await
    }

    fn validate_by_default(&self) -> bool {
        false
    }
}

impl AwsPrivatelinkConnection {
    #[allow(clippy::unused_async)]
    async fn validate(
        &self,
        id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let Some(ref cloud_resource_reader) = storage_configuration
            .connection_context
            .cloud_resource_reader
        else {
            return Err(anyhow!("AWS PrivateLink connections are unsupported"));
        };

        // No need to optionally run this in a task, as we are just validating from envd.
        let status = cloud_resource_reader.read(id).await?;

        let availability = status
            .conditions
            .as_ref()
            .and_then(|conditions| conditions.iter().find(|c| c.type_ == "Available"));

        match availability {
            Some(condition) if condition.status == "True" => Ok(()),
            Some(condition) => Err(anyhow!("{}", condition.message)),
            None => Err(anyhow!("Endpoint availability is unknown")),
        }
    }

    fn validate_by_default(&self) -> bool {
        false
    }
}
