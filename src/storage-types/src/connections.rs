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
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use mz_ccsr::tls::{Certificate, Identity};
use mz_cloud_resources::{AwsExternalIdPrefix, CloudResourceReader};
use mz_kafka_util::client::{BrokerRewrite, MzClientContext, MzKafkaError, TunnelingClientContext};
use mz_ore::error::ErrorExt;
use mz_proto::tokio_postgres::any_ssl_mode;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::url::any_url;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use mz_ssh_util::keys::SshKeyPair;
use mz_ssh_util::tunnel::SshTunnelConfig;
use mz_ssh_util::tunnel_manager::{ManagedSshTunnelHandle, SshTunnelManager};
use mz_tracing::CloneableEnvFilter;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use rdkafka::client::BrokerAddr;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::ClientContext;
use serde::{Deserialize, Serialize};
use tokio::net;
use tokio::runtime::Handle;
use tokio_postgres::config::SslMode;
use url::Url;

use crate::configuration::StorageConfiguration;
use crate::connections::aws::{AwsConnection, AwsConnectionValidationError};
use crate::errors::{ContextCreationError, CsrConnectError};

pub mod aws;
pub mod inline;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.connections.rs"));

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum StringOrSecret {
    String(String),
    Secret(GlobalId),
}

impl StringOrSecret {
    /// Gets the value as a string, reading the secret if necessary.
    pub async fn get_string(&self, secrets_reader: &dyn SecretsReader) -> anyhow::Result<String> {
        match self {
            StringOrSecret::String(s) => Ok(s.clone()),
            StringOrSecret::Secret(id) => secrets_reader.read_string(*id).await,
        }
    }

    /// Asserts that this string or secret is a string and returns its contents.
    pub fn unwrap_string(&self) -> &str {
        match self {
            StringOrSecret::String(s) => s,
            StringOrSecret::Secret(_) => panic!("StringOrSecret::unwrap_string called on a secret"),
        }
    }

    /// Asserts that this string or secret is a secret and returns its global
    /// ID.
    pub fn unwrap_secret(&self) -> GlobalId {
        match self {
            StringOrSecret::String(_) => panic!("StringOrSecret::unwrap_secret called on a string"),
            StringOrSecret::Secret(id) => *id,
        }
    }
}

impl RustType<ProtoStringOrSecret> for StringOrSecret {
    fn into_proto(&self) -> ProtoStringOrSecret {
        use proto_string_or_secret::Kind;
        ProtoStringOrSecret {
            kind: Some(match self {
                StringOrSecret::String(s) => Kind::String(s.clone()),
                StringOrSecret::Secret(id) => Kind::Secret(id.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoStringOrSecret) -> Result<Self, TryFromProtoError> {
        use proto_string_or_secret::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoStringOrSecret::kind"))?;
        Ok(match kind {
            Kind::String(s) => StringOrSecret::String(s),
            Kind::Secret(id) => StringOrSecret::Secret(GlobalId::from_proto(id)?),
        })
    }
}

impl<V: std::fmt::Display> From<V> for StringOrSecret {
    fn from(v: V) -> StringOrSecret {
        StringOrSecret::String(format!("{}", v))
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
            aws_external_id_prefix: None,
            aws_connection_role_arn: None,
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
        }
    }
}

impl Connection<InlinedConnection> {
    /// Validates this connection by attempting to connect to the upstream system.
    pub async fn validate(
        &self,
        id: GlobalId,
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

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AwsPrivatelinkConnection {
    pub service_name: String,
    pub availability_zones: Vec<String>,
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaTlsConfig {
    pub identity: Option<TlsIdentity>,
    pub root_cert: Option<StringOrSecret>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaSaslConfig {
    pub mechanism: String,
    pub username: StringOrSecret,
    pub password: GlobalId,
}

impl Arbitrary for KafkaSaslConfig {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            StringOrSecret::arbitrary(),
            GlobalId::arbitrary(),
        )
            .prop_map(|(mechanism, username, password)| KafkaSaslConfig {
                mechanism,
                username,
                password,
            })
            .boxed()
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

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaConnection<C: ConnectionAccess = InlinedConnection> {
    pub brokers: Vec<KafkaBroker<C>>,
    /// A tunnel through which to route traffic,
    /// that can be overridden for individual brokers
    /// in `brokers`.
    pub default_tunnel: Tunnel<C>,
    pub progress_topic: Option<String>,
    pub options: BTreeMap<String, StringOrSecret>,
    pub tls: Option<KafkaTlsConfig>,
    pub sasl: Option<KafkaSaslConfig>,
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaConnection, R>
    for KafkaConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaConnection {
        let KafkaConnection {
            brokers,
            progress_topic,
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
            default_tunnel: default_tunnel.into_inline_connection(&r),
            options,
            tls,
            sasl,
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
        connection_id: GlobalId,
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
    /// Creates a Kafka client for the connection.
    pub async fn create_with_context<C, T>(
        &self,
        storage_configuration: &StorageConfiguration,
        context: C,
        extra_options: &BTreeMap<&str, String>,
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

        options.insert(
            "bootstrap.servers".into(),
            self.brokers.iter().map(|b| &b.address).join(",").into(),
        );
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
            options.insert(
                "sasl.password".into(),
                StringOrSecret::Secret(sasl.password),
            );
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
                v.get_string(&*storage_configuration.connection_context.secrets_reader)
                    .await
                    .context("reading kafka secret")?,
            );
        }
        for (k, v) in extra_options {
            config.set(*k, v);
        }

        let mut context = TunnelingClientContext::new(
            context,
            Handle::current(),
            storage_configuration
                .connection_context
                .ssh_tunnel_manager
                .clone(),
            storage_configuration.parameters.ssh_timeout_config,
        );

        match &self.default_tunnel {
            Tunnel::Direct => {
                // By default, don't offer a default override for broker address lookup.
            }
            Tunnel::AwsPrivatelink(_) => {
                unreachable!("top-level AwsPrivatelink tunnels are not supported yet")
            }
            Tunnel::Ssh(ssh_tunnel) => {
                let secret = storage_configuration
                    .connection_context
                    .secrets_reader
                    .read(ssh_tunnel.connection_id)
                    .await?;
                let key_pair = SshKeyPair::from_bytes(&secret)?;

                context.set_default_ssh_tunnel(SshTunnelConfig {
                    host: ssh_tunnel.connection.host.clone(),
                    port: ssh_tunnel.connection.port,
                    user: ssh_tunnel.connection.user.clone(),
                    key_pair,
                });
            }
        }

        for broker in &self.brokers {
            let mut addr_parts = broker.address.splitn(2, ':');
            let addr = BrokerAddr {
                host: addr_parts
                    .next()
                    .context("BROKER is not address:port")?
                    .into(),
                port: addr_parts.next().unwrap_or("9092").into(),
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
                    context
                        .add_ssh_tunnel(
                            addr,
                            SshTunnelConfig {
                                host: ssh_tunnel.connection.host.clone(),
                                port: ssh_tunnel.connection.port,
                                user: ssh_tunnel.connection.user.clone(),
                                key_pair: SshKeyPair::from_bytes(
                                    &storage_configuration
                                        .connection_context
                                        .secrets_reader
                                        .read(ssh_tunnel.connection_id)
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
        _id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let (context, error_rx) = MzClientContext::with_errors();
        let consumer: BaseConsumer<_> = self
            .create_with_context(storage_configuration, context, &BTreeMap::new())
            .await?;

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
        let result = mz_ore::task::spawn_blocking(
            || "kafka_get_metadata",
            move || consumer.fetch_metadata(None, timeout),
        )
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

                match main_err {
                    Some(err) => Err(err.into()),
                    None => Err(err.into()),
                }
            }
        }
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
            password: Some(self.password.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaConnectionSaslConfig) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSaslConfig {
            mechanism: proto.mechanism,
            username: proto
                .username
                .into_rust_if_some("ProtoKafkaConnectionSaslConfig::username")?,
            password: proto
                .password
                .into_rust_if_some("ProtoKafkaConnectionSaslConfig::password")?,
        })
    }
}

impl RustType<ProtoKafkaConnection> for KafkaConnection {
    fn into_proto(&self) -> ProtoKafkaConnection {
        ProtoKafkaConnection {
            brokers: self.brokers.into_proto(),
            default_tunnel: Some(self.default_tunnel.into_proto()),
            progress_topic: self.progress_topic.into_proto(),
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CsrConnection<C: ConnectionAccess = InlinedConnection> {
    /// The URL of the schema registry.
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
    ) -> Result<mz_ccsr::Client, CsrConnectError> {
        let mut client_config = mz_ccsr::ClientConfig::new(self.url.clone());
        if let Some(root_cert) = &self.tls_root_cert {
            let root_cert = root_cert
                .get_string(&*storage_configuration.connection_context.secrets_reader)
                .await?;
            let root_cert = Certificate::from_pem(root_cert.as_bytes())?;
            client_config = client_config.add_root_certificate(root_cert);
        }

        if let Some(tls_identity) = &self.tls_identity {
            let key = &storage_configuration
                .connection_context
                .secrets_reader
                .read_string(tls_identity.key)
                .await?;
            let cert = tls_identity
                .cert
                .get_string(&*storage_configuration.connection_context.secrets_reader)
                .await?;
            // `reqwest` expects identity `pem` files to contain one key and
            // at least one certificate.
            let mut buf = Vec::new();
            buf.extend(key.as_bytes());
            buf.push(b'\n');
            buf.extend(cert.as_bytes());
            let ident = Identity::from_pem(&buf)?;
            client_config = client_config.identity(ident);
        }

        if let Some(http_auth) = &self.http_auth {
            let username = http_auth
                .username
                .get_string(&*storage_configuration.connection_context.secrets_reader)
                .await?;
            let password = match http_auth.password {
                None => None,
                Some(password) => Some(
                    storage_configuration
                        .connection_context
                        .secrets_reader
                        .read_string(password)
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

        match &self.tunnel {
            Tunnel::Direct => {}
            Tunnel::Ssh(ssh_tunnel) => {
                // TODO: use types to enforce that the URL has a string hostname.
                let host = self
                    .url
                    .host_str()
                    .ok_or_else(|| anyhow!("url missing host"))?;

                let ssh_tunnel = ssh_tunnel
                    .connect(
                        storage_configuration,
                        host,
                        // Default to the default http port, but this
                        // could default to 8081...
                        self.url.port().unwrap_or(80),
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
                assert!(connection.port.is_none());

                // TODO: use types to enforce that the URL has a string hostname.
                let host = self
                    .url
                    .host_str()
                    .ok_or_else(|| anyhow!("url missing host"))?;
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
        _id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let client = self.connect(storage_configuration).await?;
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

impl<C: ConnectionAccess> Arbitrary for CsrConnection<C> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any_url(),
            any::<Option<StringOrSecret>>(),
            any::<Option<TlsIdentity>>(),
            any::<Option<CsrConnectionHttpAuth>>(),
            any::<Tunnel<C>>(),
        )
            .prop_map(
                |(url, tls_root_cert, tls_identity, http_auth, tunnel)| CsrConnection {
                    url,
                    tls_root_cert,
                    tls_identity,
                    http_auth,
                    tunnel,
                },
            )
            .boxed()
    }
}

/// A TLS key pair used for client identity.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TlsIdentity {
    /// The client's TLS public certificate in PEM format.
    pub cert: StringOrSecret,
    /// The ID of the secret containing the client's TLS private key in PEM
    /// format.
    pub key: GlobalId,
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
    pub password: Option<GlobalId>,
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
    pub password: Option<GlobalId>,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
    /// Whether to use TLS for encryption, authentication, or both.
    pub tls_mode: SslMode,
    /// An optional root TLS certificate in PEM format, to verify the server's
    /// identity.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication.
    pub tls_identity: Option<TlsIdentity>,
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
        secrets_reader: &dyn mz_secrets::SecretsReader,
        storage_configuration: &StorageConfiguration,
    ) -> Result<mz_postgres_util::Config, anyhow::Error> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user.get_string(secrets_reader).await?)
            .ssl_mode(self.tls_mode);
        if let Some(password) = self.password {
            let password = secrets_reader.read_string(password).await?;
            config.password(password);
        }
        if let Some(tls_root_cert) = &self.tls_root_cert {
            let tls_root_cert = tls_root_cert.get_string(secrets_reader).await?;
            config.ssl_root_cert(tls_root_cert.as_bytes());
        }
        if let Some(tls_identity) = &self.tls_identity {
            let cert = tls_identity.cert.get_string(secrets_reader).await?;
            let key = secrets_reader.read_string(tls_identity.key).await?;
            config.ssl_cert(cert.as_bytes()).ssl_key(key.as_bytes());
        }

        let tunnel = match &self.tunnel {
            Tunnel::Direct => mz_postgres_util::TunnelConfig::Direct,
            Tunnel::Ssh(SshTunnel {
                connection_id,
                connection,
            }) => {
                let secret = secrets_reader.read(*connection_id).await?;
                let key_pair = SshKeyPair::from_bytes(&secret)?;
                mz_postgres_util::TunnelConfig::Ssh {
                    config: SshTunnelConfig {
                        host: connection.host.clone(),
                        port: connection.port,
                        user: connection.user.clone(),
                        key_pair,
                    },
                }
            }
            Tunnel::AwsPrivatelink(connection) => {
                assert!(connection.port.is_none());
                mz_postgres_util::TunnelConfig::AwsPrivatelink {
                    connection_id: connection.connection_id,
                }
            }
        };

        Ok(mz_postgres_util::Config::new(
            config,
            tunnel,
            storage_configuration
                .parameters
                .pg_source_tcp_timeouts
                .clone(),
            storage_configuration.parameters.ssh_timeout_config,
        )?)
    }

    async fn validate(
        &self,
        _id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let config = self
            .config(
                &*storage_configuration.connection_context.secrets_reader,
                storage_configuration,
            )
            .await?;
        config
            .connect(
                "connection validation",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;
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
        })
    }
}

impl<C: ConnectionAccess> Arbitrary for PostgresConnection<C> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<u16>(),
            any::<String>(),
            any::<StringOrSecret>(),
            any::<Option<GlobalId>>(),
            any::<Tunnel<C>>(),
            any_ssl_mode(),
            any::<Option<StringOrSecret>>(),
            any::<Option<TlsIdentity>>(),
        )
            .prop_map(
                |(
                    host,
                    port,
                    database,
                    user,
                    password,
                    tunnel,
                    tls_mode,
                    tls_root_cert,
                    tls_identity,
                )| {
                    PostgresConnection {
                        host,
                        port,
                        database,
                        user,
                        password,
                        tunnel,
                        tls_mode,
                        tls_root_cert,
                        tls_identity,
                    }
                },
            )
            .boxed()
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
        Ok(match ProtoMySqlSslMode::from_i32(proto) {
            Some(ProtoMySqlSslMode::Disabled) => MySqlSslMode::Disabled,
            Some(ProtoMySqlSslMode::Required) => MySqlSslMode::Required,
            Some(ProtoMySqlSslMode::VerifyCa) => MySqlSslMode::VerifyCa,
            Some(ProtoMySqlSslMode::VerifyIdentity) => MySqlSslMode::VerifyIdentity,
            None => {
                return Err(TryFromProtoError::UnknownEnumVariant(
                    "tls_mode".to_string(),
                ))
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MySqlConnection<C: ConnectionAccess = InlinedConnection> {
    /// The hostname of the server.
    pub host: String,
    /// The port of the server.
    pub port: u16,
    /// The username to authenticate as.
    pub user: StringOrSecret,
    /// An optional password for authentication.
    pub password: Option<GlobalId>,
    /// A tunnel through which to route traffic.
    pub tunnel: Tunnel<C>,
    /// Whether to use TLS for encryption, verify the server's certificate, and identity.
    pub tls_mode: MySqlSslMode,
    /// An optional root TLS certificate in PEM format, to verify the server's
    /// identity.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication.
    pub tls_identity: Option<TlsIdentity>,
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
        } = self;

        MySqlConnection {
            host,
            port,
            user,
            password,
            tunnel: tunnel.into_inline_connection(r),
            tls_mode,
            tls_root_cert,
            tls_identity,
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
        secrets_reader: &dyn mz_secrets::SecretsReader,
        _storage_configuration: &StorageConfiguration,
    ) -> Result<mz_mysql_util::Config, anyhow::Error> {
        // TODO(roshan): Set appropriate connection timeouts
        let mut opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user.get_string(secrets_reader).await?));

        if let Some(password) = self.password {
            let password = secrets_reader.read_string(password).await?;
            opts = opts.pass(Some(password));
        }

        // TODO(roshan): Implement SSL mode support
        if self.tls_mode != MySqlSslMode::Disabled {
            return Err(anyhow!("MySQL TLS modes are not yet supported"));
        };

        // TODO(roshan): Implement Root TLS Cert support
        if self.tls_root_cert.is_some() {
            return Err(anyhow!("MySQL TLS Certs are not yet supported"));
        }

        // TODO(roshan): Implement SSH Tunnels, AWS Privatelink
        let tunnel = match &self.tunnel {
            Tunnel::Direct => mz_mysql_util::TunnelConfig::Direct,
            _ => {
                return Err(anyhow!("MySQL Tunnels are not yet supported"));
            }
        };

        Ok(mz_mysql_util::Config::new(opts.into(), tunnel))
    }

    async fn validate(
        &self,
        _id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let config = self
            .config(
                &*storage_configuration.connection_context.secrets_reader,
                storage_configuration,
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
        })
    }
}

impl<C: ConnectionAccess> Arbitrary for MySqlConnection<C> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<u16>(),
            any::<StringOrSecret>(),
            any::<Option<GlobalId>>(),
            any::<Tunnel<C>>(),
            any_mysql_ssl_mode(),
            any::<Option<StringOrSecret>>(),
            any::<Option<TlsIdentity>>(),
        )
            .prop_map(
                |(host, port, user, password, tunnel, tls_mode, tls_root_cert, tls_identity)| {
                    MySqlConnection {
                        host,
                        port,
                        user,
                        password,
                        tunnel,
                        tls_mode,
                        tls_root_cert,
                        tls_identity,
                    }
                },
            )
            .boxed()
    }
}

/// A connection to a SSH tunnel.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SshConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub public_keys: Option<(String, String)>,
}

use proto_ssh_connection::ProtoPublicKeys;

use self::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};

impl RustType<ProtoPublicKeys> for (String, String) {
    fn into_proto(&self) -> ProtoPublicKeys {
        ProtoPublicKeys {
            primary_public_key: self.0.into_proto(),
            secondary_public_key: self.1.into_proto(),
        }
    }

    fn from_proto(proto: ProtoPublicKeys) -> Result<Self, TryFromProtoError> {
        Ok((proto.primary_public_key, proto.secondary_public_key))
    }
}

impl RustType<ProtoSshConnection> for SshConnection {
    fn into_proto(&self) -> ProtoSshConnection {
        ProtoSshConnection {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            user: self.user.into_proto(),
            public_keys: self.public_keys.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSshConnection) -> Result<Self, TryFromProtoError> {
        Ok(SshConnection {
            host: proto.host,
            port: proto.port.into_rust()?,
            user: proto.user,
            public_keys: proto.public_keys.into_rust()?,
        })
    }
}

/// Specifies an AWS PrivateLink service for a [`Tunnel`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AwsPrivatelink {
    /// The ID of the connection to the AWS PrivateLink service.
    pub connection_id: GlobalId,
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

/// Specifies an AWS PrivateLink service for a [`Tunnel`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SshTunnel<C: ConnectionAccess = InlinedConnection> {
    /// id of the ssh connection
    pub connection_id: GlobalId,
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
    ) -> Result<ManagedSshTunnelHandle, anyhow::Error> {
        storage_configuration
            .connection_context
            .ssh_tunnel_manager
            .connect(
                SshTunnelConfig {
                    host: self.connection.host.clone(),
                    port: self.connection.port,
                    user: self.connection.user.clone(),
                    key_pair: SshKeyPair::from_bytes(
                        &storage_configuration
                            .connection_context
                            .secrets_reader
                            .read(self.connection_id)
                            .await?,
                    )?,
                },
                remote_host,
                remote_port,
                storage_configuration.parameters.ssh_timeout_config,
            )
            .await
    }
}
impl SshConnection {
    #[allow(clippy::unused_async)]
    async fn validate(
        &self,
        id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let secret = storage_configuration
            .connection_context
            .secrets_reader
            .read(id)
            .await?;
        let key_pair = SshKeyPair::from_bytes(&secret)?;
        let config = SshTunnelConfig {
            host: self.host.clone(),
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
        id: GlobalId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), anyhow::Error> {
        let Some(ref cloud_resource_reader) = storage_configuration
            .connection_context
            .cloud_resource_reader
        else {
            return Err(anyhow!("AWS PrivateLink connections are unsupported"));
        };

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
