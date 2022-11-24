// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection types.

use std::collections::BTreeMap;

use std::sync::Arc;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::ClientContext;
use serde::{Deserialize, Serialize};
use tokio::net;
use tokio_postgres::config::SslMode;
use url::Url;

use mz_ccsr::tls::{Certificate, Identity};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_kafka_util::client::BrokerRewritingClientContext;
use mz_proto::tokio_postgres::any_ssl_mode;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::url::any_url;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use mz_ssh_util::keys::SshKeyPairSet;
use mz_ssh_util::tunnel::SshTunnelConfig;

use crate::types::connections::aws::AwsConfig;

pub mod aws;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.connections.rs"
));

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
    /// The level for librdkafka's logs.
    pub librdkafka_log_level: tracing::Level,
    /// A prefix for an external ID to use for all AWS AssumeRole operations.
    pub aws_external_id_prefix: Option<AwsExternalIdPrefix>,
    /// A secrets reader.
    pub secrets_reader: Arc<dyn SecretsReader>,
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
        filter: &tracing_subscriber::filter::Targets,
        aws_external_id_prefix: Option<AwsExternalIdPrefix>,
        secrets_reader: Arc<dyn SecretsReader>,
    ) -> ConnectionContext {
        ConnectionContext {
            librdkafka_log_level: mz_ore::tracing::target_level(filter, "librdkafka"),
            aws_external_id_prefix,
            secrets_reader,
        }
    }

    /// Constructs a new connection context for usage in tests.
    pub fn for_tests(secrets_reader: Arc<dyn SecretsReader>) -> ConnectionContext {
        ConnectionContext {
            librdkafka_log_level: tracing::Level::INFO,
            aws_external_id_prefix: None,
            secrets_reader,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Connection {
    Kafka(KafkaConnection),
    Csr(CsrConnection),
    Postgres(PostgresConnection),
    Ssh(SshConnection),
    Aws(AwsConfig),
    AwsPrivatelink(AwsPrivatelinkConnection),
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
pub struct SaslConfig {
    pub mechanisms: String,
    pub username: StringOrSecret,
    pub password: GlobalId,
    pub tls_root_cert: Option<StringOrSecret>,
}

impl Arbitrary for SaslConfig {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            StringOrSecret::arbitrary(),
            GlobalId::arbitrary(),
            proptest::option::of(any::<StringOrSecret>()),
        )
            .prop_map(
                |(mechanisms, username, password, tls_root_cert)| SaslConfig {
                    mechanisms,
                    username,
                    password,
                    tls_root_cert,
                },
            )
            .boxed()
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum KafkaSecurity {
    Tls(KafkaTlsConfig),
    Sasl(SaslConfig),
}

impl From<KafkaTlsConfig> for KafkaSecurity {
    fn from(c: KafkaTlsConfig) -> Self {
        KafkaSecurity::Tls(c)
    }
}

impl From<SaslConfig> for KafkaSecurity {
    fn from(c: SaslConfig) -> Self {
        KafkaSecurity::Sasl(c)
    }
}

/// Specifies a Kafka broker in a [`KafkaConnection`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaBroker {
    /// The address of the Kafka broker.
    pub address: String,
    /// An optional AWS PrivateLink service through
    pub aws_privatelink: Option<KafkaAwsPrivateLink>,
}

impl RustType<ProtoKafkaBroker> for KafkaBroker {
    fn into_proto(&self) -> ProtoKafkaBroker {
        ProtoKafkaBroker {
            address: self.address.into_proto(),
            aws_privatelink: self
                .aws_privatelink
                .as_ref()
                .map(|aws_privatelink| aws_privatelink.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaBroker) -> Result<Self, TryFromProtoError> {
        Ok(KafkaBroker {
            address: proto.address.into_rust()?,
            aws_privatelink: proto.aws_privatelink.into_rust()?,
        })
    }
}

/// Specifies an AWS PrivateLink service for a [`KafkaBroker`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaAwsPrivateLink {
    /// The ID of the connection to the AWS PrivateLink service.
    pub connection_id: GlobalId,
    /// The port to use when connecting to the AWS PrivateLink service, if
    /// different from the port in [`KafkaBroker::address`].
    pub port: Option<u16>,
}

impl RustType<ProtoKafkaAwsPrivateLink> for KafkaAwsPrivateLink {
    fn into_proto(&self) -> ProtoKafkaAwsPrivateLink {
        ProtoKafkaAwsPrivateLink {
            connection_id: Some(self.connection_id.into_proto()),
            port: self.port.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaAwsPrivateLink) -> Result<Self, TryFromProtoError> {
        Ok(KafkaAwsPrivateLink {
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoKafkaAwsPrivateLink::connection_id")?,
            port: proto.port.into_rust()?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct KafkaConnection {
    pub brokers: Vec<KafkaBroker>,
    pub progress_topic: Option<String>,
    pub security: Option<KafkaSecurity>,
    pub options: BTreeMap<String, StringOrSecret>,
}

impl KafkaConnection {
    /// Creates a Kafka client for the connection.
    pub async fn create_with_context<C, T>(
        &self,
        connection_context: &ConnectionContext,
        context: C,
        extra_options: &BTreeMap<&str, String>,
    ) -> Result<T, anyhow::Error>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<BrokerRewritingClientContext<C>>,
    {
        let mut options = self.options.clone();
        options.insert(
            "bootstrap.servers".into(),
            self.brokers.iter().map(|b| &b.address).join(",").into(),
        );
        match self.security.clone() {
            Some(KafkaSecurity::Tls(KafkaTlsConfig {
                root_cert,
                identity,
            })) => {
                options.insert("security.protocol".into(), "SSL".into());
                if let Some(root_cert) = root_cert {
                    options.insert("ssl.ca.pem".into(), root_cert);
                }
                if let Some(identity) = identity {
                    options.insert("ssl.key.pem".into(), StringOrSecret::Secret(identity.key));
                    options.insert("ssl.certificate.pem".into(), identity.cert);
                }
            }
            Some(KafkaSecurity::Sasl(SaslConfig {
                mechanisms,
                username,
                password,
                tls_root_cert: certificate_authority,
            })) => {
                options.insert("security.protocol".into(), "SASL_SSL".into());
                options.insert("sasl.mechanisms".into(), StringOrSecret::String(mechanisms));
                options.insert("sasl.username".into(), username);
                options.insert("sasl.password".into(), StringOrSecret::Secret(password));
                if let Some(certificate_authority) = certificate_authority {
                    options.insert("ssl.ca.pem".into(), certificate_authority);
                }
            }
            None => (),
        }

        let mut config = mz_kafka_util::client::create_new_client_config(
            connection_context.librdkafka_log_level,
        );
        for (k, v) in options {
            config.set(
                k,
                v.get_string(&*connection_context.secrets_reader)
                    .await
                    .expect("reading kafka secret unexpectedly failed"),
            );
        }
        for (k, v) in extra_options {
            config.set(*k, v);
        }

        let mut context = BrokerRewritingClientContext::new(context);
        for broker in &self.brokers {
            if let Some(aws_privatelink) = &broker.aws_privatelink {
                context.add_broker_rewrite(
                    &broker.address,
                    &mz_cloud_resources::vpc_endpoint_name(aws_privatelink.connection_id),
                    aws_privatelink.port,
                );
            }
        }

        Ok(config.create_with_context(context)?)
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

impl RustType<ProtoKafkaConnectionSaslConfig> for SaslConfig {
    fn into_proto(&self) -> ProtoKafkaConnectionSaslConfig {
        ProtoKafkaConnectionSaslConfig {
            mechanisms: self.mechanisms.into_proto(),
            username: Some(self.username.into_proto()),
            password: Some(self.password.into_proto()),
            tls_root_cert: self.tls_root_cert.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaConnectionSaslConfig) -> Result<Self, TryFromProtoError> {
        Ok(SaslConfig {
            mechanisms: proto.mechanisms,
            username: proto
                .username
                .into_rust_if_some("ProtoKafkaConnectionSaslConfig::username")?,
            password: proto
                .password
                .into_rust_if_some("ProtoKafkaConnectionSaslConfig::password")?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
        })
    }
}

impl RustType<ProtoKafkaConnectionSecurity> for KafkaSecurity {
    fn into_proto(&self) -> ProtoKafkaConnectionSecurity {
        use proto_kafka_connection_security::Kind;
        ProtoKafkaConnectionSecurity {
            kind: Some(match self {
                KafkaSecurity::Tls(config) => Kind::Tls(config.into_proto()),
                KafkaSecurity::Sasl(config) => Kind::Sasl(config.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoKafkaConnectionSecurity) -> Result<Self, TryFromProtoError> {
        use proto_kafka_connection_security::Kind;
        let kind = proto.kind.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoKafkaConnectionSecurity::kind")
        })?;
        Ok(match kind {
            Kind::Tls(s) => KafkaSecurity::Tls(KafkaTlsConfig::from_proto(s)?),
            Kind::Sasl(s) => KafkaSecurity::Sasl(SaslConfig::from_proto(s)?),
        })
    }
}

impl RustType<ProtoKafkaConnection> for KafkaConnection {
    fn into_proto(&self) -> ProtoKafkaConnection {
        ProtoKafkaConnection {
            brokers: self.brokers.into_proto(),
            progress_topic: self.progress_topic.into_proto(),
            security: self.security.into_proto(),
            options: self
                .options
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
        }
    }

    fn from_proto(proto: ProtoKafkaConnection) -> Result<Self, TryFromProtoError> {
        Ok(KafkaConnection {
            brokers: proto.brokers.into_rust()?,
            progress_topic: proto.progress_topic,
            security: proto.security.into_rust()?,
            options: proto
                .options
                .into_iter()
                .map(|(k, v)| StringOrSecret::from_proto(v).map(|v| (k, v)))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// A connection to a Confluent Schema Registry.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CsrConnection {
    /// The URL of the schema registry.
    pub url: Url,
    /// Trusted root TLS certificate in PEM format.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication with the schema
    /// registry.
    pub tls_identity: Option<TlsIdentity>,
    /// Optional HTTP authentication credentials for the schema registry.
    pub http_auth: Option<CsrConnectionHttpAuth>,
    /// The ID of an AWS PrivateLink connection through which to tunnel.
    pub aws_privatelink_id: Option<GlobalId>,
}

impl CsrConnection {
    /// Constructs a schema registry client from the connection.
    pub async fn connect(
        &self,
        secrets_reader: &dyn SecretsReader,
    ) -> Result<mz_ccsr::Client, anyhow::Error> {
        let mut client_config = mz_ccsr::ClientConfig::new(self.url.clone());
        if let Some(root_cert) = &self.tls_root_cert {
            let root_cert = root_cert.get_string(secrets_reader).await?;
            let root_cert = Certificate::from_pem(root_cert.as_bytes())?;
            client_config = client_config.add_root_certificate(root_cert);
        }

        if let Some(tls_identity) = &self.tls_identity {
            let key = secrets_reader.read_string(tls_identity.key).await?;
            let cert = tls_identity.cert.get_string(secrets_reader).await?;
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
            let username = http_auth.username.get_string(secrets_reader).await?;
            let password = match http_auth.password {
                None => None,
                Some(password) => Some(secrets_reader.read_string(password).await?),
            };
            client_config = client_config.auth(username, password);
        }

        if let Some(aws_privatelink_id) = self.aws_privatelink_id {
            // `net::lookup_host` requires a port but the port will be ignored
            // when passed to `resolve_to_addrs`. We use a dummy port that will
            // be easy to spot in the logs to make it obvious if some component
            // downstream incorrectly starts using this port.
            const DUMMY_PORT: u16 = 11111;

            // TODO: use types to enforce that the URL has a string hostname.
            let host = self
                .url
                .host_str()
                .ok_or_else(|| anyhow!("url missing host"))?;
            let privatelink_host = mz_cloud_resources::vpc_endpoint_name(aws_privatelink_id);
            let addrs: Vec<_> = net::lookup_host((privatelink_host, DUMMY_PORT))
                .await
                .context("resolving PrivateLink host")?
                .collect();
            client_config = client_config.resolve_to_addrs(host, &addrs)
        }

        client_config.build()
    }
}

impl RustType<ProtoCsrConnection> for CsrConnection {
    fn into_proto(&self) -> ProtoCsrConnection {
        ProtoCsrConnection {
            url: Some(self.url.into_proto()),
            tls_root_cert: self.tls_root_cert.into_proto(),
            tls_identity: self.tls_identity.into_proto(),
            http_auth: self.http_auth.into_proto(),
            aws_privatelink_id: self.aws_privatelink_id.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCsrConnection) -> Result<Self, TryFromProtoError> {
        Ok(CsrConnection {
            url: proto.url.into_rust_if_some("ProtoCsrConnection::url")?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
            tls_identity: proto.tls_identity.into_rust()?,
            http_auth: proto.http_auth.into_rust()?,
            aws_privatelink_id: proto.aws_privatelink_id.into_rust()?,
        })
    }
}

impl Arbitrary for CsrConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any_url(),
            any::<Option<StringOrSecret>>(),
            any::<Option<TlsIdentity>>(),
            any::<Option<CsrConnectionHttpAuth>>(),
            any::<Option<GlobalId>>(),
        )
            .prop_map(
                |(url, tls_root_cert, tls_identity, http_auth, aws_privatelink_id)| CsrConnection {
                    url,
                    tls_root_cert,
                    tls_identity,
                    http_auth,
                    aws_privatelink_id,
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
pub struct PostgresConnection {
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
    pub tunnel: PostgresTunnel,
    /// Whether to use TLS for encryption, authentication, or both.
    pub tls_mode: SslMode,
    /// An optional root TLS certificate in PEM format, to verify the server's
    /// identity.
    pub tls_root_cert: Option<StringOrSecret>,
    /// An optional TLS client certificate for authentication.
    pub tls_identity: Option<TlsIdentity>,
}

impl PostgresConnection {
    pub async fn config(
        &self,
        secrets_reader: &dyn mz_secrets::SecretsReader,
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
            PostgresTunnel::Direct => mz_postgres_util::TunnelConfig::Direct,
            PostgresTunnel::Ssh {
                connection_id,
                connection,
            } => {
                let secret = secrets_reader.read(*connection_id).await?;
                let key_set = SshKeyPairSet::from_bytes(&secret)?;
                let key_pair = key_set.primary().clone();
                mz_postgres_util::TunnelConfig::Ssh(SshTunnelConfig {
                    host: connection.host.clone(),
                    port: connection.port,
                    user: connection.user.clone(),
                    key_pair,
                })
            }
            PostgresTunnel::AwsPrivateLink { connection_id } => {
                mz_postgres_util::TunnelConfig::AwsPrivatelink {
                    connection_id: *connection_id,
                }
            }
        };

        Ok(mz_postgres_util::Config::new(config, tunnel)?)
    }
}

impl RustType<ProtoPostgresConnection> for PostgresConnection {
    fn into_proto(&self) -> ProtoPostgresConnection {
        use proto_postgres_connection::Tunnel;
        ProtoPostgresConnection {
            host: self.host.into_proto(),
            port: self.port.into_proto(),
            database: self.database.into_proto(),
            user: Some(self.user.into_proto()),
            password: self.password.into_proto(),
            tls_mode: Some(self.tls_mode.into_proto()),
            tls_root_cert: self.tls_root_cert.into_proto(),
            tls_identity: self.tls_identity.into_proto(),
            tunnel: Some(match &self.tunnel {
                PostgresTunnel::Direct => Tunnel::Direct(()),
                PostgresTunnel::Ssh {
                    connection_id,
                    connection,
                } => Tunnel::Ssh(ProtoPostgresSshTunnel {
                    connection_id: Some(connection_id.into_proto()),
                    connection: Some(connection.into_proto()),
                }),
                PostgresTunnel::AwsPrivateLink { connection_id } => {
                    Tunnel::AwsPrivatelink(connection_id.into_proto())
                }
            }),
        }
    }

    fn from_proto(proto: ProtoPostgresConnection) -> Result<Self, TryFromProtoError> {
        use proto_postgres_connection::Tunnel;
        Ok(PostgresConnection {
            host: proto.host,
            port: proto.port.into_rust()?,
            database: proto.database,
            user: proto
                .user
                .into_rust_if_some("ProtoPostgresConnection::user")?,
            password: proto.password.into_rust()?,
            tunnel: match proto.tunnel {
                None => {
                    return Err(TryFromProtoError::missing_field(
                        "ProtoPostgresConnection::tunnel",
                    ))
                }
                Some(Tunnel::Direct(())) => PostgresTunnel::Direct,
                Some(Tunnel::Ssh(ssh)) => PostgresTunnel::Ssh {
                    connection_id: ssh
                        .connection_id
                        .into_rust_if_some("ProtoPostgresConnection::ssh::connection_id")?,
                    connection: ssh
                        .connection
                        .into_rust_if_some("ProtoPostgresConnection::ssh::connection")?,
                },
                Some(Tunnel::AwsPrivatelink(connection_id)) => PostgresTunnel::AwsPrivateLink {
                    connection_id: connection_id.into_rust()?,
                },
            },
            tls_mode: proto
                .tls_mode
                .into_rust_if_some("ProtoPostgresConnection::tls_mode")?,
            tls_root_cert: proto.tls_root_cert.into_rust()?,
            tls_identity: proto.tls_identity.into_rust()?,
        })
    }
}

impl Arbitrary for PostgresConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<String>(),
            any::<u16>(),
            any::<String>(),
            any::<StringOrSecret>(),
            any::<Option<GlobalId>>(),
            any::<PostgresTunnel>(),
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

/// Specifies how to tunnel a [`PostgresConnection`].
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum PostgresTunnel {
    /// No tunneling.
    Direct,
    /// Via the specified SSH tunnel connection.
    Ssh {
        connection_id: GlobalId,
        connection: SshConnection,
    },
    /// Via the specified AWS PrivateLink connection.
    AwsPrivateLink { connection_id: GlobalId },
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
