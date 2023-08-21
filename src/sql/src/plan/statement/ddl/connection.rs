// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data definition language (DDL) utilities for CONNECTION objects.

use std::collections::BTreeMap;

use itertools::Itertools;
use mz_ore::str::StrExt;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ConnectionOption, ConnectionOptionName, CreateConnectionType, KafkaBroker,
    KafkaBrokerAwsPrivatelinkOption, KafkaBrokerAwsPrivatelinkOptionName, KafkaBrokerTunnel,
};
use mz_storage_types::connections::aws::{AwsAssumeRole, AwsConfig, AwsCredentials};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::{
    AwsPrivatelink, AwsPrivatelinkConnection, CsrConnection, CsrConnectionHttpAuth,
    KafkaBroker as StorageClientKafkaBroker, KafkaConnection, KafkaSecurity, KafkaTlsConfig,
    PostgresConnection, SaslConfig, SshConnection, SshTunnel, StringOrSecret, TlsIdentity, Tunnel,
};

use crate::names::Aug;
use crate::plan::statement::{Connection, ResolvedItemName};
use crate::plan::with_options::{self, TryFromValue};
use crate::plan::{PlanError, StatementContext};

const SSL_CONFIG: [ConnectionOptionName; 2] = [
    ConnectionOptionName::SslKey,
    ConnectionOptionName::SslCertificate,
];

const SASL_CONFIG: [ConnectionOptionName; 3] = [
    ConnectionOptionName::SaslMechanisms,
    ConnectionOptionName::SaslUsername,
    ConnectionOptionName::SaslPassword,
];

generate_extracted_config!(
    ConnectionOption,
    (AccessKeyId, StringOrSecret),
    (AvailabilityZones, Vec<String>),
    (AwsPrivatelink, with_options::Object),
    (Broker, Vec<KafkaBroker<Aug>>),
    (Brokers, Vec<KafkaBroker<Aug>>),
    (Database, String),
    (Endpoint, String),
    (Host, String),
    (Password, with_options::Secret),
    (Port, u16),
    (ProgressTopic, String),
    (Region, String),
    (RoleArn, String),
    (SaslMechanisms, String),
    (SaslPassword, with_options::Secret),
    (SaslUsername, StringOrSecret),
    (SecretAccessKey, with_options::Secret),
    (ServiceName, String),
    (SshTunnel, with_options::Object),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SslKey, with_options::Secret),
    (SslMode, String),
    (Token, StringOrSecret),
    (Url, String),
    (User, StringOrSecret)
);

generate_extracted_config!(
    KafkaBrokerAwsPrivatelinkOption,
    (AvailabilityZone, String),
    (Port, u16)
);

impl ConnectionOptionExtracted {
    pub(super) fn ensure_only_valid_options(
        &self,
        t: CreateConnectionType,
    ) -> Result<(), PlanError> {
        use mz_sql_parser::ast::ConnectionOptionName::*;
        let mut seen = self.seen.clone();

        let permitted_options = match t {
            CreateConnectionType::Aws => [
                AccessKeyId,
                SecretAccessKey,
                Token,
                Endpoint,
                Region,
                RoleArn,
            ]
            .as_slice(),
            CreateConnectionType::AwsPrivatelink => &[AvailabilityZones, Port, ServiceName],
            CreateConnectionType::Csr => &[
                AwsPrivatelink,
                Password,
                Port,
                SshTunnel,
                SslCertificate,
                SslCertificateAuthority,
                SslKey,
                Url,
                User,
            ],
            CreateConnectionType::Kafka => &[
                Broker,
                Brokers,
                ProgressTopic,
                SshTunnel,
                SslKey,
                SslCertificate,
                SslCertificateAuthority,
                SaslMechanisms,
                SaslUsername,
                SaslPassword,
            ],
            CreateConnectionType::Postgres => &[
                AwsPrivatelink,
                Database,
                Host,
                Password,
                Port,
                SshTunnel,
                SslCertificate,
                SslCertificateAuthority,
                SslKey,
                SslMode,
                User,
            ],
            CreateConnectionType::Ssh => &[Host, Port, User],
        };

        for o in permitted_options {
            seen.remove(o);
        }

        if !seen.is_empty() {
            sql_bail!(
                "{} connections do not support {} values",
                t,
                seen.iter().join(", ")
            )
        }

        Ok(())
    }

    pub fn try_into_connection(
        self,
        scx: &StatementContext,
        connection_type: CreateConnectionType,
    ) -> Result<Connection<ReferencedConnection>, PlanError> {
        self.ensure_only_valid_options(connection_type)?;

        let connection: Connection<ReferencedConnection> = match connection_type {
            CreateConnectionType::Aws => {
                Connection::Aws(AwsConfig {
                    credentials: AwsCredentials {
                        access_key_id: self
                            .access_key_id
                            .ok_or_else(|| sql_err!("ACCESS KEY ID option is required"))?,
                        secret_access_key: self
                            .secret_access_key
                            .ok_or_else(|| sql_err!("SECRET ACCESS KEY option is required"))?
                            .into(),
                        session_token: self.token,
                    },
                    endpoint: match self.endpoint {
                        // TODO(benesch): this should not treat an empty endpoint as equivalent to a `NULL`
                        // endpoint, but making that change now would break testdrive. AWS connections are
                        // all behind feature flags mode right now, so no particular urgency to correct
                        // this.
                        Some(endpoint) if !endpoint.is_empty() => Some(endpoint),
                        _ => None,
                    },
                    region: self.region,
                    role: self.role_arn.map(|arn| AwsAssumeRole { arn }),
                })
            }
            CreateConnectionType::AwsPrivatelink => {
                let connection = AwsPrivatelinkConnection {
                    service_name: self
                        .service_name
                        .ok_or_else(|| sql_err!("SERVICE NAME option is required"))?,
                    availability_zones: self
                        .availability_zones
                        .ok_or_else(|| sql_err!("AVAILABILITY ZONES option is required"))?,
                };

                if let Some(supported_azs) = scx.catalog.aws_privatelink_availability_zones() {
                    for connection_az in &connection.availability_zones {
                        if !supported_azs.contains(connection_az) {
                            return Err(PlanError::InvalidPrivatelinkAvailabilityZone {
                                name: connection_az.to_string(),
                                supported_azs,
                            });
                        }
                    }
                }
                Connection::AwsPrivatelink(connection)
            }
            CreateConnectionType::Kafka => Connection::Kafka(KafkaConnection {
                brokers: self.get_brokers(scx)?,
                security: Option::<KafkaSecurity>::try_from(&self)?,
                progress_topic: self.progress_topic,
                options: BTreeMap::new(),
            }),
            CreateConnectionType::Csr => {
                let url: reqwest::Url = match self.url {
                    Some(url) => url
                        .parse()
                        .map_err(|e| sql_err!("parsing schema registry url: {e}"))?,
                    None => sql_bail!("invalid CONNECTION: must specify URL"),
                };
                let _ = url
                    .host_str()
                    .ok_or_else(|| sql_err!("invalid CONNECTION: URL must specify domain name"))?;
                if url.path() != "/" {
                    sql_bail!("invalid CONNECTION: URL must have an empty path");
                }
                let cert = self.ssl_certificate;
                let key = self.ssl_key.map(|secret| secret.into());
                let tls_identity = match (cert, key) {
                    (None, None) => None,
                    (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
                    _ => sql_bail!(
                        "invalid CONNECTION: reading from SSL-auth Confluent Schema Registry requires both SSL KEY and SSL CERTIFICATE"
                    ),
                };
                let http_auth = self.user.map(|username| CsrConnectionHttpAuth {
                    username,
                    password: self.password.map(|secret| secret.into()),
                });

                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                Connection::Csr(CsrConnection {
                    url,
                    tls_root_cert: self.ssl_certificate_authority,
                    tls_identity,
                    http_auth,
                    tunnel,
                })
            }
            CreateConnectionType::Postgres => {
                let cert = self.ssl_certificate;
                let key = self.ssl_key.map(|secret| secret.into());
                let tls_identity = match (cert, key) {
                    (None, None) => None,
                    (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
                    _ => sql_bail!(
                        "invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required"
                    ),
                };
                let tls_mode = match self.ssl_mode.as_ref().map(|m| m.as_str()) {
                    None | Some("disable") => tokio_postgres::config::SslMode::Disable,
                    // "prefer" intentionally omitted because it has dubious security
                    // properties.
                    Some("require") => tokio_postgres::config::SslMode::Require,
                    Some("verify_ca") | Some("verify-ca") => {
                        tokio_postgres::config::SslMode::VerifyCa
                    }
                    Some("verify_full") | Some("verify-full") => {
                        tokio_postgres::config::SslMode::VerifyFull
                    }
                    Some(m) => sql_bail!("invalid CONNECTION: unknown SSL MODE {}", m.quoted()),
                };

                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                Connection::Postgres(PostgresConnection {
                    database: self
                        .database
                        .ok_or_else(|| sql_err!("DATABASE option is required"))?,
                    password: self.password.map(|password| password.into()),
                    host: self
                        .host
                        .ok_or_else(|| sql_err!("HOST option is required"))?,
                    port: self.port.unwrap_or(5432_u16),
                    tunnel,
                    tls_mode,
                    tls_root_cert: self.ssl_certificate_authority,
                    tls_identity,
                    user: self
                        .user
                        .ok_or_else(|| sql_err!("USER option is required"))?,
                })
            }
            CreateConnectionType::Ssh => Connection::Ssh(SshConnection {
                host: self
                    .host
                    .ok_or_else(|| sql_err!("HOST option is required"))?,
                port: self.port.unwrap_or(22_u16),
                user: match self
                    .user
                    .ok_or_else(|| sql_err!("USER option is required"))?
                {
                    StringOrSecret::String(user) => user,
                    StringOrSecret::Secret(_) => {
                        sql_bail!("SSH connections do not support supplying USER value as SECRET")
                    }
                },
                public_keys: None,
            }),
        };

        Ok(connection)
    }

    pub fn get_brokers(
        &self,
        scx: &StatementContext,
    ) -> Result<Vec<StorageClientKafkaBroker<ReferencedConnection>>, PlanError> {
        let mut brokers = match (&self.broker, &self.brokers) {
            (Some(_), Some(_)) => sql_bail!("invalid CONNECTION: cannot set BROKER and BROKERS"),
            (None, None) => sql_bail!("invalid CONNECTION: must set either BROKER or BROKERS"),
            (Some(v), None) => v.to_vec(),
            (None, Some(v)) => v.to_vec(),
        };

        // NOTE: we allow broker configurations to be mixed and matched. If/when we support
        // a top-level `SSH TUNNEL` configuration, we will need additional assertions.

        let mut out = vec![];
        for broker in &mut brokers {
            if broker.address.contains(',') {
                sql_bail!("invalid CONNECTION: cannot specify multiple Kafka broker addresses in one string.\n\n
Instead, specify BROKERS using multiple strings, e.g. BROKERS ('kafka:9092', 'kafka:9093')");
            }

            let tunnel = match &broker.tunnel {
                KafkaBrokerTunnel::Direct => Tunnel::Direct,
                KafkaBrokerTunnel::AwsPrivatelink(aws_privatelink) => {
                    let KafkaBrokerAwsPrivatelinkOptionExtracted {
                        availability_zone,
                        port,
                        seen: _,
                    } = KafkaBrokerAwsPrivatelinkOptionExtracted::try_from(
                        aws_privatelink.options.clone(),
                    )?;

                    let id = match &aws_privatelink.connection {
                        ResolvedItemName::Item { id, .. } => id,
                        _ => sql_bail!(
                            "internal error: Kafka PrivateLink connection was not resolved"
                        ),
                    };
                    let entry = scx.catalog.get_item(id);
                    match entry.connection()? {
                        Connection::AwsPrivatelink(connection) => {
                            if let Some(az) = &availability_zone {
                                if !connection.availability_zones.contains(az) {
                                    sql_bail!("AWS PrivateLink availability zone {} does not match any of the \
                                      availability zones on the AWS PrivateLink connection {}",
                                      az.quoted(),
                                        scx.catalog.resolve_full_name(entry.name()).to_string().quoted())
                                }
                            }
                            Tunnel::AwsPrivatelink(AwsPrivatelink {
                                connection_id: *id,
                                availability_zone,
                                port,
                            })
                        }
                        _ => {
                            sql_bail!("{} is not an AWS PRIVATELINK connection", entry.name().item)
                        }
                    }
                }
                KafkaBrokerTunnel::SshTunnel(ssh) => {
                    let id = match &ssh {
                        ResolvedItemName::Item { id, .. } => id,
                        _ => sql_bail!(
                            "internal error: Kafka SSH tunnel connection was not resolved"
                        ),
                    };
                    let ssh_tunnel = scx.catalog.get_item(id);
                    match ssh_tunnel.connection()? {
                        Connection::Ssh(_connection) => Tunnel::Ssh(SshTunnel {
                            connection_id: *id,
                            connection: *id,
                        }),
                        _ => {
                            sql_bail!("{} is not an SSH connection", ssh_tunnel.name().item)
                        }
                    }
                }
            };

            out.push(StorageClientKafkaBroker {
                address: broker.address.clone(),
                tunnel,
            });
        }

        Ok(out)
    }
}

impl From<&ConnectionOptionExtracted> for Option<KafkaTlsConfig> {
    fn from(k: &ConnectionOptionExtracted) -> Self {
        if SSL_CONFIG.iter().all(|config| k.seen.contains(config)) {
            Some(KafkaTlsConfig {
                identity: Some(TlsIdentity {
                    key: k.ssl_key.unwrap().into(),
                    cert: k.ssl_certificate.clone().unwrap(),
                }),
                root_cert: k.ssl_certificate_authority.clone(),
            })
        } else {
            None
        }
    }
}

impl TryFrom<&ConnectionOptionExtracted> for Option<SaslConfig> {
    type Error = PlanError;
    fn try_from(k: &ConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let res = if SASL_CONFIG.iter().all(|config| k.seen.contains(config)) {
            let sasl_mechanism = k.sasl_mechanisms.clone().unwrap();
            if sasl_mechanism
                .chars()
                .any(|c| c.is_ascii_alphabetic() && !c.is_uppercase())
            {
                sql_bail!(
                    "invalid SASL MECHANISM {}: must be uppercase",
                    sasl_mechanism.quoted()
                );
            }
            Some(SaslConfig {
                mechanisms: sasl_mechanism,
                username: k.sasl_username.clone().unwrap(),
                password: k.sasl_password.unwrap().into(),
                tls_root_cert: k.ssl_certificate_authority.clone(),
            })
        } else {
            None
        };
        Ok(res)
    }
}

impl TryFrom<&ConnectionOptionExtracted> for Option<KafkaSecurity> {
    type Error = PlanError;
    fn try_from(value: &ConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let ssl_config = Option::<KafkaTlsConfig>::from(value).map(KafkaSecurity::from);
        let sasl_config = Option::<SaslConfig>::try_from(value)?.map(KafkaSecurity::from);

        let mut security_iter = vec![ssl_config, sasl_config].into_iter();
        let res = match security_iter.find(|v| v.is_some()) {
            Some(config) => {
                if security_iter.find(|v| v.is_some()).is_some() {
                    sql_bail!("invalid CONNECTION: cannot specify multiple security protocols");
                }
                config
            }
            None => None,
        };

        if res.is_none()
            && SASL_CONFIG
                .iter()
                .chain(SSL_CONFIG.iter())
                .any(|c| value.seen.contains(c))
        {
            sql_bail!("invalid CONNECTION: under-specified security configuration");
        }

        Ok(res)
    }
}
