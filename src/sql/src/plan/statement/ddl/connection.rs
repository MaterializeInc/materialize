// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data definition language (DDL) utilities for CONNECTION objects.

use std::collections::{BTreeMap, BTreeSet};

use array_concat::concat_arrays;
use itertools::Itertools;
use mz_ore::str::StrExt;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::ConnectionOptionName::*;
use mz_sql_parser::ast::{
    ConnectionDefaultAwsPrivatelink, ConnectionOption, ConnectionOptionName, CreateConnectionType,
    KafkaBroker, KafkaBrokerAwsPrivatelinkOption, KafkaBrokerAwsPrivatelinkOptionName,
    KafkaBrokerTunnel,
};
use mz_storage_types::connections::aws::{AwsAssumeRole, AwsAuth, AwsConnection, AwsCredentials};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::{
    AwsPrivatelink, AwsPrivatelinkConnection, CsrConnection, CsrConnectionHttpAuth,
    KafkaConnection, KafkaSaslConfig, KafkaTlsConfig, MySqlConnection, MySqlSslMode,
    PostgresConnection, SshConnection, SshTunnel, StringOrSecret, TlsIdentity, Tunnel,
};

use crate::names::Aug;
use crate::plan::statement::{Connection, ResolvedItemName};
use crate::plan::with_options::{self, TryFromValue};
use crate::plan::{PlanError, StatementContext};

generate_extracted_config!(
    ConnectionOption,
    (AccessKeyId, StringOrSecret),
    (AssumeRoleArn, String),
    (AssumeRoleSessionName, String),
    (AvailabilityZones, Vec<String>),
    (AwsPrivatelink, ConnectionDefaultAwsPrivatelink<Aug>),
    // (AwsPrivatelink, with_options::Object),
    (Broker, Vec<KafkaBroker<Aug>>),
    (Brokers, Vec<KafkaBroker<Aug>>),
    (Database, String),
    (Endpoint, String),
    (Host, String),
    (Password, with_options::Secret),
    (Port, u16),
    (ProgressTopic, String),
    (Region, String),
    (SaslMechanisms, String),
    (SaslPassword, with_options::Secret),
    (SaslUsername, StringOrSecret),
    (SecretAccessKey, with_options::Secret),
    (SecurityProtocol, String),
    (ServiceName, String),
    (SshTunnel, with_options::Object),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SslKey, with_options::Secret),
    (SslMode, String),
    (SessionToken, StringOrSecret),
    (Url, String),
    (User, StringOrSecret)
);

generate_extracted_config!(
    KafkaBrokerAwsPrivatelinkOption,
    (AvailabilityZone, String),
    (Port, u16)
);

/// Options which cannot be changed using ALTER CONNECTION.
pub(crate) const INALTERABLE_OPTIONS: &[ConnectionOptionName] = &[ProgressTopic];

/// Options of which only one may be specified.
pub(crate) const MUTUALLY_EXCLUSIVE_SETS: &[&[ConnectionOptionName]] = &[&[Broker, Brokers]];

pub(super) fn validate_options_per_connection_type(
    t: CreateConnectionType,
    mut options: BTreeSet<ConnectionOptionName>,
) -> Result<(), PlanError> {
    use mz_sql_parser::ast::ConnectionOptionName::*;
    let permitted_options = match t {
        CreateConnectionType::Aws => [
            AccessKeyId,
            SecretAccessKey,
            SessionToken,
            Endpoint,
            Region,
            AssumeRoleArn,
            AssumeRoleSessionName,
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
            AwsPrivatelink,
            SshTunnel,
            SslKey,
            SslCertificate,
            SslCertificateAuthority,
            SaslMechanisms,
            SaslUsername,
            SaslPassword,
            SecurityProtocol,
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
        CreateConnectionType::MySql => &[
            AwsPrivatelink,
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
    };

    for o in permitted_options {
        options.remove(o);
    }

    if !options.is_empty() {
        sql_bail!(
            "{} connections do not support {} values",
            t,
            options.iter().join(", ")
        )
    }

    Ok(())
}

impl ConnectionOptionExtracted {
    pub(super) fn ensure_only_valid_options(
        &self,
        t: CreateConnectionType,
    ) -> Result<(), PlanError> {
        validate_options_per_connection_type(t, self.seen.clone())
    }

    pub fn try_into_connection(
        self,
        scx: &StatementContext,
        connection_type: CreateConnectionType,
    ) -> Result<Connection<ReferencedConnection>, PlanError> {
        self.ensure_only_valid_options(connection_type)?;

        let connection: Connection<ReferencedConnection> = match connection_type {
            CreateConnectionType::Aws => {
                let credentials = match (
                    self.access_key_id,
                    self.secret_access_key,
                    self.session_token,
                ) {
                    (Some(access_key_id), Some(secret_access_key), session_token) => {
                        Some(AwsCredentials {
                            access_key_id,
                            secret_access_key: secret_access_key.into(),
                            session_token,
                        })
                    }
                    (None, None, None) => None,
                    _ => {
                        sql_bail!("must specify both ACCESS KEY ID and SECRET ACCESS KEY with optional SESSION TOKEN");
                    }
                };

                let assume_role = match (self.assume_role_arn, self.assume_role_session_name) {
                    (Some(arn), session_name) => Some(AwsAssumeRole { arn, session_name }),
                    (None, Some(_)) => {
                        sql_bail!(
                            "must specify ASSUME ROLE ARN with optional ASSUME ROLE SESSION NAME"
                        );
                    }
                    _ => None,
                };

                let auth = match (credentials, assume_role) {
                    (None, None) => sql_bail!("must specify either ASSUME ROLE ARN or ACCESS KEY ID and SECRET ACCESS KEY"),
                    (Some(credentials), None) => AwsAuth::Credentials(credentials),
                    (None, Some(assume_role)) => AwsAuth::AssumeRole(assume_role),
                    (Some(_), Some(_)) => {
                        sql_bail!("cannot specify both ACCESS KEY ID and ASSUME ROLE ARN");
                    }
                };

                Connection::Aws(AwsConnection {
                    auth,
                    endpoint: match self.endpoint {
                        // TODO(benesch): this should not treat an empty endpoint as equivalent to a `NULL`
                        // endpoint, but making that change now would break testdrive. AWS connections are
                        // all behind feature flags mode right now, so no particular urgency to correct
                        // this.
                        Some(endpoint) if !endpoint.is_empty() => Some(endpoint),
                        _ => None,
                    },
                    region: self.region,
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
            CreateConnectionType::Kafka => {
                if self.ssh_tunnel.is_some() {
                    scx.require_feature_flag(
                        &crate::session::vars::ENABLE_DEFAULT_KAFKA_SSH_TUNNEL,
                    )?;
                }

                if self.aws_privatelink.is_some() {
                    scx.require_feature_flag(
                        &crate::session::vars::ENABLE_DEFAULT_KAFKA_AWS_PRIVATE_LINK,
                    )?;
                }
                let (tls, sasl) = plan_kafka_security(&self)?;

                Connection::Kafka(KafkaConnection {
                    brokers: self.get_brokers(scx)?,
                    default_tunnel: scx
                        .build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?,
                    progress_topic: self.progress_topic,
                    options: BTreeMap::new(),
                    tls,
                    sasl,
                })
            }
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

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/materialize/issues/24712#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!("invalid CONNECTION: PORT in AWS PRIVATELINK is only supported for kafka")
                    }
                }
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

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/materialize/issues/24712#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!("invalid CONNECTION: PORT in AWS PRIVATELINK is only supported for kafka")
                    }
                }
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
            CreateConnectionType::MySql => {
                scx.require_feature_flag(&crate::session::vars::ENABLE_MYSQL_SOURCE)?;

                let cert = self.ssl_certificate;
                let key = self.ssl_key.map(|secret| secret.into());
                let tls_identity = match (cert, key) {
                    (None, None) => None,
                    (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
                    _ => sql_bail!(
                        "invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required"
                    ),
                };
                // Accepts the same SSL Mode values as the MySQL Client
                // https://dev.mysql.com/doc/refman/8.0/en/connection-options.html#option_general_ssl-mode
                let tls_mode = match self
                    .ssl_mode
                    .map(|f| f.to_uppercase())
                    .as_ref()
                    .map(|m| m.as_str())
                {
                    None | Some("DISABLED") => MySqlSslMode::Disabled,
                    // "preferred" intentionally omitted because it has dubious security
                    // properties.
                    Some("REQUIRED") => MySqlSslMode::Required,
                    Some("VERIFY_CA") | Some("VERIFY-CA") => MySqlSslMode::VerifyCa,
                    Some("VERIFY_IDENTITY") | Some("VERIFY-IDENTITY") => {
                        MySqlSslMode::VerifyIdentity
                    }
                    Some(m) => sql_bail!("invalid CONNECTION: unknown SSL MODE {}", m.quoted()),
                };

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/materialize/issues/24712#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!("invalid CONNECTION: PORT in AWS PRIVATELINK is only supported for kafka")
                    }
                }
                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                Connection::MySql(MySqlConnection {
                    password: self.password.map(|password| password.into()),
                    host: self
                        .host
                        .ok_or_else(|| sql_err!("HOST option is required"))?,
                    port: self.port.unwrap_or(3306_u16),
                    tunnel,
                    tls_mode,
                    tls_root_cert: self.ssl_certificate_authority,
                    tls_identity,
                    user: self
                        .user
                        .ok_or_else(|| sql_err!("USER option is required"))?,
                })
            }
        };

        Ok(connection)
    }

    pub fn get_brokers(
        &self,
        scx: &StatementContext,
    ) -> Result<Vec<mz_storage_types::connections::KafkaBroker<ReferencedConnection>>, PlanError>
    {
        let mut brokers = match (&self.broker, &self.brokers, &self.aws_privatelink) {
            (Some(v), None, None) => v.to_vec(),
            (None, Some(v), None) => v.to_vec(),
            (None, None, Some(_)) => vec![],
            (None, None, None) => {
                sql_bail!("invalid CONNECTION: must set one of BROKER, BROKERS, or AWS PRIVATELINK")
            }
            _ => sql_bail!(
                "invalid CONNECTION: can only set one of BROKER, BROKERS, or AWS PRIVATELINK"
            ),
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

            out.push(mz_storage_types::connections::KafkaBroker {
                address: broker.address.clone(),
                tunnel,
            });
        }

        Ok(out)
    }
}

fn plan_kafka_security(
    v: &ConnectionOptionExtracted,
) -> Result<(Option<KafkaTlsConfig>, Option<KafkaSaslConfig>), PlanError> {
    const SASL_CONFIGS: [ConnectionOptionName; 3] = [
        ConnectionOptionName::SaslMechanisms,
        ConnectionOptionName::SaslUsername,
        ConnectionOptionName::SaslPassword,
    ];

    const ALL_CONFIGS: [ConnectionOptionName; 6] = concat_arrays!(
        [
            ConnectionOptionName::SslKey,
            ConnectionOptionName::SslCertificate,
            ConnectionOptionName::SslCertificateAuthority,
        ],
        SASL_CONFIGS
    );

    enum SecurityProtocol {
        Plaintext,
        Ssl,
        SaslPlaintext,
        SaslSsl,
    }

    let security_protocol = v.security_protocol.as_ref().map(|v| v.to_uppercase());
    let security_protocol = match security_protocol.as_deref() {
        Some("PLAINTEXT") => SecurityProtocol::Plaintext,
        Some("SSL") => SecurityProtocol::Ssl,
        Some("SASL_PLAINTEXT") => SecurityProtocol::SaslPlaintext,
        Some("SASL_SSL") => SecurityProtocol::SaslSsl,
        Some(p) => sql_bail!("unknown security protocol: {}", p),
        // To be secure by default, if no security protocol is explicitly
        // specified, we always choose one of the SSL-enabled protocols, using
        // the presence of any SASL options to guide us between them. Users must
        // explicitly choose a plaintext mechanism if that's what they want.
        None if SASL_CONFIGS.iter().any(|c| v.seen.contains(c)) => SecurityProtocol::SaslSsl,
        None => SecurityProtocol::Ssl,
    };

    let mut outstanding = ALL_CONFIGS
        .into_iter()
        .filter(|c| v.seen.contains(c))
        .collect::<BTreeSet<ConnectionOptionName>>();

    let tls = match security_protocol {
        SecurityProtocol::Ssl | SecurityProtocol::SaslSsl => {
            outstanding.remove(&ConnectionOptionName::SslCertificate);
            let identity = match &v.ssl_certificate {
                None => None,
                Some(cert) => {
                    outstanding.remove(&ConnectionOptionName::SslKey);
                    let Some(key) = &v.ssl_key else {
                        sql_bail!("SSL KEY must be specified with SSL CERTIFICATE");
                    };
                    Some(TlsIdentity {
                        cert: cert.clone(),
                        key: (*key).into(),
                    })
                }
            };
            outstanding.remove(&ConnectionOptionName::SslCertificateAuthority);
            Some(KafkaTlsConfig {
                identity,
                root_cert: v.ssl_certificate_authority.clone(),
            })
        }
        _ => None,
    };

    let sasl = match security_protocol {
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl => {
            outstanding.remove(&ConnectionOptionName::SaslMechanisms);
            outstanding.remove(&ConnectionOptionName::SaslUsername);
            outstanding.remove(&ConnectionOptionName::SaslPassword);
            let Some(mechanism) = &v.sasl_mechanisms else {
                // TODO(benesch): support a less confusing `SASL MECHANISM`
                // alias, as only a single mechanism that can be specified.
                sql_bail!("SASL MECHANISMS must be specified");
            };
            let Some(username) = &v.sasl_username else {
                sql_bail!("SASL USERNAME must be specified");
            };
            let Some(password) = &v.sasl_password else {
                sql_bail!("SASL PASSWORD must be specified");
            };
            Some(KafkaSaslConfig {
                // librdkafka requires SASL mechanisms to be upper case (PLAIN,
                // SCRAM-SHA-256). For usability, we automatically uppercase the
                // mechanism that user provides. This avoids a frustrating
                // interaction with identifier case folding. Consider `SASL
                // MECHANISMS = PLAIN`. Identifier case folding results in a
                // SASL mechanism of `plain` (note the lowercase), which
                // Materialize previously rejected with an error of "SASL
                // mechanism must be uppercase." This was deeply frustarting for
                // users who were not familiar with identifier case folding
                // rules. See #22205.
                mechanism: mechanism.to_uppercase(),
                username: username.clone(),
                password: (*password).into(),
            })
        }
        _ => None,
    };

    if let Some(outstanding) = outstanding.first() {
        sql_bail!("option {outstanding} not supported with this configuration");
    }

    Ok((tls, sasl))
}
