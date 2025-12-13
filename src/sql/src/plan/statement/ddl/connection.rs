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

use anyhow::Context;
use array_concat::concat_arrays;
use itertools::Itertools;
use maplit::btreemap;
use mz_ore::num::NonNeg;
use mz_ore::str::StrExt;
use mz_repr::CatalogItemId;
use mz_sql_parser::ast::ConnectionOptionName::*;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ConnectionDefaultAwsPrivatelink, ConnectionOption, ConnectionOptionName, CreateConnectionType,
    KafkaBroker, KafkaBrokerAwsPrivatelinkOption, KafkaBrokerAwsPrivatelinkOptionName,
    KafkaBrokerTunnel,
};
use mz_ssh_util::keys::SshKeyPair;
use mz_storage_types::connections::aws::{
    AwsAssumeRole, AwsAuth, AwsConnection, AwsConnectionReference, AwsCredentials,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::string_or_secret::StringOrSecret;
use mz_storage_types::connections::{
    AwsPrivatelink, AwsPrivatelinkConnection, CsrConnection, CsrConnectionHttpAuth,
    IcebergCatalogConnection, IcebergCatalogImpl, IcebergCatalogType, KafkaConnection,
    KafkaSaslConfig, KafkaTlsConfig, KafkaTopicOptions, MySqlConnection, MySqlSslMode,
    PostgresConnection, RestIcebergCatalog, S3TablesRestIcebergCatalog, SqlServerConnectionDetails,
    SshConnection, SshTunnel, TlsIdentity, Tunnel,
};

use crate::names::Aug;
use crate::plan::statement::{Connection, ResolvedItemName};
use crate::plan::with_options::{self};
use crate::plan::{ConnectionDetails, PlanError, SshKey, StatementContext};
use crate::session::vars::{self, ENABLE_AWS_MSK_IAM_AUTH};

generate_extracted_config!(
    ConnectionOption,
    (AccessKeyId, StringOrSecret),
    (AssumeRoleArn, String),
    (AssumeRoleSessionName, String),
    (AvailabilityZones, Vec<String>),
    (AwsConnection, with_options::Object),
    (AwsPrivatelink, ConnectionDefaultAwsPrivatelink<Aug>),
    // (AwsPrivatelink, with_options::Object),
    (Broker, Vec<KafkaBroker<Aug>>),
    (Brokers, Vec<KafkaBroker<Aug>>),
    (Credential, StringOrSecret),
    (Database, String),
    (Endpoint, String),
    (Host, String),
    (Password, with_options::Secret),
    (Port, u16),
    (ProgressTopic, String),
    (ProgressTopicReplicationFactor, i32),
    (PublicKey1, String),
    (PublicKey2, String),
    (Region, String),
    (SaslMechanisms, String),
    (SaslPassword, with_options::Secret),
    (SaslUsername, StringOrSecret),
    (Scope, String),
    (SecretAccessKey, with_options::Secret),
    (SecurityProtocol, String),
    (ServiceName, String),
    (SshTunnel, with_options::Object),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SslKey, with_options::Secret),
    (SslMode, String),
    (SessionToken, StringOrSecret),
    (CatalogType, IcebergCatalogType),
    (Url, String),
    (User, StringOrSecret),
    (Warehouse, String)
);

generate_extracted_config!(
    KafkaBrokerAwsPrivatelinkOption,
    (AvailabilityZone, String),
    (Port, u16)
);

/// Options which cannot be changed using ALTER CONNECTION.
pub(crate) const INALTERABLE_OPTIONS: &[ConnectionOptionName] =
    &[ProgressTopic, ProgressTopicReplicationFactor];

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
            AwsConnection,
            Broker,
            Brokers,
            ProgressTopic,
            ProgressTopicReplicationFactor,
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
        CreateConnectionType::Ssh => &[Host, Port, User, PublicKey1, PublicKey2],
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
            AwsConnection,
        ],
        CreateConnectionType::SqlServer => &[
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
        CreateConnectionType::IcebergCatalog => &[
            AwsConnection,
            CatalogType,
            Credential,
            Scope,
            Url,
            Warehouse,
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

    pub fn try_into_connection_details(
        self,
        scx: &StatementContext,
        connection_type: CreateConnectionType,
    ) -> Result<ConnectionDetails, PlanError> {
        self.ensure_only_valid_options(connection_type)?;

        let connection: ConnectionDetails = match connection_type {
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
                        sql_bail!(
                            "must specify both ACCESS KEY ID and SECRET ACCESS KEY with optional SESSION TOKEN"
                        );
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
                    (None, None) => sql_bail!(
                        "must specify either ASSUME ROLE ARN or ACCESS KEY ID and SECRET ACCESS KEY"
                    ),
                    (Some(credentials), None) => AwsAuth::Credentials(credentials),
                    (None, Some(assume_role)) => AwsAuth::AssumeRole(assume_role),
                    (Some(_), Some(_)) => {
                        sql_bail!("cannot specify both ACCESS KEY ID and ASSUME ROLE ARN");
                    }
                };

                ConnectionDetails::Aws(AwsConnection {
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
                    let mut unique_azs: BTreeSet<String> = BTreeSet::new();
                    let mut duplicate_azs: BTreeSet<String> = BTreeSet::new();
                    // Validate each AZ is supported
                    for connection_az in &connection.availability_zones {
                        if unique_azs.contains(connection_az) {
                            duplicate_azs.insert(connection_az.to_string());
                        } else {
                            unique_azs.insert(connection_az.to_string());
                        }
                        if !supported_azs.contains(connection_az) {
                            return Err(PlanError::InvalidPrivatelinkAvailabilityZone {
                                name: connection_az.to_string(),
                                supported_azs,
                            });
                        }
                    }
                    if duplicate_azs.len() > 0 {
                        return Err(PlanError::DuplicatePrivatelinkAvailabilityZone {
                            duplicate_azs,
                        });
                    }
                }
                ConnectionDetails::AwsPrivatelink(connection)
            }
            CreateConnectionType::Kafka => {
                let (tls, sasl) = plan_kafka_security(scx, &self)?;

                ConnectionDetails::Kafka(KafkaConnection {
                    brokers: self.get_brokers(scx)?,
                    default_tunnel: scx
                        .build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?,
                    progress_topic: self.progress_topic,
                    progress_topic_options: KafkaTopicOptions {
                        // We only allow configuring the progress topic replication factor for now.
                        // For correctness, the partition count MUST be one and for performance the compaction
                        // policy MUST be enabled.
                        partition_count: Some(NonNeg::try_from(1).expect("1 is positive")),
                        replication_factor: self.progress_topic_replication_factor.map(|val| {
                            if val <= 0 {
                                Err(sql_err!("invalid CONNECTION: PROGRESS TOPIC REPLICATION FACTOR must be greater than 0"))?
                            }
                            NonNeg::try_from(val).map_err(|e| sql_err!("{e}"))
                        }).transpose()?,
                        topic_config: btreemap! {
                            "cleanup.policy".to_string() => "compact".to_string(),
                            "segment.bytes".to_string() => "134217728".to_string(), // 128 MB
                        },
                    },
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

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/database-issues/issues/7359#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!(
                            "invalid CONNECTION: CONFLUENT SCHEMA REGISTRY does not support PORT for AWS PRIVATELINK"
                        )
                    }
                }
                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                ConnectionDetails::Csr(CsrConnection {
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
                    Some("require") | Some("required") => tokio_postgres::config::SslMode::Require,
                    Some("verify_ca") | Some("verify-ca") => {
                        tokio_postgres::config::SslMode::VerifyCa
                    }
                    Some("verify_full") | Some("verify-full") => {
                        tokio_postgres::config::SslMode::VerifyFull
                    }
                    Some(m) => sql_bail!("invalid CONNECTION: unknown SSL MODE {}", m.quoted()),
                };

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/database-issues/issues/7359#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!(
                            "invalid CONNECTION: POSTGRES does not support PORT for AWS PRIVATELINK"
                        )
                    }
                }
                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                ConnectionDetails::Postgres(PostgresConnection {
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
            CreateConnectionType::Ssh => {
                let ensure_key = |public_key| match public_key {
                    Some(public_key) => Ok::<_, anyhow::Error>(SshKey::PublicOnly(public_key)),
                    None => {
                        let key = SshKeyPair::new().context("creating SSH key")?;
                        Ok(SshKey::Both(key))
                    }
                };
                ConnectionDetails::Ssh {
                    connection: SshConnection {
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
                                sql_bail!(
                                    "SSH connections do not support supplying USER value as SECRET"
                                )
                            }
                        },
                    },
                    key_1: ensure_key(self.public_key1)?,
                    key_2: ensure_key(self.public_key2)?,
                }
            }
            CreateConnectionType::MySql => {
                let aws_connection = get_aws_connection_reference(scx, &self)?;
                if aws_connection.is_some() && self.password.is_some() {
                    sql_bail!(
                        "invalid CONNECTION: AWS IAM authentication is not supported with password"
                    );
                }

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
                    None | Some("DISABLED") => {
                        if aws_connection.is_some() {
                            sql_bail!(
                                "invalid CONNECTION: AWS IAM authentication requires SSL to be enabled"
                            )
                        }
                        MySqlSslMode::Disabled
                    }
                    // "preferred" intentionally omitted because it has dubious security
                    // properties.
                    Some("REQUIRED") | Some("REQUIRE") => MySqlSslMode::Required,
                    Some("VERIFY_CA") | Some("VERIFY-CA") => MySqlSslMode::VerifyCa,
                    Some("VERIFY_IDENTITY") | Some("VERIFY-IDENTITY") => {
                        MySqlSslMode::VerifyIdentity
                    }
                    Some(m) => sql_bail!("invalid CONNECTION: unknown SSL MODE {}", m.quoted()),
                };

                // TODO we should move to self.port being unsupported if aws_privatelink is some, see <https://github.com/MaterializeInc/database-issues/issues/7359#issuecomment-1925443977>
                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!(
                            "invalid CONNECTION: MYSQL does not support PORT for AWS PRIVATELINK"
                        )
                    }
                }
                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                ConnectionDetails::MySql(MySqlConnection {
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
                    aws_connection,
                })
            }
            CreateConnectionType::SqlServer => {
                scx.require_feature_flag(&vars::ENABLE_SQL_SERVER_SOURCE)?;

                let aws_connection = get_aws_connection_reference(scx, &self)?;
                if aws_connection.is_some() && self.password.is_some() {
                    sql_bail!(
                        "invalid CONNECTION: AWS IAM authentication is not supported with password"
                    );
                }

                let (encryption, certificate_validation_policy) = match self
                    .ssl_mode
                    .map(|mode| mode.to_uppercase())
                    .as_ref()
                    .map(|mode| mode.as_str())
                {
                    None | Some("DISABLED") => (
                        mz_sql_server_util::config::EncryptionLevel::None,
                        mz_sql_server_util::config::CertificateValidationPolicy::TrustAll,
                    ),
                    Some("REQUIRED") => (
                        mz_sql_server_util::config::EncryptionLevel::Required,
                        mz_sql_server_util::config::CertificateValidationPolicy::TrustAll,
                    ),
                    Some("VERIFY") => (
                        mz_sql_server_util::config::EncryptionLevel::Required,
                        mz_sql_server_util::config::CertificateValidationPolicy::VerifySystem,
                    ),
                    Some("VERIFY_CA") => {
                        if self.ssl_certificate_authority.is_none() {
                            sql_bail!(
                                "invalid CONNECTION: SSL MODE 'verify_ca' requires SSL CERTIFICATE AUTHORITY"
                            );
                        }
                        (
                            mz_sql_server_util::config::EncryptionLevel::Required,
                            mz_sql_server_util::config::CertificateValidationPolicy::VerifyCA,
                        )
                    }
                    Some(mode) => {
                        sql_bail!("invalid CONNECTION: unknown SSL MODE {}", mode.quoted())
                    }
                };

                if let Some(privatelink) = self.aws_privatelink.as_ref() {
                    if privatelink.port.is_some() {
                        sql_bail!(
                            "invalid CONNECTION: SQL SERVER does not support PORT for AWS PRIVATELINK"
                        )
                    }
                }

                // 1433 is the default port for SQL Server instances running over TCP.
                //
                // See: <https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-a-server-to-listen-on-a-specific-tcp-port?view=sql-server-ver16>
                let port = self.port.unwrap_or(1433_u16);
                let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

                ConnectionDetails::SqlServer(SqlServerConnectionDetails {
                    host: self
                        .host
                        .ok_or_else(|| sql_err!("HOST option is required"))?,
                    port,
                    database: self
                        .database
                        .ok_or_else(|| sql_err!("DATABASE option is required"))?,
                    user: self
                        .user
                        .ok_or_else(|| sql_err!("USER option is required"))?,
                    password: self
                        .password
                        .ok_or_else(|| sql_err!("PASSWORD option is required"))
                        .map(|pass| pass.into())?,
                    tunnel,
                    encryption,
                    certificate_validation_policy,
                    tls_root_cert: self.ssl_certificate_authority,
                })
            }
            CreateConnectionType::IcebergCatalog => {
                let catalog_type = self.catalog_type.clone().ok_or_else(|| {
                    sql_err!("invalid CONNECTION: ICEBERG connections must specify CATALOG TYPE")
                })?;

                let uri: reqwest::Url = match &self.url {
                    Some(url) => url
                        .parse()
                        .map_err(|e| sql_err!("parsing Iceberg catalog url: {e}"))?,
                    None => sql_bail!("invalid CONNECTION: must specify URL"),
                };

                let warehouse = self.warehouse.clone();
                let credential = self.credential.clone();
                let aws_connection = get_aws_connection_reference(scx, &self)?;

                let catalog = match catalog_type {
                    IcebergCatalogType::S3TablesRest => {
                        let Some(warehouse) = warehouse else {
                            sql_bail!(
                                "invalid CONNECTION: ICEBERG s3tablesrest connections must specify WAREHOUSE"
                            );
                        };
                        let Some(aws_connection) = aws_connection else {
                            sql_bail!(
                                "invalid CONNECTION: ICEBERG s3tablesrest connections require an AWS connection"
                            );
                        };

                        IcebergCatalogImpl::S3TablesRest(S3TablesRestIcebergCatalog {
                            aws_connection,
                            warehouse,
                        })
                    }
                    IcebergCatalogType::Rest => {
                        let Some(credential) = credential else {
                            sql_bail!(
                                "invalid CONNECTION: ICEBERG rest connections require a CREDENTIAL"
                            );
                        };

                        IcebergCatalogImpl::Rest(RestIcebergCatalog {
                            credential,
                            scope: self.scope.clone(),
                            warehouse,
                        })
                    }
                };

                ConnectionDetails::IcebergCatalog(IcebergCatalogConnection { catalog, uri })
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
                                    sql_bail!(
                                        "AWS PrivateLink availability zone {} does not match any of the \
                                      availability zones on the AWS PrivateLink connection {}",
                                        az.quoted(),
                                        scx.catalog
                                            .resolve_full_name(entry.name())
                                            .to_string()
                                            .quoted()
                                    )
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

fn get_aws_connection_reference(
    scx: &StatementContext,
    conn_options: &ConnectionOptionExtracted,
) -> Result<Option<AwsConnectionReference<ReferencedConnection>>, PlanError> {
    let Some(aws_connection_id) = conn_options.aws_connection else {
        return Ok(None);
    };

    let id = CatalogItemId::from(aws_connection_id);
    let item = scx.catalog.get_item(&id);
    Ok(match item.connection()? {
        Connection::Aws(_) => Some(AwsConnectionReference {
            connection_id: id,
            connection: id,
        }),
        _ => sql_bail!("{} is not an AWS connection", item.name().item),
    })
}

fn plan_kafka_security(
    scx: &StatementContext,
    v: &ConnectionOptionExtracted,
) -> Result<
    (
        Option<KafkaTlsConfig>,
        Option<KafkaSaslConfig<ReferencedConnection>>,
    ),
    PlanError,
> {
    const SASL_CONFIGS: [ConnectionOptionName; 4] = [
        ConnectionOptionName::AwsConnection,
        ConnectionOptionName::SaslMechanisms,
        ConnectionOptionName::SaslUsername,
        ConnectionOptionName::SaslPassword,
    ];

    const ALL_CONFIGS: [ConnectionOptionName; 7] = concat_arrays!(
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
            outstanding.remove(&ConnectionOptionName::AwsConnection);
            match get_aws_connection_reference(scx, v)? {
                Some(aws) => {
                    scx.require_feature_flag(&ENABLE_AWS_MSK_IAM_AUTH)?;
                    Some(KafkaSaslConfig {
                        mechanism: "OAUTHBEARER".into(),
                        username: "".into(),
                        password: None,
                        aws: Some(aws),
                    })
                }
                None => {
                    outstanding.remove(&ConnectionOptionName::SaslMechanisms);
                    outstanding.remove(&ConnectionOptionName::SaslUsername);
                    outstanding.remove(&ConnectionOptionName::SaslPassword);
                    // TODO(benesch): support a less confusing `SASL MECHANISM`
                    // alias, as only a single mechanism that can be specified.
                    let Some(mechanism) = &v.sasl_mechanisms else {
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
                        // rules. See database-issues#6693.
                        mechanism: mechanism.to_uppercase(),
                        username: username.clone(),
                        password: Some((*password).into()),
                        aws: None,
                    })
                }
            }
        }
        _ => None,
    };

    if let Some(outstanding) = outstanding.first() {
        sql_bail!("option {outstanding} not supported with this configuration");
    }

    Ok((tls, sasl))
}
