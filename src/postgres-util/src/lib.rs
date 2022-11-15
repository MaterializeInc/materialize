// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PostgreSQL utility library.

use std::time::Duration;

use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{Host, ReplicationMode, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::Client;

use mz_ore::task;
use mz_repr::GlobalId;
use mz_ssh_util::tunnel::SshTunnelConfig;

use crate::desc::{PostgresColumnDesc, PostgresTableDesc};

pub mod desc;

/// An error representing pg, ssh, ssl, and other failures.
#[derive(Debug, thiserror::Error)]
pub enum PostgresError {
    /// Any other error we bail on.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// Error using ssh.
    #[error(transparent)]
    Ssh(#[from] openssh::Error),
    /// Error doing io to setup an ssh connection.
    #[error(transparent)]
    SshIo(#[from] std::io::Error),
    /// A postgres error.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    /// Error setting up postgres ssl.
    #[error(transparent)]
    PostgresSsl(#[from] openssl::error::ErrorStack),
}

macro_rules! bail_generic {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($fmt, $($arg)*)))
    };
    ($err:expr $(,)?) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($err)))
    };
}

/// Creates a TLS connector for the given [`Config`].
pub fn make_tls(config: &tokio_postgres::Config) -> Result<MakeTlsConnector, PostgresError> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate(&*X509::from_pem(ssl_cert)?)?;
            builder.set_private_key(&*PKey::private_key_from_pem(ssl_key)?)?;
        }
        (None, Some(_)) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslkey")
        }
        (Some(_), None) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslcert")
        }
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder
            .cert_store_mut()
            .add_cert(X509::from_pem(ssl_root_cert)?)?;
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
}

/// Fetches table schema information from an upstream Postgres source for all tables that are part
/// of a publication, given a connection string and the publication name.
///
/// # Errors
///
/// - Invalid connection string, user information, or user permissions.
/// - Upstream publication does not exist or contains invalid values.
pub async fn publication_info(
    config: &Config,
    publication: &str,
) -> Result<Vec<PostgresTableDesc>, PostgresError> {
    let client = config.connect("postgres_publication_info").await?;

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow::anyhow!("publication {:?} does not exist", publication))?;

    let tables = client
        .query(
            "SELECT
                c.oid, p.schemaname, p.tablename
            FROM
                pg_catalog.pg_class AS c
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_publication_tables AS p ON
                        c.relname = p.tablename AND n.nspname = p.schemaname
            WHERE
                p.pubname = $1",
            &[&publication],
        )
        .await?;

    let mut table_infos = vec![];
    for row in tables {
        let oid = row.get("oid");

        let columns = client
            .query(
                "SELECT
                        a.attname AS name,
                        a.atttypid AS typoid,
                        a.atttypmod AS typmod,
                        a.attnotnull AS not_null,
                        b.oid IS NOT NULL AS primary_key
                    FROM pg_catalog.pg_attribute a
                    LEFT JOIN pg_catalog.pg_constraint b
                        ON a.attrelid = b.conrelid
                        AND b.contype = 'p'
                        AND a.attnum = ANY (b.conkey)
                    WHERE a.attnum > 0::pg_catalog.int2
                        AND NOT a.attisdropped
                        AND a.attrelid = $1
                    ORDER BY a.attnum",
                &[&oid],
            )
            .await?
            .into_iter()
            .map(|row| {
                let name: String = row.get("name");
                let type_oid = row.get("typoid");
                let type_mod: i32 = row.get("typmod");
                let not_null: bool = row.get("not_null");
                let primary_key = row.get("primary_key");
                Ok(PostgresColumnDesc {
                    name,
                    type_oid,
                    type_mod,
                    nullable: !not_null,
                    primary_key,
                })
            })
            .collect::<Result<Vec<_>, PostgresError>>()?;

        table_infos.push(PostgresTableDesc {
            oid,
            namespace: row.get("schemaname"),
            name: row.get("tablename"),
            columns,
        });
    }

    Ok(table_infos)
}

pub async fn drop_replication_slots(config: Config, slots: &[&str]) -> Result<(), PostgresError> {
    let client = config.connect("postgres_drop_replication_slots").await?;
    let replication_client = config.connect_replication().await?;
    for slot in slots {
        let rows = client
            .query(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
                &[&slot],
            )
            .await?;
        match rows.len() {
            0 => {
                // DROP_REPLICATION_SLOT will error if the slot does not exist
                // todo@jldlaughlin: don't let invalid Postgres sources ship!
                continue;
            }
            1 => {
                replication_client
                    .simple_query(&format!("DROP_REPLICATION_SLOT {} WAIT", slot))
                    .await?;
            }
            _ => {
                return Err(PostgresError::Generic(anyhow::anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                )))
            }
        }
    }
    Ok(())
}

/// Configures an optional tunnel for use when connecting to a PostgreSQL
/// database.
#[derive(Debug, PartialEq, Clone)]
pub enum TunnelConfig {
    /// Establish a direct TCP connection to the database host.
    Direct,
    /// Establish a TCP connection to the database via an SSH tunnel.
    /// This means first establishing an SSH connection to a bastion host,
    /// and then opening a separate connection from that host to the database.
    /// This is commonly referred by vendors as a "direct SSH tunnel", in
    /// opposition to "reverse SSH tunnel", which is currently unsupported.
    Ssh(SshTunnelConfig),
    /// Establish a TCP connection to the database via an AWS PrivateLink
    /// service.
    AwsPrivatelink {
        /// The ID of the AWS PrivateLink service.
        connection_id: GlobalId,
    },
}

/// Configuration for PostgreSQL connections.
///
/// This wraps [`tokio_postgres::Config`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Debug, PartialEq, Clone)]
pub struct Config {
    inner: tokio_postgres::Config,
    tunnel: TunnelConfig,
}

impl Config {
    pub fn new(inner: tokio_postgres::Config, tunnel: TunnelConfig) -> Result<Self, PostgresError> {
        let config = Self { inner, tunnel };

        // Early validate that the configuration contains only a single TCP
        // server.
        config.address()?;

        Ok(config)
    }

    /// Connects to the configured PostgreSQL database.
    pub async fn connect(&self, task_name: &str) -> Result<Client, PostgresError> {
        self.connect_internal(task_name, |_| ()).await
    }

    /// Starts a replication connection to the configured PostgreSQL database.
    pub async fn connect_replication(&self) -> Result<Client, PostgresError> {
        self.connect_internal("postgres_connect_replication", |config| {
            config
                .replication_mode(ReplicationMode::Logical)
                .connect_timeout(Duration::from_secs(30))
                .keepalives_idle(Duration::from_secs(10 * 60));
        })
        .await
    }

    fn address(&self) -> Result<(&str, u16), PostgresError> {
        match (self.inner.get_hosts(), self.inner.get_ports()) {
            ([Host::Tcp(host)], [port]) => Ok((host, *port)),
            _ => bail_generic!("only TCP connections to a single PostgreSQL server are supported"),
        }
    }

    async fn connect_internal<F>(
        &self,
        task_name: &str,
        configure: F,
    ) -> Result<Client, PostgresError>
    where
        F: FnOnce(&mut tokio_postgres::Config),
    {
        let mut postgres_config = self.inner.clone();
        configure(&mut postgres_config);
        let mut tls = make_tls(&postgres_config)?;
        match &self.tunnel {
            TunnelConfig::Direct => {
                let (client, connection) = postgres_config.connect(tls).await?;
                task::spawn(|| task_name, connection);
                Ok(client)
            }
            TunnelConfig::Ssh(tunnel) => {
                let (host, port) = self.address()?;
                let (session, local_port) = tunnel.connect(host, port).await?;
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect(("localhost", local_port)).await?;
                let (client, connection) = postgres_config.connect_raw(tcp_stream, tls).await?;
                task::spawn(|| task_name, async {
                    _ = connection
                        .await
                        .err()
                        .map(|e| tracing::error!("Postgres connection failed: {e}"));
                    _ = session
                        .close()
                        .await
                        .err()
                        .map(|e| tracing::error!("failed to close ssh tunnel: {e}"));
                });
                Ok(client)
            }
            TunnelConfig::AwsPrivatelink { connection_id } => {
                let (host, port) = self.address()?;
                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect((privatelink_host, port)).await?;
                let (client, connection) = postgres_config.connect_raw(tcp_stream, tls).await?;
                task::spawn(|| task_name, connection);
                Ok(client)
            }
        }
    }
}
