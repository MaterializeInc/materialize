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

use anyhow::{anyhow, bail};
use async_ssh2_lite::{AsyncChannel, AsyncSession, SessionConfiguration};
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{ReplicationMode, SslMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config as PostgresConfig};

use mz_ore::task;

use crate::desc::{PostgresColumnDesc, PostgresTableDesc};

pub mod desc;

/// Creates a TLS connector for the given [`Config`].
pub fn make_tls(config: &PostgresConfig) -> Result<MakeTlsConnector, anyhow::Error> {
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
        (None, Some(_)) => bail!("must provide both sslcert and sslkey, but only provided sslkey"),
        (Some(_), None) => bail!("must provide both sslcert and sslkey, but only provided sslcert"),
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
) -> Result<Vec<PostgresTableDesc>, anyhow::Error> {
    let client = config.connect("postgres_publication_info").await?;

    client
        .query(
            "SELECT oid FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .get(0)
        .ok_or_else(|| anyhow!("publication {:?} does not exist", publication))?;

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
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        table_infos.push(PostgresTableDesc {
            oid,
            namespace: row.get("schemaname"),
            name: row.get("tablename"),
            columns,
        });
    }

    Ok(table_infos)
}

pub async fn drop_replication_slots(config: Config, slots: &[&str]) -> Result<(), anyhow::Error> {
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
                return Err(anyhow!(
                    "multiple pg_replication_slots entries for slot {}",
                    &slot
                ))
            }
        }
    }
    Ok(())
}

/// Configuration on how to connect a given Postgres database.
#[derive(PartialEq, Clone)]
pub enum SshTunnelConfig {
    /// Establish a direct TCP connection to the database host.
    Direct,
    /// Establish a TCP connection to the database via an SSH tunnel.
    /// This means first establishing an SSH connection to a bastion host,
    /// and then opening a separate connection from that host to the database.
    /// This is commonly referred by vendors as a "direct SSH tunnel", in
    /// opposition to "reverse SSH tunnel", which is currently unsupported.
    Tunnel {
        /// Hostname of the SSH bastion host
        host: String,
        /// Port where `sshd` is running in the bastion host
        port: u16,
        /// Username to be used in the SSH connection
        user: String,
        /// Public SSH key used for authentication, in OpenSSH format.
        /// Stored as a string (instead of `Vec<u8>`) because that is how
        /// the SSH library expects it.
        public_key: String,
        /// Private SSH key used for authentication, in OpenSSH format.
        /// Stored as a string (instead of `Vec<u8>`) because that is how
        /// the SSH library expects it.
        private_key: String,
    },
}

// Omit keys from debug output
impl std::fmt::Debug for SshTunnelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct => write!(f, "Direct"),
            Self::Tunnel {
                host, port, user, ..
            } => f
                .debug_struct("Tunnel")
                .field("host", host)
                .field("port", port)
                .field("user", user)
                .finish(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Config {
    postgres_config: PostgresConfig,
    host: String,
    port: u16,
    ssh_tunnel: SshTunnelConfig,
}

impl Config {
    pub fn new(
        postgres_config: PostgresConfig,
        host: &str,
        port: u16,
        ssh_tunnel: SshTunnelConfig,
    ) -> Self {
        Self {
            postgres_config,
            host: host.to_string(),
            port,
            ssh_tunnel,
        }
    }

    /// Connect to a Postgres database, automatically managing SSL and SSH details as needed.
    pub async fn connect(&self, connection_task_name: &str) -> Result<Client, anyhow::Error> {
        let mut tls = make_tls(&self.postgres_config)?;
        match self.ssh_tunnel {
            SshTunnelConfig::Direct => {
                let (client, connection) = self.postgres_config.connect(tls).await?;
                task::spawn(|| connection_task_name, connection);
                Ok(client)
            }
            SshTunnelConfig::Tunnel { .. } => {
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, &self.host)?;
                let channel = self.establish_ssh_connection().await?;
                let (client, connection) = self.postgres_config.connect_raw(channel, tls).await?;
                task::spawn(|| connection_task_name, connection);
                Ok(client)
            }
        }
    }

    /// Starts a replication connection to the upstream database
    pub async fn connect_replication(mut self) -> Result<Client, anyhow::Error> {
        let mut tls = make_tls(&self.postgres_config)?;
        match self.ssh_tunnel {
            SshTunnelConfig::Direct => {
                let (client, connection) = self
                    .postgres_config
                    .replication_mode(ReplicationMode::Logical)
                    .connect_timeout(Duration::from_secs(30))
                    .keepalives_idle(Duration::from_secs(10 * 60))
                    .connect(tls)
                    .await?;
                task::spawn(|| "postgres_connect_replication", connection);
                Ok(client)
            }
            SshTunnelConfig::Tunnel { .. } => {
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, &self.host)?;
                let channel = self.establish_ssh_connection().await?;
                let (client, connection) = self
                    .postgres_config
                    .replication_mode(ReplicationMode::Logical)
                    .connect_timeout(Duration::from_secs(30))
                    .keepalives_idle(Duration::from_secs(10 * 60))
                    .connect_raw(channel, tls)
                    .await?;
                task::spawn(|| "postgres_connect_replication", connection);
                Ok(client)
            }
        }
    }

    async fn establish_ssh_connection(
        &self,
    ) -> Result<AsyncChannel<TokioTcpStream>, anyhow::Error> {
        match &self.ssh_tunnel {
            SshTunnelConfig::Tunnel {
                host,
                port,
                user,
                public_key,
                private_key,
            } => {
                let tcp_stream = TokioTcpStream::connect((host.as_str(), *port)).await?;
                let mut session = AsyncSession::new(tcp_stream, Some(SessionConfiguration::new()))?;
                session.handshake().await?;

                session
                    .userauth_pubkey_memory(user, Some(public_key), private_key, None)
                    .await?;

                if !session.authenticated() {
                    bail!("failed SSH authentication")
                };

                // Advertise the source connection to Postgres as coming from the bastion host
                let src = Some((host.as_str(), *port));

                Ok(session
                    .channel_direct_tcpip(&self.host, self.port, src)
                    .await?)
            }
            SshTunnelConfig::Direct => bail!("connection not setup to use SSH"),
        }
    }
}
