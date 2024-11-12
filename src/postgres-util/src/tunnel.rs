// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use mz_ore::future::{InTask, OreFutureExt};
use mz_ore::option::OptionExt;
use mz_ore::task;
use mz_proto::{RustType, TryFromProtoError};
use mz_repr::CatalogItemId;
use mz_ssh_util::tunnel::{SshTimeoutConfig, SshTunnelConfig};
use mz_ssh_util::tunnel_manager::SshTunnelManager;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{Host, ReplicationMode};
use tokio_postgres::tls::MakeTlsConnect;
use tracing::{info, warn};

use crate::PostgresError;

include!(concat!(env!("OUT_DIR"), "/mz_postgres_util.tunnel.rs"));

macro_rules! bail_generic {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($fmt, $($arg)*)))
    };
    ($err:expr $(,)?) => {
        return Err(PostgresError::Generic(anyhow::anyhow!($err)))
    };
}

/// Configures an optional tunnel for use when connecting to a PostgreSQL
/// database.
#[derive(Debug, PartialEq, Clone)]
pub enum TunnelConfig {
    /// Establish a direct TCP connection to the database host.
    /// If `resolved_ips` is not None, the provided IPs will be used
    /// rather than resolving the hostname.
    Direct {
        resolved_ips: Option<BTreeSet<IpAddr>>,
    },
    /// Establish a TCP connection to the database via an SSH tunnel.
    /// This means first establishing an SSH connection to a bastion host,
    /// and then opening a separate connection from that host to the database.
    /// This is commonly referred by vendors as a "direct SSH tunnel", in
    /// opposition to "reverse SSH tunnel", which is currently unsupported.
    Ssh { config: SshTunnelConfig },
    /// Establish a TCP connection to the database via an AWS PrivateLink
    /// service.
    AwsPrivatelink {
        /// The ID of the AWS PrivateLink service.
        connection_id: CatalogItemId,
    },
}

pub const DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT: Duration = Duration::ZERO;

/// A wrapper for [`tokio_postgres::Client`] that can report the server version.
pub struct Client {
    inner: tokio_postgres::Client,
    server_version: Option<String>,
}

impl Client {
    fn new<S, T>(
        client: tokio_postgres::Client,
        connection: &tokio_postgres::Connection<S, T>,
    ) -> Client
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let server_version = connection
            .parameter("server_version")
            .map(|v| v.to_string());
        Client {
            inner: client,
            server_version,
        }
    }

    /// Reports the value of the `server_version` parameter reported by the
    /// server.
    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    /// Reports the postgres flavor as indicated by the server version.
    pub fn server_flavor(&self) -> PostgresFlavor {
        match self.server_version.as_ref() {
            Some(v) if v.contains("-YB-") => PostgresFlavor::Yugabyte,
            _ => PostgresFlavor::Vanilla,
        }
    }
}

impl Deref for Client {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub enum PostgresFlavor {
    /// A normal PostgreSQL server.
    Vanilla,
    /// A Yugabyte server.
    Yugabyte,
}

impl RustType<ProtoPostgresFlavor> for PostgresFlavor {
    fn into_proto(&self) -> ProtoPostgresFlavor {
        let kind = match self {
            PostgresFlavor::Vanilla => proto_postgres_flavor::Kind::Vanilla(()),
            PostgresFlavor::Yugabyte => proto_postgres_flavor::Kind::Yugabyte(()),
        };
        ProtoPostgresFlavor { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoPostgresFlavor) -> Result<Self, TryFromProtoError> {
        let flavor = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("kind"))?;
        Ok(match flavor {
            proto_postgres_flavor::Kind::Vanilla(()) => PostgresFlavor::Vanilla,
            proto_postgres_flavor::Kind::Yugabyte(()) => PostgresFlavor::Yugabyte,
        })
    }
}

/// Configuration for PostgreSQL connections.
///
/// This wraps [`tokio_postgres::Config`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Clone, Debug)]
pub struct Config {
    inner: tokio_postgres::Config,
    tunnel: TunnelConfig,
    in_task: InTask,
    ssh_timeout_config: SshTimeoutConfig,
}

impl Config {
    pub fn new(
        inner: tokio_postgres::Config,
        tunnel: TunnelConfig,
        ssh_timeout_config: SshTimeoutConfig,
        in_task: InTask,
    ) -> Result<Self, PostgresError> {
        let config = Self {
            inner,
            tunnel,
            in_task,
            ssh_timeout_config,
        };

        // Early validate that the configuration contains only a single TCP
        // server.
        config.address()?;

        Ok(config)
    }

    /// Connects to the configured PostgreSQL database.
    pub async fn connect(
        &self,
        task_name: &str,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<Client, PostgresError> {
        self.connect_traced(task_name, |_| (), ssh_tunnel_manager)
            .await
    }

    /// Starts a replication connection to the configured PostgreSQL database.
    pub async fn connect_replication(
        &self,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<Client, PostgresError> {
        self.connect_traced(
            "postgres_connect_replication",
            |config| {
                config.replication_mode(ReplicationMode::Logical);
            },
            ssh_tunnel_manager,
        )
        .await
    }

    fn address(&self) -> Result<(&str, u16), PostgresError> {
        match (self.inner.get_hosts(), self.inner.get_ports()) {
            ([Host::Tcp(host)], [port]) => Ok((host, *port)),
            _ => bail_generic!("only TCP connections to a single PostgreSQL server are supported"),
        }
    }

    async fn connect_traced<F>(
        &self,
        task_name: &str,
        configure: F,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<Client, PostgresError>
    where
        F: FnOnce(&mut tokio_postgres::Config),
    {
        let (host, port) = self.address()?;
        let address = format!(
            "{}@{}:{}/{}",
            self.get_user().display_or("<unknown-user>"),
            host,
            port,
            self.get_dbname().display_or("<unknown-dbname>")
        );
        info!(%task_name, %address, "connecting");
        match self
            .connect_internal(task_name, configure, ssh_tunnel_manager)
            .await
        {
            Ok(t) => {
                let backend_pid = t.backend_pid();
                info!(%task_name, %address, %backend_pid, "connected");
                Ok(t)
            }
            Err(e) => {
                warn!(%task_name, %address, "connection failed: {e:#}");
                Err(e)
            }
        }
    }

    async fn connect_internal<F>(
        &self,
        task_name: &str,
        configure: F,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<Client, PostgresError>
    where
        F: FnOnce(&mut tokio_postgres::Config),
    {
        let mut postgres_config = self.inner.clone();
        configure(&mut postgres_config);

        let mut tls = mz_tls_util::make_tls(&postgres_config).map_err(|tls_err| match tls_err {
            mz_tls_util::TlsError::Generic(e) => PostgresError::Generic(e),
            mz_tls_util::TlsError::OpenSsl(e) => PostgresError::PostgresSsl(e),
        })?;

        match &self.tunnel {
            TunnelConfig::Direct { resolved_ips } => {
                if let Some(ips) = resolved_ips {
                    let host = match postgres_config.get_hosts() {
                        [Host::Tcp(host)] => host,
                        _ => bail_generic!(
                            "only TCP connections to a single PostgreSQL server are supported"
                        ),
                    }
                    .to_owned();
                    // Associate each resolved ip with the exact same, singular host, for tls
                    // verification. We are required to do this dance because `tokio-postgres`
                    // enforces that the number of 'host' and 'hostaddr' values must be the same.
                    for (idx, ip) in ips.iter().enumerate() {
                        if idx != 0 {
                            postgres_config.host(&host);
                        }
                        postgres_config.hostaddr(ip.clone());
                    }
                };

                let (client, connection) = async move { postgres_config.connect(tls).await }
                    .run_in_task_if(self.in_task, || "pg_connect".to_string())
                    .await?;
                let client = Client::new(client, &connection);
                task::spawn(|| task_name, connection);
                Ok(client)
            }
            TunnelConfig::Ssh { config } => {
                let (host, port) = self.address()?;
                let tunnel = ssh_tunnel_manager
                    .connect(
                        config.clone(),
                        host,
                        port,
                        self.ssh_timeout_config,
                        self.in_task,
                    )
                    .await
                    .map_err(PostgresError::Ssh)?;

                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect(tunnel.local_addr())
                    .await
                    .map_err(PostgresError::SshIo)?;
                // Because we are connecting to a local host/port, we don't configure any TCP
                // keepalive settings. The connection is entirely local to the machine running the
                // process and we trust the kernel to keep a local connection alive without keepalives.
                //
                // Ideally we'd be able to configure SSH to enable TCP keepalives on the other
                // end of the tunnel, between the SSH bastion host and the PostgreSQL server,
                // but SSH does not expose an option for this.
                let (client, connection) =
                    async move { postgres_config.connect_raw(tcp_stream, tls).await }
                        .run_in_task_if(self.in_task, || "pg_connect".to_string())
                        .await?;
                let client = Client::new(client, &connection);
                task::spawn(|| task_name, async {
                    let _tunnel = tunnel; // Keep SSH tunnel alive for duration of connection.

                    if let Err(e) = connection.await {
                        warn!("postgres connection failed: {e}");
                    }
                });
                Ok(client)
            }
            TunnelConfig::AwsPrivatelink { connection_id } => {
                // This section of code is somewhat subtle. We are overriding the host
                // for the actual TCP connection to be the PrivateLink host, but leaving the host
                // for TLS verification as the original host. Managing the
                // `tokio_postgres::Config` to do this is somewhat confusing, and requires we edit
                // the singular host in place.

                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);
                // `net::lookup_host` requires a port to be specified, but the port has no effect
                // on the lookup so use a dummy one
                let privatelink_addrs = tokio::net::lookup_host((privatelink_host, 11111)).await?;

                // Override the actual IPs to connect to for the TCP connection, leaving the original host in-place
                // for TLS verification
                let host = match postgres_config.get_hosts() {
                    [Host::Tcp(host)] => host,
                    _ => bail_generic!(
                        "only TCP connections to a single PostgreSQL server are supported"
                    ),
                }
                .to_owned();
                // Associate each resolved ip with the exact same, singular host, for tls
                // verification. We are required to do this dance because `tokio-postgres`
                // enforces that the number of 'host' and 'hostaddr' values must be the same.
                for (idx, addr) in privatelink_addrs.enumerate() {
                    if idx != 0 {
                        postgres_config.host(&host);
                    }
                    postgres_config.hostaddr(addr.ip());
                }

                let (client, connection) = async move { postgres_config.connect(tls).await }
                    .run_in_task_if(self.in_task, || "pg_connect".to_string())
                    .await?;
                let client = Client::new(client, &connection);
                task::spawn(|| task_name, connection);
                Ok(client)
            }
        }
    }

    pub fn get_user(&self) -> Option<&str> {
        self.inner.get_user()
    }

    pub fn get_dbname(&self) -> Option<&str> {
        self.inner.get_dbname()
    }
}
