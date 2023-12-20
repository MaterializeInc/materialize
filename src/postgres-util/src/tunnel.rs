// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_ore::option::OptionExt;
use mz_ore::task;
use mz_repr::GlobalId;
use mz_ssh_util::tunnel::{SshTimeoutConfig, SshTunnelConfig};
use mz_ssh_util::tunnel_manager::SshTunnelManager;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{Host, ReplicationMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::Client;
use tracing::{info, warn};

use crate::PostgresError;

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
    Direct,
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
        connection_id: GlobalId,
    },
}

// Some of these defaults were recommended by @ph14
// https://github.com/MaterializeInc/materialize/pull/18644#discussion_r1160071692
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
pub const DEFAULT_KEEPALIVE_RETRIES: u32 = 5;
// This is meant to be DEFAULT_KEEPALIVE_IDLE
// + DEFAULT_KEEPALIVE_RETRIES * DEFAULT_KEEPALIVE_INTERVAL
pub const DEFAULT_TCP_USER_TIMEOUT: Duration = Duration::from_secs(60);

/// Configurable timeouts for Postgres connections.
///
/// Currently only apply to non-ssh/privatelink connections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpTimeoutConfig {
    pub connect_timeout: Option<Duration>,
    pub keepalives_retries: Option<u32>,
    pub keepalives_idle: Option<Duration>,
    pub keepalives_interval: Option<Duration>,
    pub tcp_user_timeout: Option<Duration>,
}

impl Default for TcpTimeoutConfig {
    fn default() -> Self {
        TcpTimeoutConfig {
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
            keepalives_retries: Some(DEFAULT_KEEPALIVE_RETRIES),
            keepalives_idle: Some(DEFAULT_KEEPALIVE_IDLE),
            keepalives_interval: Some(DEFAULT_KEEPALIVE_INTERVAL),
            tcp_user_timeout: Some(DEFAULT_TCP_USER_TIMEOUT),
        }
    }
}

pub const DEFAULT_SNAPSHOT_STATEMENT_TIMEOUT: Duration = Duration::ZERO;

/// Configuration for PostgreSQL connections.
///
/// This wraps [`tokio_postgres::Config`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Clone, Debug)]
pub struct Config {
    inner: tokio_postgres::Config,
    tunnel: TunnelConfig,
    ssh_timeout_config: SshTimeoutConfig,
}

impl Config {
    pub fn new(
        inner: tokio_postgres::Config,
        tunnel: TunnelConfig,
        tcp_timeouts: TcpTimeoutConfig,
        ssh_timeout_config: SshTimeoutConfig,
    ) -> Result<Self, PostgresError> {
        let config = Self {
            inner,
            tunnel,
            ssh_timeout_config,
        }
        .tcp_timeouts(tcp_timeouts);

        // Early validate that the configuration contains only a single TCP
        // server.
        config.address()?;

        Ok(config)
    }

    fn tcp_timeouts(mut self, tcp_timeouts: TcpTimeoutConfig) -> Config {
        if let Some(connect_timeout) = tcp_timeouts.connect_timeout {
            self.inner.connect_timeout(connect_timeout);
        }
        if let Some(keepalives_retries) = tcp_timeouts.keepalives_retries {
            self.inner.keepalives_retries(keepalives_retries);
        }
        if let Some(keepalives_idle) = tcp_timeouts.keepalives_idle {
            self.inner.keepalives_idle(keepalives_idle);
        }
        if let Some(keepalives_interval) = tcp_timeouts.keepalives_interval {
            self.inner.keepalives_interval(keepalives_interval);
        }
        if let Some(tcp_user_timeout) = tcp_timeouts.tcp_user_timeout {
            self.inner.tcp_user_timeout(tcp_user_timeout);
        }
        self
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
                info!(%task_name, %address, "connected");
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
            TunnelConfig::Direct => {
                let (client, connection) = postgres_config.connect(tls).await?;
                task::spawn(|| task_name, connection);
                Ok(client)
            }
            TunnelConfig::Ssh { config } => {
                let (host, port) = self.address()?;
                let tunnel = ssh_tunnel_manager
                    .connect(config.clone(), host, port, self.ssh_timeout_config)
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
                let (client, connection) = postgres_config.connect_raw(tcp_stream, tls).await?;
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

                let (host, _) = self.address()?;
                postgres_config.tls_verify_host(host);

                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);

                match postgres_config.get_hosts_mut() {
                    [Host::Tcp(host)] => *host = privatelink_host,
                    _ => bail_generic!(
                        "only TCP connections to a single PostgreSQL server are supported"
                    ),
                }

                let (client, connection) = postgres_config.connect(tls).await?;
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
