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

use mysql_async::{Conn, Opts, OptsBuilder};

use mz_ore::future::{InTask, TimeoutError};
use mz_ore::option::OptionExt;
use mz_ore::task::{spawn, JoinHandleExt};
use mz_repr::CatalogItemId;
use mz_ssh_util::tunnel::{SshTimeoutConfig, SshTunnelConfig};
use mz_ssh_util::tunnel_manager::{ManagedSshTunnelHandle, SshTunnelManager};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::MySqlError;

/// Configures an optional tunnel for use when connecting to a MySQL
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

pub const DEFAULT_TCP_KEEPALIVE: Duration = Duration::from_secs(60);
pub const DEFAULT_SNAPSHOT_MAX_EXECUTION_TIME: Duration = Duration::ZERO;
pub const DEFAULT_SNAPSHOT_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(3600);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutConfig {
    // Snapshot-related configs
    pub snapshot_max_execution_time: Option<Duration>,
    pub snapshot_lock_wait_timeout: Option<Duration>,

    // Socket-related configs
    pub tcp_keepalive: Option<Duration>,

    // Connection timeout.  This timeout covers creating an authenticated connection
    // (e.g. includes network connection, TLS handshake, authentication, etc.).
    // If the connection has not been established in that time, it is considered an error.
    pub connect_timeout: Option<Duration>,
    // There are other timeout options on `mysql_async::OptsBuilder`
    // (e.g. `conn_ttl` and `wait_timeout`) that could be exposed
    // but they only apply to connection pools, which we are not currently using.
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            snapshot_max_execution_time: Some(DEFAULT_SNAPSHOT_MAX_EXECUTION_TIME),
            snapshot_lock_wait_timeout: Some(DEFAULT_SNAPSHOT_LOCK_WAIT_TIMEOUT),
            tcp_keepalive: Some(DEFAULT_TCP_KEEPALIVE),
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
        }
    }
}

impl TimeoutConfig {
    pub fn build(
        snapshot_max_execution_time: Duration,
        snapshot_lock_wait_timeout: Duration,
        tcp_keepalive: Duration,
        connect_timeout: Duration,
    ) -> Self {
        // Verify values are within valid ranges
        // Note we error log but do not fail as this is called in a non-fallible
        // LD-sync in the adapter.

        // https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lock_wait_timeout
        let snapshot_lock_wait_timeout = if snapshot_lock_wait_timeout.as_secs() > 31536000 {
            error!(
                "snapshot_lock_wait_timeout is too large: {}. Maximum is 31536000.",
                snapshot_lock_wait_timeout.as_secs()
            );
            Some(DEFAULT_SNAPSHOT_LOCK_WAIT_TIMEOUT)
        } else {
            Some(snapshot_lock_wait_timeout)
        };

        // https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_execution_time
        let snapshot_max_execution_time = if snapshot_max_execution_time.as_millis() > 4294967295 {
            error!(
                "snapshot_max_execution_time is too large: {}. Maximum is 4294967295.",
                snapshot_max_execution_time.as_secs()
            );
            Some(DEFAULT_SNAPSHOT_MAX_EXECUTION_TIME)
        } else {
            Some(snapshot_max_execution_time)
        };

        let tcp_keepalive = match u32::try_from(tcp_keepalive.as_millis()) {
            Err(_) => {
                error!(
                    "tcp_keepalive is too large: {}. Maximum is {}.",
                    tcp_keepalive.as_millis(),
                    u32::MAX,
                );
                Some(DEFAULT_TCP_KEEPALIVE)
            }
            Ok(_) => Some(tcp_keepalive),
        };

        let connect_timeout = match u32::try_from(connect_timeout.as_millis()) {
            Err(_) => {
                error!(
                    "connect_timeout is too large: {}. Maximum is {}.",
                    connect_timeout.as_millis(),
                    u32::MAX,
                );
                Some(DEFAULT_CONNECT_TIMEOUT)
            }
            Ok(_) => Some(connect_timeout),
        };

        Self {
            snapshot_max_execution_time,
            snapshot_lock_wait_timeout,
            tcp_keepalive,
            connect_timeout,
        }
    }

    /// Apply relevant timeout configurations to a `mysql_async::OptsBuilder`.
    pub fn apply_to_opts(&self, mut opts_builder: OptsBuilder) -> Result<OptsBuilder, MySqlError> {
        if let Some(tcp_keepalive) = self.tcp_keepalive {
            opts_builder = opts_builder.tcp_keepalive(Some(
                u32::try_from(tcp_keepalive.as_millis()).map_err(|e| {
                    MySqlError::InvalidClientConfig(format!(
                        "invalid tcp_keepalive duration: {}",
                        e
                    ))
                })?,
            ));
        }
        Ok(opts_builder)
    }
}

/// A MySQL connection with an optional SSH tunnel handle.
///
/// This wrapper is intended to be used in place of `mysql_async::Conn` to
/// keep the SSH tunnel alive for the lifecycle of the connection by holding
/// a reference to the tunnel handle.
#[derive(Debug)]
pub struct MySqlConn {
    conn: Conn,
    _ssh_tunnel_handle: Option<ManagedSshTunnelHandle>,
}

impl Deref for MySqlConn {
    type Target = Conn;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for MySqlConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl MySqlConn {
    pub async fn disconnect(mut self) -> Result<(), MySqlError> {
        self.conn.disconnect().await?;
        self._ssh_tunnel_handle.take();
        Ok(())
    }

    pub fn take(self) -> (Conn, Option<ManagedSshTunnelHandle>) {
        (self.conn, self._ssh_tunnel_handle)
    }
}

/// Configuration for MySQL connections.
///
/// This wraps [`mysql_async::Opts`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Clone, Debug)]
pub struct Config {
    inner: Opts,
    tunnel: TunnelConfig,
    // Whether to poll I/O for this connection in a tokio task
    // TODO(roshan): Make this apply to queries on the returned connection, not just the initial
    // connection.
    in_task: InTask,
    ssh_timeout_config: SshTimeoutConfig,
    mysql_timeout_config: TimeoutConfig,
}

impl Config {
    pub fn new(
        builder: OptsBuilder,
        tunnel: TunnelConfig,
        ssh_timeout_config: SshTimeoutConfig,
        in_task: InTask,
        mysql_timeout_config: TimeoutConfig,
    ) -> Result<Self, MySqlError> {
        let opts = mysql_timeout_config.apply_to_opts(builder)?;
        Ok(Self {
            inner: opts.into(),
            tunnel,
            in_task,
            ssh_timeout_config,
            mysql_timeout_config,
        })
    }

    pub async fn connect(
        &self,
        task_name: &str,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<MySqlConn, MySqlError> {
        let address = format!(
            "mysql:://{}@{}:{}/{}",
            self.inner.user().display_or("<unknown-user>"),
            self.inner.ip_or_hostname(),
            self.inner.tcp_port(),
            self.inner.db_name().display_or("<unknown-dbname>"),
        );
        info!(%task_name, %address, "connecting");
        match self.connect_internal(ssh_tunnel_manager).await {
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

    fn address(&self) -> (&str, u16) {
        (self.inner.ip_or_hostname(), self.inner.tcp_port())
    }

    async fn connect_internal(
        &self,
        ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<MySqlConn, MySqlError> {
        let mut opts_builder = OptsBuilder::from_opts(self.inner.clone());

        match &self.tunnel {
            TunnelConfig::Direct { resolved_ips } => {
                opts_builder = opts_builder.resolved_ips(
                    resolved_ips
                        .clone()
                        .map(|ips| ips.into_iter().collect::<Vec<_>>()),
                );

                Ok(MySqlConn {
                    conn: self.connect_with_timeout(opts_builder).await?,
                    _ssh_tunnel_handle: None,
                })
            }
            TunnelConfig::Ssh { config } => {
                let (host, port) = self.address();
                let tunnel = ssh_tunnel_manager
                    .connect(
                        config.clone(),
                        host,
                        port,
                        self.ssh_timeout_config,
                        self.in_task,
                    )
                    .await
                    .map_err(MySqlError::Ssh)?;

                let tunnel_addr = tunnel.local_addr();
                // Override the connection host and port for the actual TCP connection to point to
                // the local tunnel instead.
                opts_builder = opts_builder
                    .ip_or_hostname(tunnel_addr.ip().to_string())
                    .tcp_port(tunnel_addr.port());

                if let Some(ssl_opts) = self.inner.ssl_opts() {
                    if !ssl_opts.skip_domain_validation() {
                        // If the TLS configuration will validate the hostname, we need to set
                        // the TLS hostname back to the actual upstream host and not the hostname
                        // of the local SSH tunnel
                        opts_builder = opts_builder.ssl_opts(Some(
                            ssl_opts.clone().with_danger_tls_hostname_override(Some(
                                self.inner.ip_or_hostname().to_string(),
                            )),
                        ));
                    }
                }

                Ok(MySqlConn {
                    conn: self.connect_with_timeout(opts_builder).await?,
                    _ssh_tunnel_handle: Some(tunnel),
                })
            }
            TunnelConfig::AwsPrivatelink { connection_id } => {
                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);

                // Override the connection host for the actual TCP connection to point to
                // the privatelink hostname instead.
                let mut opts_builder = opts_builder.ip_or_hostname(privatelink_host);

                if let Some(ssl_opts) = self.inner.ssl_opts() {
                    if !ssl_opts.skip_domain_validation() {
                        // If the TLS configuration will validate the hostname, we need to set
                        // the TLS hostname back to the actual upstream host and not the
                        // privatelink hostname.
                        opts_builder = opts_builder.ssl_opts(Some(
                            ssl_opts.clone().with_danger_tls_hostname_override(Some(
                                self.inner.ip_or_hostname().to_string(),
                            )),
                        ));
                    }
                }

                Ok(MySqlConn {
                    conn: self.connect_with_timeout(opts_builder).await?,
                    _ssh_tunnel_handle: None,
                })
            }
        }
    }

    async fn connect_with_timeout(
        &self,
        opts_builder: OptsBuilder,
    ) -> Result<mysql_async::Conn, MySqlError> {
        let connection_future = if let InTask::Yes = self.in_task {
            spawn(|| "mysql_connect".to_string(), Conn::new(opts_builder))
                .abort_on_drop()
                .wait_and_assert_finished()
        } else {
            Conn::new(opts_builder)
        };

        if let Some(connect_timeout) = self.mysql_timeout_config.connect_timeout {
            mz_ore::future::timeout(connect_timeout, connection_future)
                .await
                .map_err(|err| match err {
                    // match instead of impl From<> for MySqlError so we can capture the timeout value
                    TimeoutError::DeadlineElapsed => MySqlError::ConnectionTimeout(connect_timeout),
                    TimeoutError::Inner(e) => MySqlError::from(e),
                })
        } else {
            connection_future.await.map_err(MySqlError::from)
        }
    }
}
