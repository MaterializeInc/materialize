// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::{Deref, DerefMut};

use mysql_async::{Conn, Opts, OptsBuilder};
use tracing::{info, warn};

use mz_ore::option::OptionExt;
use mz_repr::GlobalId;
use mz_ssh_util::tunnel::{SshTimeoutConfig, SshTunnelConfig};
use mz_ssh_util::tunnel_manager::{ManagedSshTunnelHandle, SshTunnelManager};

use crate::MySqlError;

/// Configures an optional tunnel for use when connecting to a MySQL
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
    ssh_timeout_config: SshTimeoutConfig,
}

impl Config {
    pub fn new(inner: Opts, tunnel: TunnelConfig, ssh_timeout_config: SshTimeoutConfig) -> Self {
        Self {
            inner,
            tunnel,
            ssh_timeout_config,
        }
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
        match &self.tunnel {
            TunnelConfig::Direct => Ok(MySqlConn {
                conn: Conn::new(self.inner.clone())
                    .await
                    .map_err(MySqlError::from)?,
                _ssh_tunnel_handle: None,
            }),
            TunnelConfig::Ssh { config } => {
                let (host, port) = self.address();
                let tunnel = ssh_tunnel_manager
                    .connect(config.clone(), host, port, self.ssh_timeout_config)
                    .await
                    .map_err(MySqlError::Ssh)?;

                let tunnel_addr = tunnel.local_addr();
                // Override the connection host and port for the actual TCP connection to point to
                // the local tunnel instead.
                let mut opts_builder = OptsBuilder::from_opts(self.inner.clone())
                    .ip_or_hostname(tunnel_addr.ip().to_string())
                    .tcp_port(tunnel_addr.port());

                if let Some(ssl_opts) = self.inner.ssl_opts() {
                    if !ssl_opts.skip_domain_validation() {
                        // If the TLS configuration will validate the hostname, we need to set
                        // the TLS hostname back to the actual upstream host and not the hostname
                        // of the local SSH tunnel
                        opts_builder = opts_builder.ssl_opts(Some(
                            ssl_opts.clone().with_tls_hostname_override(Some(
                                self.inner.ip_or_hostname().to_string(),
                            )),
                        ));
                    }
                }

                Ok(MySqlConn {
                    conn: Conn::new(opts_builder).await.map_err(MySqlError::from)?,
                    _ssh_tunnel_handle: Some(tunnel),
                })
            }
            TunnelConfig::AwsPrivatelink { connection_id } => {
                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);

                // Override the connection host for the actual TCP connection to point to
                // the privatelink hostname instead.
                let mut opts_builder =
                    OptsBuilder::from_opts(self.inner.clone()).ip_or_hostname(privatelink_host);

                if let Some(ssl_opts) = self.inner.ssl_opts() {
                    if !ssl_opts.skip_domain_validation() {
                        // If the TLS configuration will validate the hostname, we need to set
                        // the TLS hostname back to the actual upstream host and not the
                        // privatelink hostname.
                        opts_builder = opts_builder.ssl_opts(Some(
                            ssl_opts.clone().with_tls_hostname_override(Some(
                                self.inner.ip_or_hostname().to_string(),
                            )),
                        ));
                    }
                }

                Ok(MySqlConn {
                    conn: Conn::new(opts_builder).await.map_err(MySqlError::from)?,
                    _ssh_tunnel_handle: None,
                })
            }
        }
    }
}
