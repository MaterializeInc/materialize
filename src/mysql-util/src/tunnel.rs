// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::option::OptionExt;
use mz_ssh_util::tunnel_manager::SshTunnelManager;

use mysql_async::{Conn, Opts};
use tracing::{info, warn};

use crate::MySqlError;

/// Configures an optional tunnel for use when connecting to a MySQL
/// database.
#[derive(Debug, PartialEq, Clone)]
pub enum TunnelConfig {
    /// Establish a direct TCP connection to the database host.
    Direct,
    // TODO: Implement SSH tunneling for MySQL connections
    // TODO: Implement AWS PrivateLink tunneling for MySQL connections
}

/// Configuration for MySQL connections.
///
/// This wraps [`mysql_async::Opts`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Clone, Debug)]
pub struct Config {
    inner: Opts,
    tunnel: TunnelConfig,
}

impl Config {
    pub fn new(inner: Opts, tunnel: TunnelConfig) -> Self {
        Self { inner, tunnel }
    }

    pub async fn connect(
        &self,
        task_name: &str,
        _ssh_tunnel_manager: &SshTunnelManager,
    ) -> Result<Conn, MySqlError> {
        let address = format!(
            "mysql:://{}@{}:{}/{}",
            self.inner.user().display_or("<unknown-user>"),
            self.inner.ip_or_hostname(),
            self.inner.tcp_port(),
            self.inner.db_name().display_or("<unknown-dbname>"),
        );
        info!(%task_name, %address, "connecting");
        match self.connect_internal().await {
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

    async fn connect_internal(&self) -> Result<Conn, MySqlError> {
        match &self.tunnel {
            TunnelConfig::Direct => Conn::new(self.inner.clone())
                .await
                .map_err(MySqlError::from),
        }
    }
}
