// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_postgres::config::{Host, ReplicationMode};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::Client;
use tracing::warn;

use mz_ore::task;
use mz_repr::GlobalId;
use mz_ssh_util::tunnel::SshTunnelConfig;

use crate::{make_tls, PostgresError};

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

/// Configurable timeouts that apply only when using [`Config::connect_replication`].
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationTimeouts {
    pub connect_timeout: Option<Duration>,
    pub keepalives_retries: Option<u32>,
    pub keepalives_idle: Option<Duration>,
    pub keepalives_interval: Option<Duration>,
    pub tcp_user_timeout: Option<Duration>,
}

// Some of these defaults were recommended by @ph14
// https://github.com/MaterializeInc/materialize/pull/18644#discussion_r1160071692
pub const DEFAULT_REPLICATION_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_REPLICATION_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_REPLICATION_KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
pub const DEFAULT_REPLICATION_KEEPALIVE_RETRIES: u32 = 5;
// This is meant to be DEFAULT_REPLICATION_KEEPALIVE_IDLE
// + DEFAULT_REPLICATION_KEEPALIVE_RETRIES * DEFAULT_REPLICATION_KEEPALIVE_INTERVAL
pub const DEFAULT_REPLICATION_TCP_USER_TIMEOUT: Duration = Duration::from_secs(60);

/// Configuration for PostgreSQL connections.
///
/// This wraps [`tokio_postgres::Config`] to allow the configuration of a
/// tunnel via a [`TunnelConfig`].
#[derive(Debug, PartialEq, Clone)]
pub struct Config {
    inner: tokio_postgres::Config,
    tunnel: TunnelConfig,
    replication_timeouts: ReplicationTimeouts,
}

impl Config {
    pub fn new(inner: tokio_postgres::Config, tunnel: TunnelConfig) -> Result<Self, PostgresError> {
        let config = Self {
            inner,
            tunnel,
            replication_timeouts: ReplicationTimeouts::default(),
        };

        // Early validate that the configuration contains only a single TCP
        // server.
        config.address()?;

        Ok(config)
    }

    pub fn replication_timeouts(mut self, replication_timeouts: ReplicationTimeouts) -> Config {
        self.replication_timeouts = replication_timeouts;
        self
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
                .connect_timeout(
                    self.replication_timeouts
                        .connect_timeout
                        .unwrap_or(DEFAULT_REPLICATION_CONNECT_TIMEOUT),
                )
                .keepalives_interval(
                    self.replication_timeouts
                        .keepalives_interval
                        .unwrap_or(DEFAULT_REPLICATION_KEEPALIVE_INTERVAL),
                )
                .keepalives_idle(
                    self.replication_timeouts
                        .keepalives_idle
                        .unwrap_or(DEFAULT_REPLICATION_KEEPALIVE_IDLE),
                )
                .keepalives_retries(
                    self.replication_timeouts
                        .keepalives_retries
                        .unwrap_or(DEFAULT_REPLICATION_KEEPALIVE_RETRIES),
                )
                .tcp_user_timeout(
                    self.replication_timeouts
                        .tcp_user_timeout
                        .unwrap_or(DEFAULT_REPLICATION_TCP_USER_TIMEOUT),
                );
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
                let tunnel = tunnel.connect(host, port).await?;
                let tls = MakeTlsConnect::<TokioTcpStream>::make_tls_connect(&mut tls, host)?;
                let tcp_stream = TokioTcpStream::connect(tunnel.local_addr()).await?;
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
