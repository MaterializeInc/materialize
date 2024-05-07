// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::bail;
use itertools::Itertools;
use mz_ore::error::ErrorExt;
use mz_ore::task::{self, AbortOnDropHandle};
use openssh::{ForwardType, Session};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::time;
use tracing::{info, warn};

use crate::keys::SshKeyPair;

// TODO(benesch): allow configuring the following connection parameters via
// server configuration parameters.

pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(30);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// TCP idle timeouts of 30s are common in the wild. An idle timeout of 10s
/// is comfortably beneath that threshold without being overly chatty.
pub const DEFAULT_KEEPALIVES_IDLE: Duration = Duration::from_secs(10);

/// Configuration of Ssh session and tunnel timeouts.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SshTimeoutConfig {
    /// How often to check whether the SSH session is still alive.
    pub check_interval: Duration,
    /// The timeout to use when establishing the connection to the SSH server.
    pub connect_timeout: Duration,
    /// The idle time after which the SSH control leader process should send a
    /// keepalive packet to the SSH server to determine whether the server is
    /// still alive.
    pub keepalives_idle: Duration,
}

impl Default for SshTimeoutConfig {
    fn default() -> SshTimeoutConfig {
        SshTimeoutConfig {
            check_interval: DEFAULT_CHECK_INTERVAL,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            keepalives_idle: DEFAULT_KEEPALIVES_IDLE,
        }
    }
}

/// Specifies an SSH tunnel.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SshTunnelConfig {
    /// The hostname/IP of the SSH bastion server.
    /// If multiple hosts are specified, they are tried in order.
    pub host: BTreeSet<String>,
    /// The port to connect to.
    pub port: u16,
    /// The name of the user to connect as.
    pub user: String,
    /// The SSH key pair to authenticate with.
    pub key_pair: SshKeyPair,
}

impl fmt::Debug for SshTunnelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tunnel")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            // Omit keys from debug output.
            .finish()
    }
}

impl fmt::Display for SshTunnelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{}:{}",
            self.user,
            self.host.iter().join(","),
            self.port
        )
    }
}

/// The status of a running SSH tunnel.
#[derive(Clone, Debug)]
pub enum SshTunnelStatus {
    /// The SSH tunnel is healthy.
    Running,
    /// The SSH tunnel is broken, with the given error message.
    Errored(String),
}

impl SshTunnelConfig {
    /// Establishes a connection to the specified host and port via the
    /// configured SSH tunnel.
    ///
    /// Returns a handle to the SSH tunnel. The SSH tunnel is automatically shut
    /// down when the handle is dropped.
    pub async fn connect(
        &self,
        remote_host: &str,
        remote_port: u16,
        timeout_config: SshTimeoutConfig,
    ) -> Result<SshTunnelHandle, anyhow::Error> {
        let tunnel_id = format!("{}:{} via {}", remote_host, remote_port, self);

        // N.B.
        //
        // We could probably move this into the look and use the above channel to report this
        // initial connection error, but this is simpler and easier to read!
        info!(%tunnel_id, "connecting to ssh tunnel");
        let mut session = match connect(self, timeout_config).await {
            Ok(s) => s,
            Err(e) => {
                warn!(%tunnel_id, "failed to connect to ssh tunnel: {}", e.display_with_causes());
                return Err(e);
            }
        };
        let local_port = match port_forward(&session, remote_host, remote_port).await {
            Ok(local_port) => local_port,
            Err(e) => {
                warn!(%tunnel_id, "failed to forward port through ssh tunnel: {}", e.display_with_causes());
                return Err(e);
            }
        };
        info!(%tunnel_id, %local_port, "connected to ssh tunnel");
        let local_port = Arc::new(AtomicU16::new(local_port));
        let status = Arc::new(Mutex::new(SshTunnelStatus::Running));

        let join_handle = task::spawn(|| format!("ssh_session_{remote_host}:{remote_port}"), {
            let config = self.clone();
            let remote_host = remote_host.to_string();
            let local_port = Arc::clone(&local_port);
            let status = Arc::clone(&status);
            async move {
                scopeguard::defer! {
                    info!(%tunnel_id, "terminating ssh tunnel");
                }
                let mut interval = time::interval(timeout_config.check_interval);
                // Just in case checking takes a long time.
                interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                // The first tick happens immediately.
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Err(e) = session.check().await {
                        warn!(%tunnel_id, "ssh tunnel unhealthy: {}", e.display_with_causes());
                        let s = match connect(&config, timeout_config).await {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(%tunnel_id, "reconnection to ssh tunnel failed: {}", e.display_with_causes());
                                *status.lock().expect("poisoned") =
                                    SshTunnelStatus::Errored(e.to_string_with_causes());
                                continue;
                            }
                        };
                        let lp = match port_forward(&s, &remote_host, remote_port).await {
                            Ok(lp) => lp,
                            Err(e) => {
                                warn!(%tunnel_id, "reconnection to ssh tunnel failed: {}", e.display_with_causes());
                                *status.lock().expect("poisoned") =
                                    SshTunnelStatus::Errored(e.to_string_with_causes());
                                continue;
                            }
                        };
                        session = s;
                        local_port.store(lp, Ordering::SeqCst);
                        *status.lock().expect("poisoned") = SshTunnelStatus::Running;
                    }
                }
            }
        });

        Ok(SshTunnelHandle {
            local_port,
            status,
            _join_handle: join_handle.abort_on_drop(),
        })
    }

    /// Validates the SSH configuration by establishing a connection to the intermediate SSH
    /// bastion host. It does not set up a port forwarding tunnel.
    pub async fn validate(&self, timeout_config: SshTimeoutConfig) -> Result<(), anyhow::Error> {
        connect(self, timeout_config).await?;
        Ok(())
    }
}

/// A handle to a running SSH tunnel.
#[derive(Debug)]
pub struct SshTunnelHandle {
    local_port: Arc<AtomicU16>,
    status: Arc<Mutex<SshTunnelStatus>>,
    _join_handle: AbortOnDropHandle<()>,
}

impl SshTunnelHandle {
    /// Returns the local address at which the SSH tunnel is listening.
    pub fn local_addr(&self) -> SocketAddr {
        let port = self.local_port.load(Ordering::SeqCst);
        // Force use of IPv4 loopback. Do not use the hostname `localhost`, as
        // that can resolve to IPv6, and the SSH tunnel is only listening for
        // IPv4 connections.
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    /// Returns the current status of the SSH tunnel.
    ///
    /// Note this status may be stale, as the health of the underlying SSH
    /// tunnel is only checked periodically.
    pub fn check_status(&self) -> SshTunnelStatus {
        self.status.lock().expect("poisoned").clone()
    }
}

async fn connect(
    config: &SshTunnelConfig,
    timeout_config: SshTimeoutConfig,
) -> Result<Session, anyhow::Error> {
    let tempdir = tempfile::Builder::new()
        .prefix("ssh-tunnel-key")
        .tempdir()?;
    let path = tempdir.path().join("key");
    let mut tempfile = File::create(&path)?;
    // Grant read and write permissions on the file.
    tempfile.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    tempfile.write_all(config.key_pair.ssh_private_key().as_bytes())?;
    // Remove write permissions as soon as the key is written.
    // Mostly helpful to ensure the file is not accidentally overwritten.
    tempfile.set_permissions(std::fs::Permissions::from_mode(0o400))?;

    // Try connecting to each host in turn.
    let mut connect_err = None;
    for host in &config.host {
        // Bastion hosts (and therefore keys) tend to change, so we don't want
        // to lock ourselves into trusting only the first we see. In any case,
        // recording a known host would only last as long as the life of a
        // storage pod, so it doesn't offer any protection.
        match openssh::SessionBuilder::default()
            .known_hosts_check(openssh::KnownHosts::Accept)
            .user_known_hosts_file("/dev/null")
            .user(config.user.clone())
            .port(config.port)
            .keyfile(&path)
            .server_alive_interval(timeout_config.keepalives_idle)
            .connect_timeout(timeout_config.connect_timeout)
            .connect_mux(host.clone())
            .await
        {
            Ok(session) => {
                // Delete the private key for safety: since `ssh` still has an open
                // handle to it, it still has access to the key.
                drop(tempfile);
                fs::remove_file(&path)?;
                drop(tempdir);

                // Ensure session is healthy.
                session.check().await?;

                return Ok(session);
            }
            Err(err) => {
                connect_err = Some(err);
            }
        }
    }
    Err(connect_err
        .map(Into::into)
        .unwrap_or_else(|| anyhow::anyhow!("no hosts to connect to")))
}

async fn port_forward(session: &Session, host: &str, port: u16) -> Result<u16, anyhow::Error> {
    // Loop trying to find an open port.
    for _ in 0..50 {
        // Choose a dynamic port according to RFC 6335.
        let mut rng = StdRng::from_entropy();
        let local_port: u16 = rng.gen_range(49152..65535);

        // Force use of IPv4 loopback. Do not use the hostname `localhost`,
        // as that can resolve to IPv6, and the SSH tunnel is only listening
        // for IPv4 connections.
        let local = openssh::Socket::from((Ipv4Addr::LOCALHOST, local_port));
        let remote = openssh::Socket::new(host, port);

        match session
            .request_port_forward(ForwardType::Local, local, remote)
            .await
        {
            Ok(_) => return Ok(local_port),
            Err(err) => match err {
                openssh::Error::SshMux(openssh_mux_client::Error::RequestFailure(e))
                    if &*e == "Port forwarding failed" =>
                {
                    info!("port {local_port} already in use; testing another port");
                }
                _ => {
                    warn!("ssh connection failed: {}", err.display_with_causes());
                    bail!("failed to open SSH tunnel: {}", err.display_with_causes())
                }
            },
        };
    }
    // If we failed to find an open port after 50 attempts,
    // something is seriously wrong.
    bail!("failed to find an open port for SSH tunnel")
}
