// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use openssh::{ForwardType, Session};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time;
use tracing::{info, warn};

use mz_ore::error::ErrorExt;
use mz_ore::task::{self, AbortOnDropHandle, JoinHandleExt};

use crate::keys::SshKeyPair;

// TODO(benesch): allow configuring the following connection parameters via
// server configuration parameters.

/// How often to check whether the SSH session is still alive.
const CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// The timeout to use when establishing the connection to the SSH server.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The idle time after which the SSH control master process should send a
/// keepalive packet to the SSH server to determine whether the server is
/// still alive.
///
/// TCP idle timeouts of 30s are common in the wild. An idle timeout of 10s
/// is comfortably beneath that threshold without being overly chatty.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(10);

/// Specifies an SSH tunnel.
#[derive(PartialEq, Clone)]
pub struct SshTunnelConfig {
    /// The hostname of the SSH bastion server.
    pub host: String,
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
    ) -> Result<SshTunnelHandle, anyhow::Error> {
        let (mut session, local_port) = connect(self, remote_host, remote_port).await?;
        let local_port = Arc::new(AtomicU16::new(local_port));

        let join_handle = task::spawn(|| format!("ssh_session_{remote_host}:{remote_port}"), {
            let config = self.clone();
            let remote_host = remote_host.to_string();
            let local_port = Arc::clone(&local_port);
            async move {
                scopeguard::defer! {
                    info!(
                        "terminating ssh tunnel ({}:{} via {}@{}:{})",
                        remote_host,
                        remote_port,
                        config.user,
                        config.host,
                        config.port,
                    );
                }
                loop {
                    time::sleep(CHECK_INTERVAL).await;
                    if let Err(e) = session.check().await {
                        warn!("ssh tunnel unhealthy: {}", e.display_with_causes());
                        match connect(&config, &remote_host, remote_port).await {
                            Ok((s, lp)) => {
                                session = s;
                                local_port.store(lp, Ordering::SeqCst)
                            }
                            Err(e) => {
                                warn!(
                                    "reconnection to ssh tunnel failed: {}",
                                    e.display_with_causes()
                                )
                            }
                        };
                    }
                }
            }
        });

        Ok(SshTunnelHandle {
            local_port,
            _join_handle: join_handle.abort_on_drop(),
        })
    }
}

/// A handle to a running SSH tunnel.
#[derive(Debug)]
pub struct SshTunnelHandle {
    local_port: Arc<AtomicU16>,
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
}

async fn connect(
    config: &SshTunnelConfig,
    host: &str,
    port: u16,
) -> Result<(Session, u16), anyhow::Error> {
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

    // Bastion hosts (and therefore keys) tend to change, so we don't want
    // to lock ourselves into trusting only the first we see. In any case,
    // recording a known host would only last as long as the life of a
    // storage pod, so it doesn't offer any protection.
    let session = openssh::SessionBuilder::default()
        .known_hosts_check(openssh::KnownHosts::Accept)
        .user_known_hosts_file("/dev/null")
        .user(config.user.clone())
        .port(config.port)
        .keyfile(&path)
        .server_alive_interval(KEEPALIVE_IDLE)
        .connect_timeout(CONNECT_TIMEOUT)
        .connect_mux(config.host.clone())
        .await?;

    // Delete the private key for safety: since `ssh` still has an open
    // handle to it, it still has access to the key.
    drop(tempfile);
    fs::remove_file(&path)?;
    drop(tempdir);

    // Ensure session is healthy.
    session.check().await?;

    // Loop trying to find an open port.
    for _ in 0..50 {
        // Choose a dynamic port according to RFC 6335.
        let mut rng = StdRng::from_entropy();
        let local_port: u16 = rng.gen_range(49152..65535);

        // Force use of IPv4 loopback. Do not use the hostname `localhost`,
        // as that can resolve to IPv6, and the SSH tunnel is only listening
        // for IPv4 connections.
        let local = openssh::Socket::new(&(Ipv4Addr::LOCALHOST, local_port))?;
        let remote = openssh::Socket::new(&(host, port))?;

        match session
            .request_port_forward(ForwardType::Local, local, remote)
            .await
        {
            Ok(_) => return Ok((session, local_port)),
            Err(err) => match err {
                openssh::Error::SshMux(err)
                    if err.to_string().contains("forwarding request failed") =>
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
