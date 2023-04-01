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
use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;

use anyhow::bail;
use rand::Rng;
use tracing::{error, info};

use crate::keys::SshKeyPair;

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
    /// Establishes a connection to the specified host and port via the configured SSH tunnel.
    ///
    /// Returns the `Session` value you must keep alive to keep the tunnel
    /// open and the local port the tunnel is listening on.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<(openssh::Session, u16), anyhow::Error> {
        let tempdir = tempfile::Builder::new()
            .prefix("ssh-tunnel-key")
            .tempdir()?;
        let path = tempdir.path().join("key");
        let mut tempfile = File::create(&path)?;
        // Grant read and write permissions on the file.
        tempfile.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        tempfile.write_all(self.key_pair.ssh_private_key().as_bytes())?;
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
            .user(self.user.clone())
            .port(self.port)
            .keyfile(&path)
            .connect_mux(self.host.clone())
            .await?;

        // Delete the private key for safety: since `ssh` still has an open
        // handle to it, it still has access to the key.
        drop(tempfile);
        fs::remove_file(&path)?;
        drop(tempdir);

        // Ensure session is healthy.
        session.check().await?;

        // Loop trying to find an open port.
        let mut attempts = 0;
        let local_port = loop {
            if attempts > 50 {
                // If we failed to find an open port after 50 attempts,
                // something is seriously wrong.
                bail!("failed to find an open port to open the SSH tunnel")
            } else {
                attempts += 1;
            }

            let mut rng: rand::rngs::StdRng = rand::SeedableRng::from_entropy();
            // Choosing a dynamic port according to RFC 6335
            let local_port: u16 = rng.gen_range(49152..65535);

            // Force use of IPv4 loopback. Do not use the hostname `localhost`,
            // as that can resolve to IPv6, and the SSH tunnel is only listening
            // for IPv4 connections.
            let local = openssh::Socket::new(&(Ipv4Addr::LOCALHOST, local_port))?;
            let remote = openssh::Socket::new(&(host, port))?;

            match session
                .request_port_forward(openssh::ForwardType::Local, local, remote)
                .await
            {
                Err(err) => match err {
                    openssh::Error::SshMux(err)
                        if err.to_string().contains("forwarding request failed") =>
                    {
                        info!("port {local_port} already in use, testing another port");
                    }
                    _ => {
                        error!("SSH connection failed: {err}");
                        bail!("failed to open SSH tunnel")
                    }
                },
                Ok(_) => break local_port,
            };
        };

        Ok((session, local_port))
    }
}
