// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SSH tunnel management.

// NOTE(benesch): The synchronization in this module is tricky because SSH
// tunnels 1) require an async `connect` method that can return errors and 2)
// once connected, launch a long-running background task whose handle must be
// managed. The manager would be far simpler if `connect` was neither async nor
// fallible and instead synchronously returned a handle to the background task.
// That would require a different means of asynchronously reporting SSH tunnel
// errors, though, and that's a large project. A worthwhile project, though: at
// present SSH tunnel errors that occur after the initial connection are
// reported only to the logs, and not to users.

use std::collections::{btree_map, BTreeMap};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use mz_ore::future::{InTask, OreFutureExt};
use scopeguard::ScopeGuard;
use tokio::sync::watch;
use tracing::{error, info};

use crate::tunnel::{SshTimeoutConfig, SshTunnelConfig, SshTunnelHandle, SshTunnelStatus};

/// Thread-safe manager of SSH tunnel connections.
#[derive(Debug, Clone, Default)]
pub struct SshTunnelManager {
    tunnels: Arc<Mutex<BTreeMap<SshTunnelKey, SshTunnelState>>>,
}

impl SshTunnelManager {
    /// Establishes an SSH tunnel for the given remote host and port using the
    /// provided `tunnel` configuration.
    ///
    /// If there is an existing SSH tunnel, a handle to that tunnel is returned,
    /// rather than establishing a new tunnel.
    ///
    /// The manager guarantees that there will never be more than one in flight
    /// connection attempt for the same tunnel, even when this method is called
    /// concurrently from multiple threads.
    pub async fn connect(
        &self,
        config: SshTunnelConfig,
        remote_host: &str,
        remote_port: u16,
        // This could be held behind a lock and updated within the global `SshTunnelManager`, but
        // requiring all configuration at connection time is more consistent with how other
        // connections work within the workspace.
        timeout_config: SshTimeoutConfig,
        // Whether or not to connect to ssh from a Tokio task (to ensure futures are
        // polled promptly).
        in_task: InTask,
    ) -> Result<ManagedSshTunnelHandle, anyhow::Error> {
        // An SSH tunnel connection is uniquely identified by the SSH tunnel
        // configuration and the remote address.
        let key = SshTunnelKey {
            config: config.clone(),
            remote_host: remote_host.to_string(),
            remote_port,
        };

        loop {
            // NOTE: this code is structured awkwardly to convince rustc that
            // the lock is not held across an await point. rustc's analysis
            // does not take into account explicit `drop` calls, so we have to
            // structure such that the lock guard goes out of scope.
            // See: https://github.com/rust-lang/rust/issues/69663
            enum Action {
                Return(ManagedSshTunnelHandle),
                AwaitConnection(watch::Receiver<()>),
                StartConnection(watch::Sender<()>),
            }

            let action = match self
                .tunnels
                .lock()
                .expect("lock poisoned")
                .entry(key.clone())
            {
                btree_map::Entry::Occupied(mut occupancy) => match occupancy.get_mut() {
                    // There is an existing tunnel.
                    SshTunnelState::Connected(handle) => Action::Return(ManagedSshTunnelHandle {
                        handle: Arc::clone(handle),
                        manager: self.clone(),
                        key: key.clone(),
                    }),
                    // There is an existing connection attempt.
                    SshTunnelState::Connecting(rx) => Action::AwaitConnection(rx.clone()),
                },
                btree_map::Entry::Vacant(vacancy) => {
                    // There is no existing tunnel or connection attempt. Record
                    // that we're starting one.
                    let (tx, rx) = watch::channel(());
                    vacancy.insert(SshTunnelState::Connecting(rx));
                    Action::StartConnection(tx)
                }
            };

            match action {
                Action::Return(handle) => {
                    if let SshTunnelStatus::Errored(e) = handle.check_status() {
                        error!(
                            "not using existing ssh tunnel \
                            ({}:{} via {}) because it's broken: {e}",
                            remote_host, remote_port, config
                        );

                        // This is bit unfortunate, as this method returns an
                        // `anyhow::Error`, but the SSH status needs to share a
                        // cloneable `String`. So we just package up the
                        // pre-`.to_string_with_causes()` error that is at the
                        // bottom of the stack. In the future we can probably
                        // make ALL SSH errors structured to avoid this.
                        return Err(anyhow::anyhow!(e));
                    }

                    info!(
                        "reusing existing ssh tunnel ({}:{} via {})",
                        remote_host, remote_port, config
                    );
                    return Ok(handle);
                }
                Action::AwaitConnection(mut rx) => {
                    // Wait for the connection attempt to finish. The next turn
                    // of the loop will determine whether the connection attempt
                    // succeeded or failed and proceed accordingly.
                    let _ = rx.changed().await;
                }
                Action::StartConnection(_tx) => {
                    // IMPORTANT: clear the `Connecting` state on scope exit.
                    // This is *required* for cancel safety. If the future is
                    // dropped at the following await point, we need to record
                    // that we are no longer attemping the connection.
                    let guard = scopeguard::guard((), |()| {
                        let mut tunnels = self.tunnels.lock().expect("lock poisoned");
                        tunnels.remove(&key);
                    });

                    // Try to connect.
                    info!(
                        "initiating new ssh tunnel ({}:{} via {})",
                        remote_host, remote_port, config
                    );

                    let config = config.clone();
                    let remote_host = remote_host.to_string();
                    let handle = async move {
                        config
                            .connect(&remote_host, remote_port, timeout_config)
                            .await
                    }
                    .run_in_task_if(in_task, || "ssh_connect".to_string())
                    .await?;

                    // Successful connection, so defuse the scope guard.
                    let _ = ScopeGuard::into_inner(guard);

                    // Record the tunnel handle for future threads.
                    let handle = Arc::new(handle);
                    let mut tunnels = self.tunnels.lock().expect("lock poisoned");
                    tunnels.insert(key.clone(), SshTunnelState::Connected(Arc::clone(&handle)));

                    // Return a handle to the tunnel.
                    return Ok(ManagedSshTunnelHandle {
                        handle,
                        manager: self.clone(),
                        key: key.clone(),
                    });
                }
            }
        }
    }
}

/// Identifies a connection to a remote host via an SSH tunnel.
/// There are a couple of edge cases where this key format may result
/// in extra connections being created:
/// 1. If a host resolves to a different number of ips on different workers
/// 2. Different workers connect to different upstream resolved ips if they
/// appear connectable at different times.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
struct SshTunnelKey {
    config: SshTunnelConfig,
    remote_host: String,
    remote_port: u16,
}

/// The state of an SSH tunnel connection.
///
/// There is an additional state not represented by this enum, which is the
/// absence of an entry in the map entirely, indicating there is neither an
/// existing tunnel nor an existing connection attempt.
#[derive(Debug)]
enum SshTunnelState {
    /// An existing thread is connecting to the tunnel.
    ///
    /// The managing thread will resolve the enclosed future when the connection
    /// attempt is complete. Only the thread that entered the `Connecting` state
    /// is allowed to move out of this state.
    Connecting(watch::Receiver<()>),
    /// An existing thread has successfully established the tunnel.
    ///
    /// Only the last `ManagedSshTunnelHandle` is allowed to move out of this
    /// state.
    Connected(Arc<SshTunnelHandle>),
}

/// A clonable handle to an SSH tunnel managed by an [`SshTunnelManager`].
///
/// The tunnel will be automatically closed when all handles are dropped.
#[derive(Debug, Clone)]
pub struct ManagedSshTunnelHandle {
    handle: Arc<SshTunnelHandle>,
    manager: SshTunnelManager,
    key: SshTunnelKey,
}

impl Deref for ManagedSshTunnelHandle {
    type Target = SshTunnelHandle;

    fn deref(&self) -> &SshTunnelHandle {
        &self.handle
    }
}

impl Drop for ManagedSshTunnelHandle {
    fn drop(&mut self) {
        let mut tunnels = self.manager.tunnels.lock().expect("lock poisoned");
        // If there are only two strong references, the manager holds one and we
        // hold the other, so this is the last handle.
        //
        // IMPORTANT: We must be holding the lock when we perform this check, to
        // ensure no other threads can acquire a new handle via the manager.
        if Arc::strong_count(&self.handle) == 2 {
            tunnels.remove(&self.key);
        }
    }
}
