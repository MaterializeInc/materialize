// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client for replicas of a compute instance.

use std::sync::Arc;
use std::sync::atomic::{self, AtomicBool};
use std::time::{Duration, Instant};

use anyhow::bail;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_compute_types::dyncfgs::ENABLE_COMPUTE_REPLICA_EXPIRATION;
use mz_dyncfg::{ConfigSet, ConfigUpdates};
use mz_ore::channel::InstrumentedUnboundedSender;
use mz_ore::retry::{Retry, RetryState};
use mz_ore::task::AbortOnDropHandle;
use mz_service::client::{GenericClient, Partitioned};
use mz_service::params::GrpcClientParameters;
use mz_service::transport;
use mz_service::transport::tls::ClientTlsConfig;
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::controller::ReplicaId;
use crate::controller::instance::ReplicaResponse;
use crate::controller::sequential_hydration::SequentialHydration;
use crate::logging::LoggingConfig;
use crate::metrics::IntCounter;
use crate::metrics::ReplicaMetrics;
use crate::protocol::command::ComputeCommand;
use crate::protocol::response::ComputeResponse;

type Client = Partitioned<ComputeCtpClient, ComputeCommand, ComputeResponse>;

/// Replica-specific configuration.
#[derive(Clone, Debug)]
pub(super) struct ReplicaConfig {
    pub location: ClusterReplicaLocation,
    pub logging: LoggingConfig,
    pub grpc_client: GrpcClientParameters,
    /// TLS config for connecting to the replica, if cluster transport TLS is enabled.
    pub tls: Option<ClientTlsConfig>,
    /// The offset to use for replica expiration, if any.
    pub expiration_offset: Option<Duration>,
    /// Whether arrangements on this replica use dictionary compression, captured at creation.
    pub arrangement_dictionary_compression: bool,
}

/// A client for a replica task.
#[derive(Debug)]
pub(super) struct ReplicaClient {
    /// A sender for commands for the replica.
    command_tx: UnboundedSender<ComputeCommand>,
    /// A handle to the task that aborts it when the replica is dropped.
    ///
    /// If the task is finished, the replica has failed and needs rehydration.
    task: AbortOnDropHandle<()>,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// Flag reporting whether the replica connection has been established.
    connected: Arc<AtomicBool>,
}

impl ReplicaClient {
    pub(super) fn spawn(
        id: ReplicaId,
        build_info: &'static BuildInfo,
        config: ReplicaConfig,
        epoch: u64,
        metrics: ReplicaMetrics,
        dyncfg: Arc<ConfigSet>,
        response_tx: InstrumentedUnboundedSender<ReplicaResponse, IntCounter>,
    ) -> Self {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let connected = Arc::new(AtomicBool::new(false));

        let task = mz_ore::task::spawn(
            || format!("active-replication-replica-{id}"),
            ReplicaTask {
                replica_id: id,
                build_info,
                config: config.clone(),
                command_rx,
                response_tx,
                epoch,
                metrics: metrics.clone(),
                connected: Arc::clone(&connected),
                replica_dyncfg: seed_replica_dyncfg(&dyncfg),
                dyncfg,
            }
            .run(),
        );

        Self {
            command_tx,
            task: task.abort_on_drop(),
            metrics,
            connected,
        }
    }
}

impl ReplicaClient {
    /// Sends a command to this replica.
    pub(super) fn send(&self, command: ComputeCommand) -> Result<(), SendError<ComputeCommand>> {
        self.command_tx.send(command).map(|r| {
            self.metrics.inner.command_queue_size.inc();
            r
        })
    }

    /// Determine if the replica task has failed.
    pub(super) fn is_failed(&self) -> bool {
        self.task.is_finished()
    }

    /// Determine if the replica connection has been established.
    pub(super) fn is_connected(&self) -> bool {
        self.connected.load(atomic::Ordering::Relaxed)
    }
}

type ComputeCtpClient = transport::Client<ComputeCommand, ComputeResponse>;

/// Creates a replica's effective configuration, seeded from the environment-wide one.
///
/// The seed covers the window before the first configuration command arrives, and is replaced
/// wholesale by the snapshot that `CreateInstance` carries.
fn seed_replica_dyncfg(dyncfg: &ConfigSet) -> ConfigSet {
    let replica_dyncfg = mz_dyncfgs::all_dyncfgs();
    ConfigUpdates::from(dyncfg).apply(&replica_dyncfg);
    replica_dyncfg
}

/// Applies the configuration a command carries, if any, to a replica's effective configuration.
///
/// `CreateInstance` carries a full snapshot, `UpdateConfiguration` the subsequent deltas.
fn apply_config_command(command: &ComputeCommand, dyncfg: &ConfigSet) {
    match command {
        ComputeCommand::CreateInstance(config) => config.initial_config.apply(dyncfg),
        ComputeCommand::UpdateConfiguration(params) => params.dyncfg_updates.apply(dyncfg),
        _ => (),
    }
}

/// Configuration for `replica_task`.
struct ReplicaTask {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Replica configuration.
    config: ReplicaConfig,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: InstrumentedUnboundedSender<ReplicaResponse, IntCounter>,
    /// A number identifying this incarnation of the replica.
    /// The semantics of this don't matter, except that it must strictly increase.
    epoch: u64,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// Flag to report successful replica connection.
    connected: Arc<AtomicBool>,
    /// The controller's environment-wide dynamic system configuration.
    dyncfg: Arc<ConfigSet>,
    /// This replica's effective dynamic system configuration.
    ///
    /// Holds what the replica itself reads, including its scoped overrides, as opposed to
    /// [`Self::dyncfg`], which holds the environment-wide values. Seeded from the environment-wide
    /// configuration and then kept current from the configuration commands passing through this
    /// task, which `Instance::specialize_command_for_replica` has already specialized for this
    /// replica. Read it for any `ParameterScope::Replica` config the controller realizes on this
    /// replica's behalf, else the scope declaration is a silent no-op.
    ///
    /// A set of its own, rather than a clone of the controller's: a cloned `ConfigSet` shares its
    /// values with the original, so applying this replica's overrides to a clone would overwrite
    /// the environment-wide configuration for everyone.
    replica_dyncfg: ConfigSet,
}

impl ReplicaTask {
    /// Asynchronously forwards commands to and responses from a single replica.
    async fn run(self) {
        let replica_id = self.replica_id;
        info!(replica = ?replica_id, "starting replica task");

        let client = self.connect().await;
        match self.run_message_loop(client).await {
            Ok(()) => info!(replica = ?replica_id, "stopped replica task"),
            Err(error) => warn!(replica = ?replica_id, "replica task failed: {error:#}"),
        }
    }

    /// Connects to the replica.
    ///
    /// The connection is retried forever (with backoff) and this method returns only after
    /// a connection was successfully established.
    async fn connect(&self) -> Client {
        let try_connect = async |retry: RetryState| {
            let version = self.build_info.semver_version();
            let client_params = &self.config.grpc_client;
            let connect_timeout = client_params.connect_timeout.unwrap_or(Duration::MAX);
            let keepalive_timeout = client_params
                .http2_keep_alive_timeout
                .unwrap_or(Duration::MAX);

            let connect_start = Instant::now();
            let connect_result = ComputeCtpClient::connect_partitioned(
                self.config.location.ctl_addrs.clone(),
                version,
                self.config.tls.clone(),
                connect_timeout,
                keepalive_timeout,
                self.metrics.clone(),
            )
            .await;

            self.metrics.observe_connect_time(connect_start.elapsed());

            connect_result.inspect_err(|error| {
                let next_backoff = retry.next_backoff.unwrap();
                if retry.i >= mz_service::retry::INFO_MIN_RETRIES {
                    info!(
                        replica_id = %self.replica_id, ?next_backoff,
                        "error connecting to replica: {error:#}",
                    );
                } else {
                    debug!(
                        replica_id = %self.replica_id, ?next_backoff,
                        "error connecting to replica: {error:#}",
                    );
                }
            })
        };

        let client = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(try_connect)
            .await
            .expect("retry retries forever");

        self.metrics.observe_connect();
        self.connected.store(true, atomic::Ordering::Relaxed);

        client
    }

    /// Runs the message loop.
    ///
    /// Returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
    /// If no error condition is encountered, the task runs until the controller disconnects from
    /// the command channel, or the task is dropped.
    async fn run_message_loop(mut self, mut client: Client) -> Result<(), anyhow::Error> {
        // The sequential hydration interceptor holds back `Schedule` commands and releases them as
        // hydration capacity frees up. It is recreated per incarnation, matching the lifetime of
        // the connection: any in-flight hydration state is reset when we reconnect.
        let mut hydration = SequentialHydration::new(self.metrics.clone());

        loop {
            select! {
                // Command from controller to forward to replica.
                command = self.command_rx.recv() => {
                    let Some(mut command) = command else {
                        // Controller is no longer interested in this replica. Shut down.
                        break;
                    };

                    self.specialize_command(&mut command);
                    self.observe_command(&command);
                    apply_config_command(&command, &self.replica_dyncfg);
                    for command in hydration.absorb_command(command, &self.replica_dyncfg) {
                        client.send(command).await?;
                    }
                },
                // Response from replica to forward to controller.
                response = client.recv() => {
                    let Some(response) = response? else {
                        bail!("replica unexpectedly gracefully terminated connection");
                    };

                    self.observe_response(&response);

                    for command in hydration.observe_response(&response, &self.replica_dyncfg) {
                        client.send(command).await?;
                    }

                    if self.response_tx.send((self.replica_id, self.epoch, response)).is_err() {
                        // Controller is no longer interested in this replica. Shut down.
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Specialize a command for the given replica configuration.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command(&self, command: &mut ComputeCommand) {
        match command {
            ComputeCommand::Hello { nonce } => {
                *nonce = Uuid::new_v4();
            }
            ComputeCommand::CreateInstance(config) => {
                config.logging = self.config.logging.clone();
                if ENABLE_COMPUTE_REPLICA_EXPIRATION.get(&self.dyncfg) {
                    config.expiration_offset = self.config.expiration_offset;
                }
                config.arrangement_dictionary_compression =
                    self.config.arrangement_dictionary_compression;
            }
            _ => {}
        }
    }

    /// Update task state according to an observed command.
    #[mz_ore::instrument(level = "debug")]
    fn observe_command(&self, command: &ComputeCommand) {
        if let ComputeCommand::Peek(peek) = command {
            peek.otel_ctx.attach_as_parent();
        }

        trace!(
            replica = ?self.replica_id,
            command = ?command,
            "sending command to replica",
        );

        self.metrics.inner.command_queue_size.dec();
    }

    /// Update task state according to an observed response.
    #[mz_ore::instrument(level = "debug")]
    fn observe_response(&self, response: &ComputeResponse) {
        if let ComputeResponse::PeekResponse(_, _, otel_ctx) = response {
            otel_ctx.attach_as_parent();
        }

        trace!(
            replica = ?self.replica_id,
            response = ?response,
            "received response from replica",
        );
    }
}

#[cfg(test)]
mod tests {
    use mz_compute_types::dyncfgs::HYDRATION_CONCURRENCY;

    use crate::protocol::command::{ComputeParameters, InstanceConfig};

    use super::*;

    /// A replica's effective configuration tracks the configuration commands passing through its
    /// task, which carry the replica's scoped overrides, and leaves the environment-wide
    /// configuration alone.
    #[mz_ore::test]
    fn replica_dyncfg_tracks_config_commands() {
        let env_wide = mz_dyncfgs::all_dyncfgs();
        let mut updates = ConfigUpdates::default();
        updates.add(&HYDRATION_CONCURRENCY, 1);
        updates.apply(&env_wide);

        let replica_dyncfg = seed_replica_dyncfg(&env_wide);
        assert_eq!(HYDRATION_CONCURRENCY.get(&replica_dyncfg), 1);

        // A replica-scoped override arrives merged into the create-time snapshot.
        let mut initial_config = ConfigUpdates::default();
        initial_config.add(&HYDRATION_CONCURRENCY, 2);
        let create = ComputeCommand::CreateInstance(Box::new(InstanceConfig {
            logging: Default::default(),
            expiration_offset: None,
            peek_stash_persist_location: mz_persist_client::PersistLocation::new_in_mem(),
            arrangement_dictionary_compression: false,
            initial_config,
        }));
        apply_config_command(&create, &replica_dyncfg);
        assert_eq!(HYDRATION_CONCURRENCY.get(&replica_dyncfg), 2);

        // And into subsequent configuration updates.
        let mut dyncfg_updates = ConfigUpdates::default();
        dyncfg_updates.add(&HYDRATION_CONCURRENCY, 3);
        let update = ComputeCommand::UpdateConfiguration(Box::new(ComputeParameters {
            dyncfg_updates,
            ..Default::default()
        }));
        apply_config_command(&update, &replica_dyncfg);
        assert_eq!(HYDRATION_CONCURRENCY.get(&replica_dyncfg), 3);

        // The environment-wide configuration is untouched by the replica's overrides.
        assert_eq!(HYDRATION_CONCURRENCY.get(&env_wide), 1);
    }
}
