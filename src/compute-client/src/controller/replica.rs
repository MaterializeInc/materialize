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
use mz_cluster_client::client::{ClusterReplicaLocation, ClusterStartupEpoch, TimelyConfig};
use mz_compute_types::dyncfgs::ENABLE_COMPUTE_REPLICA_EXPIRATION;
use mz_dyncfg::ConfigSet;
use mz_ore::channel::InstrumentedUnboundedSender;
use mz_ore::retry::{Retry, RetryState};
use mz_ore::task::AbortOnDropHandle;
use mz_service::client::GenericClient;
use mz_service::params::GrpcClientParameters;
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{debug, info, trace, warn};

use crate::controller::instance::ReplicaResponse;
use crate::controller::sequential_hydration::SequentialHydration;
use crate::controller::{ComputeControllerTimestamp, ReplicaId};
use crate::logging::LoggingConfig;
use crate::metrics::IntCounter;
use crate::metrics::ReplicaMetrics;
use crate::protocol::command::{ComputeCommand, InitialComputeParameters};
use crate::protocol::response::ComputeResponse;
use crate::service::{ComputeClient, ComputeGrpcClient};

type Client<T> = SequentialHydration<T>;

/// Replica-specific configuration.
#[derive(Clone, Debug)]
pub(super) struct ReplicaConfig {
    pub location: ClusterReplicaLocation,
    pub logging: LoggingConfig,
    pub grpc_client: GrpcClientParameters,
    /// The offset to use for replica expiration, if any.
    pub expiration_offset: Option<Duration>,
    pub initial_config: InitialComputeParameters,
}

/// A client for a replica task.
#[derive(Debug)]
pub(super) struct ReplicaClient<T> {
    /// A sender for commands for the replica.
    command_tx: UnboundedSender<ComputeCommand<T>>,
    /// A handle to the task that aborts it when the replica is dropped.
    ///
    /// If the task is finished, the replica has failed and needs rehydration.
    task: AbortOnDropHandle<()>,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// Flag reporting whether the replica connection has been established.
    connected: Arc<AtomicBool>,
}

impl<T> ReplicaClient<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn spawn(
        id: ReplicaId,
        build_info: &'static BuildInfo,
        config: ReplicaConfig,
        epoch: ClusterStartupEpoch,
        metrics: ReplicaMetrics,
        dyncfg: Arc<ConfigSet>,
        response_tx: InstrumentedUnboundedSender<ReplicaResponse<T>, IntCounter>,
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

impl<T> ReplicaClient<T> {
    /// Sends a command to this replica.
    pub(super) fn send(
        &self,
        command: ComputeCommand<T>,
    ) -> Result<(), SendError<ComputeCommand<T>>> {
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

/// Configuration for `replica_task`.
struct ReplicaTask<T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Replica configuration.
    config: ReplicaConfig,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: InstrumentedUnboundedSender<ReplicaResponse<T>, IntCounter>,
    /// A number (technically, pair of numbers) identifying this incarnation of the replica.
    /// The semantics of this don't matter, except that it must strictly increase.
    epoch: ClusterStartupEpoch,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// Flag to report successful replica connection.
    connected: Arc<AtomicBool>,
    /// Dynamic system configuration.
    dyncfg: Arc<ConfigSet>,
}

impl<T> ReplicaTask<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
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
    async fn connect(&self) -> Client<T> {
        let try_connect = |retry: RetryState| {
            let addrs = &self.config.location.ctl_addrs;
            let dests = addrs
                .iter()
                .map(|addr| (addr.clone(), self.metrics.clone()))
                .collect();
            let version = self.build_info.semver_version();
            let client_params = &self.config.grpc_client;

            async move {
                let connect_start = Instant::now();
                let connect_result =
                    ComputeGrpcClient::connect_partitioned(dests, version, client_params).await;
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
            }
        };

        let client = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(try_connect)
            .await
            .expect("retry retries forever");

        self.metrics.observe_connect();
        self.connected.store(true, atomic::Ordering::Relaxed);

        let dyncfg = Arc::clone(&self.dyncfg);
        let metrics = self.metrics.clone();
        SequentialHydration::new(client, dyncfg, metrics)
    }

    /// Runs the message loop.
    ///
    /// Returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
    /// If no error condition is encountered, the task runs until the controller disconnects from
    /// the command channel, or the task is dropped.
    async fn run_message_loop(mut self, mut client: Client<T>) -> Result<(), anyhow::Error>
    where
        T: ComputeControllerTimestamp,
        ComputeGrpcClient: ComputeClient<T>,
    {
        let id = self.replica_id;
        let incarnation = self.epoch.replica();
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
                    client.send(command).await?;
                },
                // Response from replica to forward to controller.
                response = client.recv() => {
                    let Some(response) = response? else {
                        bail!("replica unexpectedly gracefully terminated connection");
                    };

                    self.observe_response(&response);

                    if self.response_tx.send((id, incarnation, response)).is_err() {
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
    fn specialize_command(&self, command: &mut ComputeCommand<T>) {
        match command {
            ComputeCommand::CreateTimely { config, epoch } => {
                **config = TimelyConfig {
                    workers: self.config.location.workers,
                    process: 0,
                    addresses: self.config.location.dataflow_addrs.clone(),
                    arrangement_exert_proportionality: self
                        .config
                        .initial_config
                        .arrangement_exert_proportionality,
                    enable_zero_copy: self.config.initial_config.enable_zero_copy,
                    enable_zero_copy_lgalloc: self.config.initial_config.enable_zero_copy_lgalloc,
                    zero_copy_limit: self.config.initial_config.zero_copy_limit,
                };
                *epoch = self.epoch;
            }
            ComputeCommand::CreateInstance(config) => {
                config.logging = self.config.logging.clone();
                if ENABLE_COMPUTE_REPLICA_EXPIRATION.get(&self.dyncfg) {
                    config.expiration_offset = self.config.expiration_offset;
                }
            }
            _ => {}
        }
    }

    /// Update task state according to an observed command.
    #[mz_ore::instrument(level = "debug")]
    fn observe_command(&mut self, command: &ComputeCommand<T>) {
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
    fn observe_response(&mut self, response: &ComputeResponse<T>) {
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
