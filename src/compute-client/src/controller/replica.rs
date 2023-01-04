// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client for replicas of a compute instance.

use std::time::Duration;

use anyhow::bail;
use differential_dataflow::lattice::Lattice;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::TryFutureExt;
use mz_orchestrator::ServiceProcessMetrics;
use timely::progress::Timestamp;
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use mz_build_info::BuildInfo;
use mz_ore::retry::Retry;
use mz_service::client::GenericClient;

use crate::command::{ComputeCommand, ComputeStartupEpoch, TimelyConfig};
use crate::logging::LoggingConfig;
use crate::response::ComputeResponse;
use crate::service::{ComputeClient, ComputeGrpcClient};

use super::orchestrator::ComputeOrchestrator;
use super::{ComputeInstanceId, ComputeReplicaLocation, ReplicaId};

/// A response from a replica to the controller
#[derive(Debug)]
pub(crate) enum ReplicaResponse<T> {
    ComputeResponse(ComputeResponse<T>),
    MetricsUpdate(Result<Vec<ServiceProcessMetrics>, anyhow::Error>),
}

/// State for a single replica.
#[derive(Debug)]
pub(super) struct Replica<T> {
    /// A sender for commands for the replica.
    ///
    /// If sending to this channel fails, the replica has failed and requires
    /// rehydration.
    command_tx: UnboundedSender<ComputeCommand<T>>,
    /// A receiver for responses from the replica.
    ///
    /// If receiving from the channel returns `None`, the replica has failed
    /// and requires rehydration.
    response_rx: UnboundedReceiver<ReplicaResponse<T>>,
    /// Location of the replica
    pub location: ComputeReplicaLocation,
    /// The logging config specific to this replica.
    pub logging_config: LoggingConfig,
    /// Handle to the active-replication-replica task.
    pub replica_task: Option<JoinHandle<()>>,
}

impl<T> Replica<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn spawn(
        id: ReplicaId,
        instance_id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        location: ComputeReplicaLocation,
        logging_config: LoggingConfig,
        orchestrator: ComputeOrchestrator,
        epoch: ComputeStartupEpoch,
    ) -> Self {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();

        let replica_task = mz_ore::task::spawn(
            || format!("active-replication-replica-{id}"),
            ReplicaTask {
                instance_id,
                replica_id: id,
                build_info,
                location: location.clone(),
                logging_config: logging_config.clone(),
                orchestrator,
                command_rx,
                response_tx,
                epoch,
            }
            .run(),
        );

        Self {
            command_tx,
            response_rx,
            location,
            logging_config,
            replica_task: Some(replica_task),
        }
    }

    /// Sends a command to this replica.
    pub(super) fn send(
        &self,
        command: ComputeCommand<T>,
    ) -> Result<(), SendError<ComputeCommand<T>>> {
        self.command_tx.send(command)
    }

    /// Receives the next response from this replica.
    ///
    /// This method is cancellation safe.
    pub(super) async fn recv(&mut self) -> Option<ReplicaResponse<T>> {
        self.response_rx.recv().await
    }
}

/// Configuration for `replica_task`.
struct ReplicaTask<T> {
    /// The ID of the compute instance.
    instance_id: ComputeInstanceId,
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Location
    location: ComputeReplicaLocation,
    /// Logging
    logging_config: LoggingConfig,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: UnboundedSender<ReplicaResponse<T>>,
    /// Orchestrator responsible for setting up clusterds
    orchestrator: ComputeOrchestrator,
    /// A number (technically, pair of numbers) identifying this incarnation of the replica.
    /// The semantics of this don't matter, except that it must strictly increase.
    epoch: ComputeStartupEpoch,
}

fn metrics_stream(
    orchestrator: ComputeOrchestrator,
    instance_id: ComputeInstanceId,
    replica_id: ReplicaId,
) -> BoxStream<'static, Result<Vec<ServiceProcessMetrics>, anyhow::Error>> {
    const METRICS_INTERVAL: Duration = Duration::from_secs(10);

    // TODO[btv] -- I tried implementing a `watch_metrics` function,
    // similar to `watch_services`, but it crashed due to
    // https://github.com/kube-rs/kube/issues/1092 .
    //
    // If `metrics-server` can be made to fill in `resourceVersion`,
    // or if that bug is fixed, we can try that again rather than using this inelegant
    // loop.
    let s = async_stream::stream! {
        let mut interval = tokio::time::interval(METRICS_INTERVAL);
        loop {
            interval.tick().await;
            yield orchestrator.fetch_replica_metrics(instance_id, replica_id).await;
        }
    };
    s.boxed()
}

impl<T> ReplicaTask<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Asynchronously forwards commands to and responses from a single replica.
    async fn run(self) {
        let ReplicaTask {
            instance_id,
            replica_id,
            location,
            logging_config,
            build_info,
            command_rx,
            response_tx,
            orchestrator,
            epoch,
        } = self;

        tracing::info!("starting replica task for {replica_id}");

        let result = orchestrator
            .ensure_replica_location(instance_id, replica_id, location)
            .and_then(|(addrs, timely_config)| {
                let cmd_spec = CommandSpecialization {
                    logging_config,
                    timely_config,
                    epoch,
                };
                let metrics = metrics_stream(orchestrator.clone(), instance_id, replica_id);

                run_message_loop(
                    replica_id,
                    command_rx,
                    response_tx,
                    build_info,
                    addrs,
                    cmd_spec,
                    metrics,
                )
            })
            .await;

        if let Err(error) = result {
            tracing::warn!("replica task for {replica_id} failed: {error}");
        } else {
            panic!("replica message loop should never return successfully");
        }
    }
}

/// Connects to the replica and runs the message loop.
///
/// The initial replica connection is retried forever (with backoff). Once connected, the task
/// returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
///
/// If no error condition is encountered, the task runs forever. The instance controller should
/// expected to abort the task when the replica is removed.
async fn run_message_loop<T>(
    replica_id: ReplicaId,
    mut command_rx: UnboundedReceiver<ComputeCommand<T>>,
    response_tx: UnboundedSender<ReplicaResponse<T>>,
    build_info: &BuildInfo,
    addrs: Vec<String>,
    cmd_spec: CommandSpecialization,
    mut metrics: BoxStream<'static, Result<Vec<ServiceProcessMetrics>, anyhow::Error>>,
) -> Result<(), anyhow::Error>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let mut client = Retry::default()
        .clamp_backoff(Duration::from_secs(32))
        .retry_async(|state| {
            let addrs = addrs.clone();
            let version = build_info.semver_version();

            async move {
                match ComputeGrpcClient::connect_partitioned(addrs, version).await {
                    Ok(client) => Ok(client),
                    Err(e) => {
                        if state.i >= mz_service::retry::INFO_MIN_RETRIES {
                            tracing::info!(
                                "error connecting to replica {replica_id}, retrying in {:?}: {e}",
                                state.next_backoff.unwrap()
                            );
                        } else {
                            tracing::debug!(
                                "error connecting to replica {replica_id}, retrying in {:?}: {e}",
                                state.next_backoff.unwrap()
                            );
                        }
                        Err(e)
                    }
                }
            }
        })
        .await
        .expect("retry retries forever");

    loop {
        select! {
            // Command from controller to forward to replica.
            command = command_rx.recv() => match command {
                None => bail!("controller unexpectedly dropped command_rx"),
                Some(mut command) => {
                    cmd_spec.specialize_command(&mut command);
                    client.send(command).await?;
                }
            },
            // Response from replica to forward to controller.
            response = client.recv() => {
                let response = match response? {
                    None => bail!("replica unexpectedly gracefully terminated connection"),
                    Some(response) => response,
                };
                response_tx.send(ReplicaResponse::ComputeResponse(response))?;
            }
            metrics_result = metrics.next() => {
                let Some(metrics_result) = metrics_result else {
                    tracing::error!("Metrics stream unexpectedly terminated");
                    continue;
                };
                response_tx.send(ReplicaResponse::MetricsUpdate(metrics_result))?;
            }
        }
    }
}

struct CommandSpecialization {
    logging_config: LoggingConfig,
    timely_config: TimelyConfig,
    epoch: ComputeStartupEpoch,
}

impl CommandSpecialization {
    /// Specialize a command for the given replica configuration.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command<T>(&self, command: &mut ComputeCommand<T>) {
        if let ComputeCommand::CreateInstance(logging) = command {
            *logging = self.logging_config.clone();
        }

        if let ComputeCommand::CreateTimely { config, epoch } = command {
            *config = self.timely_config.clone();
            *epoch = self.epoch;
        }
    }
}
