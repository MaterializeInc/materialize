// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client for replicas of a compute instance.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::bail;
use differential_dataflow::lattice::Lattice;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterReplicaLocation, ClusterStartupEpoch, TimelyConfig};
use mz_ore::retry::Retry;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_repr::GlobalId;
use mz_service::client::{GenericClient, Partitioned};
use mz_service::params::GrpcClientParameters;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{debug, info, trace, warn};

use crate::controller::ReplicaId;
use crate::logging::LoggingConfig;
use crate::metrics::{ReplicaCollectionMetrics, ReplicaMetrics};
use crate::protocol::command::{ComputeCommand, InstanceConfig};
use crate::protocol::response::{ComputeResponse, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};

type ReplicaClient<T> = Partitioned<ComputeGrpcClient, ComputeCommand<T>, ComputeResponse<T>>;

/// Replica-specific configuration.
#[derive(Clone, Debug)]
pub(super) struct ReplicaConfig {
    pub location: ClusterReplicaLocation,
    pub logging: LoggingConfig,
    pub idle_arrangement_merge_effort: u32,
    pub grpc_client: GrpcClientParameters,
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
    response_rx: UnboundedReceiver<ComputeResponse<T>>,
    /// A handle to the task that aborts it when the replica is dropped.
    _task: AbortOnDropHandle<()>,
    /// Configuration specific to this replica.
    pub config: ReplicaConfig,
    /// Replica metrics.
    metrics: ReplicaMetrics,
}

impl<T> Replica<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub(super) fn spawn(
        id: ReplicaId,
        build_info: &'static BuildInfo,
        config: ReplicaConfig,
        epoch: ClusterStartupEpoch,
        metrics: ReplicaMetrics,
    ) -> Self {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();

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
                collections: Default::default(),
            }
            .run(),
        );

        Self {
            command_tx,
            response_rx,
            _task: task.abort_on_drop(),
            config,
            metrics,
        }
    }

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

    /// Receives the next response from this replica.
    ///
    /// This method is cancellation safe.
    pub(super) async fn recv(&mut self) -> Option<ComputeResponse<T>> {
        self.response_rx.recv().await.map(|r| {
            self.metrics.inner.response_queue_size.dec();
            r
        })
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
    response_tx: UnboundedSender<ComputeResponse<T>>,
    /// A number (technically, pair of numbers) identifying this incarnation of the replica.
    /// The semantics of this don't matter, except that it must strictly increase.
    epoch: ClusterStartupEpoch,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// Tracked collection state.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
}

impl<T> ReplicaTask<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Asynchronously forwards commands to and responses from a single replica.
    async fn run(self) {
        let replica_id = self.replica_id;
        info!(replica = ?replica_id, "starting replica task");

        let client = self.connect().await;
        match self.run_message_loop(client).await {
            Ok(()) => info!(replica = ?replica_id, "stopped replica task"),
            Err(error) => warn!(replica = ?replica_id, "replica task failed: {error}"),
        }
    }

    /// Connects to the replica.
    ///
    /// The connection is retried forever (with backoff) and this method returns only after
    /// a connection was successfully established.
    async fn connect(&self) -> ReplicaClient<T> {
        Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(|state| {
                let addrs = &self.config.location.ctl_addrs;
                let dests = addrs
                    .iter()
                    .map(|addr| (addr.clone(), self.metrics.clone()))
                    .collect();
                let version = self.build_info.semver_version();
                let client_params = &self.config.grpc_client;

                async move {
                    match ComputeGrpcClient::connect_partitioned(dests, version, client_params)
                        .await
                    {
                        Ok(client) => Ok(client),
                        Err(e) => {
                            if state.i >= mz_service::retry::INFO_MIN_RETRIES {
                                info!(
                                    replica = ?self.replica_id,
                                    "error connecting to replica, retrying in {:?}: {e}",
                                    state.next_backoff.unwrap()
                                );
                            } else {
                                debug!(
                                    replica = ?self.replica_id,
                                    "error connecting to replica, retrying in {:?}: {e}",
                                    state.next_backoff.unwrap()
                                );
                            }
                            Err(e)
                        }
                    }
                }
            })
            .await
            .expect("retry retries forever")
    }

    /// Runs the message loop.
    ///
    /// Returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
    /// If no error condition is encountered, the task runs until the controller disconnects from
    /// the command channel, or the task is dropped.
    async fn run_message_loop(mut self, mut client: ReplicaClient<T>) -> Result<(), anyhow::Error>
    where
        T: Timestamp + Lattice,
        ComputeGrpcClient: ComputeClient<T>,
    {
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

                    if self.response_tx.send(response).is_err() {
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
        if let ComputeCommand::CreateInstance(InstanceConfig {
            logging_config,
            variable_length_row_encoding: _,
        }) = command
        {
            *logging_config = self.config.logging.clone();
        }

        if let ComputeCommand::CreateTimely { config, epoch } = command {
            *config = TimelyConfig {
                workers: self.config.location.workers,
                process: 0,
                addresses: self.config.location.dataflow_addrs.clone(),
                idle_arrangement_merge_effort: self.config.idle_arrangement_merge_effort,
            };
            *epoch = self.epoch;
        }
    }

    /// Update task state according to an observed command.
    fn observe_command(&mut self, command: &ComputeCommand<T>) {
        trace!(
            replica = ?self.replica_id,
            command = ?command,
            "sending command to replica",
        );

        self.metrics.inner.command_queue_size.dec();

        // Initialize or drop per-collection state.
        match command {
            ComputeCommand::CreateDataflow(dataflow) => {
                for id in dataflow.export_ids() {
                    let metrics = self.metrics.for_collection(id);
                    let state = CollectionState {
                        metrics,
                        created_at: Instant::now(),
                        as_of: dataflow.as_of.clone().unwrap(),
                    };
                    self.collections.insert(id, state);
                }
            }
            ComputeCommand::AllowCompaction { id, frontier } if frontier.is_empty() => {
                self.collections.remove(id);
            }
            _ => (),
        }
    }

    /// Update task state according to an observed response.
    fn observe_response(&mut self, response: &ComputeResponse<T>) {
        trace!(
            replica = ?self.replica_id,
            response = ?response,
            "received response from replica",
        );

        self.metrics.inner.response_queue_size.inc();

        // Apply changes to the `initial_output_duration_seconds` metric.
        let collection_frontier = match response {
            ComputeResponse::FrontierUpper { id, upper } => Some((id, upper.clone())),
            ComputeResponse::SubscribeResponse(id, resp) => match resp {
                SubscribeResponse::Batch(batch) => Some((id, batch.upper.clone())),
                SubscribeResponse::DroppedAt(_) => Some((id, Antichain::new())),
            },
            _ => None,
        };
        if let Some((id, frontier)) = collection_frontier {
            if let Some(state) = self.collections.get(id) {
                state.observe_frontier_update(&frontier);
            }
        }
    }
}

/// State tracked for a compute collection.
struct CollectionState<T> {
    /// Metrics tracked for this collection.
    ///
    /// If this is `None`, no metrics are collected.
    metrics: Option<ReplicaCollectionMetrics>,
    /// Time at which this collection was installed.
    created_at: Instant,
    /// Original as_of of this collection.
    as_of: Antichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Apply the given frontier update to the collection metrics.
    fn observe_frontier_update(&self, frontier: &Antichain<T>) {
        let Some(metrics) = &self.metrics else { return };

        // If the value of `initial_output_duration_seconds` is greater than 0, that means we have
        // already observed the output before and have nothing else to do.
        let initial_output_duration = metrics.initial_output_duration_seconds.get();
        if initial_output_duration > 0. {
            return;
        }

        if PartialOrder::less_than(&self.as_of, frontier) {
            let duration = self.created_at.elapsed().as_secs_f64();
            metrics.initial_output_duration_seconds.set(duration);
        }
    }
}
