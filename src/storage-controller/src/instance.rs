// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller for a storage instance.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroI64;
use std::time::Duration;

use anyhow::bail;
use differential_dataflow::lattice::Lattice;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterReplicaLocation, ClusterStartupEpoch, TimelyConfig};
use mz_cluster_client::ReplicaId;
use mz_ore::now::NowFn;
use mz_ore::retry::{Retry, RetryState};
use mz_ore::task::AbortOnDropHandle;
use mz_repr::GlobalId;
use mz_service::client::{GenericClient, Partitioned};
use mz_service::params::GrpcClientParameters;
use mz_storage_client::client::{
    Status, StatusUpdate, StorageClient, StorageCommand, StorageGrpcClient, StorageResponse,
};
use mz_storage_client::metrics::{InstanceMetrics, ReplicaMetrics};
use timely::progress::Timestamp;
use tokio::select;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::history::CommandHistory;

/// A controller for a storage instance.
///
/// Encapsulates communication with replicas in this instance, and their rehydration.
///
/// Note that storage objects (sources and sinks) don't currently support replication (database-issues#5051).
/// An instance can have muliple replicas connected, but only if it has no storage objects
/// installed. Attempting to install storage objects on multi-replica instances, or attempting to
/// add more than one replica to instances that have storage objects installed, is illegal and will
/// lead to panics.
#[derive(Debug)]
pub(crate) struct Instance<T> {
    /// The replicas connected to this storage instance.
    replicas: BTreeMap<ReplicaId, Replica<T>>,
    /// The ingestions currently running on this instance.
    ///
    /// While this is derivable from `history` on demand, keeping a denormalized
    /// list of running ingestions is quite a bit more convenient in the
    /// implementation of `StorageController::active_ingestions`.
    active_ingestions: BTreeSet<GlobalId>,
    /// The command history, used to replay past commands when introducing new replicas or
    /// reconnecting to existing replicas.
    history: CommandHistory<T>,
    /// The current cluster startup epoch.
    ///
    /// The `replica` value of the epoch is increased every time a replica is (re)connected,
    /// allowing the distinction of different replica incarnations.
    epoch: ClusterStartupEpoch,
    /// Metrics tracked for this storage instance.
    metrics: InstanceMetrics,
    /// A function that returns the current time.
    now: NowFn,
    /// A sender for responses from replicas.
    response_tx: mpsc::UnboundedSender<StorageResponse<T>>,
}

impl<T> Instance<T>
where
    T: Timestamp + Lattice,
    StorageGrpcClient: StorageClient<T>,
{
    /// Creates a new [`Instance`].
    pub fn new(
        envd_epoch: NonZeroI64,
        metrics: InstanceMetrics,
        now: NowFn,
        instance_response_tx: mpsc::UnboundedSender<StorageResponse<T>>,
    ) -> Self {
        let history = CommandHistory::new(metrics.for_history());
        let epoch = ClusterStartupEpoch::new(envd_epoch, 0);

        let mut instance = Self {
            replicas: Default::default(),
            active_ingestions: BTreeSet::new(),
            history,
            epoch,
            metrics,
            now,
            response_tx: instance_response_tx,
        };

        instance.send(StorageCommand::CreateTimely {
            config: TimelyConfig::default(),
            epoch,
        });

        instance
    }

    /// Returns the IDs of all replicas connected to this storage instance.
    pub fn replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Adds a new replica to this storage instance.
    ///
    /// # Panics
    ///
    /// Panics if this storage instance has storage objects installed and already has a replica
    /// connected.
    pub fn add_replica(&mut self, id: ReplicaId, config: ReplicaConfig) {
        // Reduce the history to limit the amount of commands sent to the new replica, and to
        // enable the `objects_installed` assert below.
        self.history.reduce();

        let objects_installed = self.history.iter().any(|cmd| cmd.installs_objects());
        assert!(
            !objects_installed || self.replicas.is_empty(),
            "replication not supported for storage objects",
        );

        self.epoch.bump_replica();
        let metrics = self.metrics.for_replica(id);
        let replica = Replica::new(id, config, self.epoch, metrics, self.response_tx.clone());

        // Replay the commands at the new replica.
        for command in self.history.iter() {
            replica.send(command.clone());
        }

        self.replicas.insert(id, replica);
    }

    /// Removes the identified replica from this storage instance.
    pub fn drop_replica(&mut self, id: ReplicaId) {
        let replica = self.replicas.remove(&id);

        if replica.is_some() && self.replicas.is_empty() {
            self.update_paused_statuses();
        }
    }

    /// Rehydrates any failed replicas of this storage instance.
    pub fn rehydrate_failed_replicas(&mut self) {
        let replicas = self.replicas.iter();
        let failed_replicas: Vec<_> = replicas
            .filter_map(|(id, replica)| replica.failed().then_some(*id))
            .collect();

        for id in failed_replicas {
            let replica = self.replicas.remove(&id).expect("must exist");
            self.add_replica(id, replica.config);
        }
    }

    /// Returns the ingestions running on this instance.
    pub fn active_ingestions(&self) -> &BTreeSet<GlobalId> {
        &self.active_ingestions
    }

    /// Sets the status to paused for all sources/sinks in the history.
    fn update_paused_statuses(&mut self) {
        let now = mz_ore::now::to_datetime((self.now)());
        let make_update = |id, object_type| StatusUpdate {
            id,
            status: Status::Paused,
            timestamp: now,
            error: None,
            hints: BTreeSet::from([format!(
                "There is currently no replica running this {object_type}"
            )]),
            namespaced_errors: Default::default(),
        };

        self.history.reduce();

        let mut status_updates = Vec::new();
        for command in self.history.iter() {
            match command {
                StorageCommand::RunIngestions(cmds) => {
                    let updates = cmds.iter().map(|c| make_update(c.id, "source"));
                    status_updates.extend(updates);
                }
                StorageCommand::RunSinks(cmds) => {
                    let updates = cmds.iter().map(|c| make_update(c.id, "sink"));
                    status_updates.extend(updates);
                }
                _ => (),
            }
        }

        if !status_updates.is_empty() {
            let response = StorageResponse::StatusUpdates(status_updates);
            let _ = self.response_tx.send(response);
        }
    }

    /// Sends a command to this storage instance.
    ///
    /// # Panics
    ///
    /// Panics if the storage instance has multiple replicas connected and the given command
    /// instructs the installation of storage objects.
    pub fn send(&mut self, command: StorageCommand<T>) {
        assert!(
            !command.installs_objects() || self.replicas.len() <= 1,
            "replication not supported for storage objects"
        );

        match &command {
            StorageCommand::RunIngestions(ingestions) => {
                for ingestion in ingestions {
                    self.active_ingestions.insert(ingestion.id);
                }
            }
            StorageCommand::AllowCompaction(policies) => {
                for (id, frontier) in policies {
                    if frontier.is_empty() {
                        self.active_ingestions.remove(id);
                    }
                }
            }
            _ => (),
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(command.clone());

        // Clone the command for each active replica.
        for replica in self.replicas.values_mut() {
            replica.send(command.clone());
        }

        if command.installs_objects() && self.replicas.is_empty() {
            self.update_paused_statuses();
        }
    }
}

/// Replica-specific configuration.
#[derive(Clone, Debug)]
pub(super) struct ReplicaConfig {
    pub build_info: &'static BuildInfo,
    pub location: ClusterReplicaLocation,
    pub grpc_client: GrpcClientParameters,
}

/// State maintained about individual replicas.
#[derive(Debug)]
pub struct Replica<T> {
    /// Replica configuration.
    config: ReplicaConfig,
    /// A sender for commands for the replica.
    ///
    /// If sending to this channel fails, the replica has failed and requires
    /// rehydration.
    command_tx: mpsc::UnboundedSender<StorageCommand<T>>,
    /// A handle to the task that aborts it when the replica is dropped.
    task: AbortOnDropHandle<()>,
}

impl<T> Replica<T>
where
    T: Timestamp + Lattice,
    StorageGrpcClient: StorageClient<T>,
{
    /// Creates a new [`Replica`].
    fn new(
        id: ReplicaId,
        config: ReplicaConfig,
        epoch: ClusterStartupEpoch,
        metrics: ReplicaMetrics,
        response_tx: mpsc::UnboundedSender<StorageResponse<T>>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let task = mz_ore::task::spawn(
            || "storage-replica-{id}",
            ReplicaTask {
                replica_id: id,
                config: config.clone(),
                epoch,
                metrics: metrics.clone(),
                command_rx,
                response_tx,
            }
            .run(),
        );

        Self {
            config,
            command_tx,
            task: task.abort_on_drop(),
        }
    }

    /// Sends a command to the replica.
    fn send(&self, command: StorageCommand<T>) {
        // Send failures ignored, we'll check for failed replicas separately.
        let _ = self.command_tx.send(command);
    }

    /// Determine if this replica has failed. This is true if the replica
    /// task has terminated.
    fn failed(&self) -> bool {
        self.task.is_finished()
    }
}

type ReplicaClient<T> = Partitioned<StorageGrpcClient, StorageCommand<T>, StorageResponse<T>>;

/// A task handling communication with a replica.
struct ReplicaTask<T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// Replica configuration.
    config: ReplicaConfig,
    /// The epoch identifying this incarnation of the replica.
    epoch: ClusterStartupEpoch,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: mpsc::UnboundedReceiver<StorageCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: mpsc::UnboundedSender<StorageResponse<T>>,
}

impl<T> ReplicaTask<T>
where
    T: Timestamp + Lattice,
    StorageGrpcClient: StorageClient<T>,
{
    /// Runs the replica task.
    async fn run(self) {
        let replica_id = self.replica_id;
        info!(%replica_id, "starting replica task");

        let client = self.connect().await;
        match self.run_message_loop(client).await {
            Ok(()) => info!(%replica_id, "stopped replica task"),
            Err(error) => warn!(%replica_id, %error, "replica task failed"),
        }
    }

    /// Connects to the replica.
    ///
    /// The connection is retried forever (with backoff) and this method returns only after
    /// a connection was successfully established.
    async fn connect(&self) -> ReplicaClient<T> {
        let try_connect = |retry: RetryState| {
            let addrs = &self.config.location.ctl_addrs;
            let dests = addrs
                .iter()
                .map(|addr| (addr.clone(), self.metrics.clone()))
                .collect();
            let version = self.config.build_info.semver_version();
            let client_params = &self.config.grpc_client;

            async move {
                StorageGrpcClient::connect_partitioned(dests, version, client_params)
                    .await
                    .inspect_err(|error| {
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

        Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(try_connect)
            .await
            .expect("retries forever")
    }

    /// Runs the message loop.
    ///
    /// Returns (with an `Err`) if it encounters an error condition (e.g. the replica disconnects).
    /// If no error condition is encountered, the task runs until the controller disconnects from
    /// the command channel, or the task is dropped.
    async fn run_message_loop(mut self, mut client: ReplicaClient<T>) -> Result<(), anyhow::Error> {
        loop {
            select! {
                // Command from controller to forward to replica.
                // `tokio::sync::mpsc::UnboundedReceiver::recv` is documented as cancel safe.
                command = self.command_rx.recv() => {
                    let Some(mut command) = command else {
                        // Controller is no longer interested in this replica. Shut down.
                        break;
                    };

                    self.specialize_command(&mut command);
                    client.send(command).await?;
                },
                // Response from replica to forward to controller.
                // `GenericClient::recv` implementations are required to be cancel safe.
                response = client.recv() => {
                    let Some(response) = response? else {
                        bail!("replica unexpectedly gracefully terminated connection");
                    };

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
    /// Most [`StorageCommand`]s are independent of the target replica, but some contain
    /// replica-specific fields that must be adjusted before sending.
    fn specialize_command(&self, command: &mut StorageCommand<T>) {
        if let StorageCommand::CreateTimely { config, epoch } = command {
            *config = TimelyConfig {
                workers: self.config.location.workers,
                // Overridden by the storage `PartitionedState` implementation.
                process: 0,
                addresses: self.config.location.dataflow_addrs.clone(),
                // This value is not currently used by storage, so we just choose
                // some identifiable value.
                arrangement_exert_proportionality: 1337,
            };
            *epoch = self.epoch;
        }
    }
}
