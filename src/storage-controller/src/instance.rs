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
use itertools::Itertools;
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
    RunIngestionCommand, Status, StatusUpdate, StorageClient, StorageCommand, StorageGrpcClient,
    StorageResponse,
};
use mz_storage_client::metrics::{InstanceMetrics, ReplicaMetrics};
use mz_storage_types::sources::SourceConnection;
use timely::progress::{Antichain, Timestamp};
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
    active_ingestions: BTreeMap<GlobalId, ActiveIngestion>,
    /// A map from ingestion export ID to the ingestion that is producing it.
    ingestion_exports: BTreeMap<GlobalId, GlobalId>,
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

#[derive(Debug)]
struct ActiveIngestion {
    /// Whether the ingestion prefers running on a single replica.
    prefers_single_replica: bool,

    /// The set of replicas that this ingestion is currently running on.
    active_replicas: BTreeSet<ReplicaId>,
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
            active_ingestions: Default::default(),
            ingestion_exports: Default::default(),
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
    pub fn add_replica(&mut self, id: ReplicaId, config: ReplicaConfig) {
        // Reduce the history to limit the amount of commands sent to the new replica, and to
        // enable the `objects_installed` assert below.
        self.history.reduce();

        self.epoch.bump_replica();
        let metrics = self.metrics.for_replica(id);
        let replica = Replica::new(id, config, self.epoch, metrics, self.response_tx.clone());

        // Replay the commands at the new replica.
        for command in self.history.iter() {
            // replica.send(command.clone());
            match command.clone() {
                StorageCommand::RunIngestions(_) => {
                    // Ingestions are handled by schedule_ingestions below.
                }
                StorageCommand::AllowCompaction(_) => {
                    // Compactions are handled by send_compactions below.
                }
                command => {
                    replica.send(command.clone());
                }
            }
        }

        self.replicas.insert(id, replica);

        self.schedule_ingestions();

        let compactions = self
            .history
            .iter()
            .flat_map(|cmd| {
                if let StorageCommand::AllowCompaction(cmds) = cmd {
                    cmds.clone()
                } else {
                    Vec::new()
                }
            })
            .collect();
        self.send_compactions(&compactions);
    }

    /// Removes the identified replica from this storage instance.
    pub fn drop_replica(&mut self, id: ReplicaId) {
        let replica = self.replicas.remove(&id);

        let mut needs_rescheduling = false;
        for (ingestion_id, ingestion) in self.active_ingestions.iter_mut() {
            let was_running = ingestion.active_replicas.remove(&id);
            if was_running {
                tracing::debug!(
                    %ingestion_id,
                    replica_id = %id,
                    "ingestion was running on dropped replica, updating scheduling decisions"
                );
                needs_rescheduling = true;
            }
        }

        if needs_rescheduling {
            self.schedule_ingestions();
        }

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
    pub fn active_ingestions(&self) -> impl Iterator<Item = &GlobalId> {
        self.active_ingestions.keys()
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
    pub fn send(&mut self, command: StorageCommand<T>) {
        // Record the command so that new replicas can be brought up to speed.
        self.history.push(command.clone());

        match command.clone() {
            StorageCommand::RunIngestions(ingestions) => {
                self.absorb_ingestions(ingestions);
            }
            StorageCommand::AllowCompaction(cmds) => {
                self.absorb_compactions(cmds);
            }
            command => {
                for replica in self.replicas.values_mut() {
                    replica.send(command.clone());
                }
            }
        }

        if command.installs_objects() && self.replicas.is_empty() {
            self.update_paused_statuses();
        }
    }

    /// Updates internal state based on incoming ingestion commands.
    ///
    /// If a command updates an existing ingestion, it is sent out to the same
    /// replicas that are already running that ingestion. Otherwise, we only
    /// record the ingestion and leave scheduling up to
    /// [`schedule_ingestions`](Self::schedule_ingestions).
    fn absorb_ingestions(&mut self, ingestions: Vec<RunIngestionCommand>) {
        for ingestion in ingestions {
            let prefers_single_replica = ingestion
                .description
                .desc
                .connection
                .prefers_single_replica();

            let existing_ingestion_state = self.active_ingestions.get_mut(&ingestion.id);

            if let Some(ingestion_state) = existing_ingestion_state {
                assert!(
                    ingestion_state.prefers_single_replica == prefers_single_replica,
                    "single-replica preference changed"
                );
                tracing::debug!(
                    ingestion_id = %ingestion.id,
                    active_replicas = %ingestion_state.active_replicas.iter().map(|id| id.to_string()).join(", "),
                    "updating ingestion"
                );
                for id in ingestion.description.source_exports.keys() {
                    self.ingestion_exports.insert(id.clone(), ingestion.id);
                }

                // It's an update for an existing ingestion. So we need to send
                // it out to the same replicas that are already running that
                // ingestion. We don't need to change anything about our
                // scheduling decisions, no need to update active_ingestions.
                for active_replica_id in ingestion_state.active_replicas.iter() {
                    let active_replica = self
                        .replicas
                        .get_mut(active_replica_id)
                        .expect("missing replica");
                    active_replica.send(StorageCommand::RunIngestions(vec![ingestion.clone()]));
                }
            } else {
                // We create a new ingestion state for this ingestion and then defer to schedule_ingestions.
                let ingestion_state = ActiveIngestion {
                    prefers_single_replica,
                    active_replicas: BTreeSet::new(),
                };
                self.active_ingestions.insert(ingestion.id, ingestion_state);

                for id in ingestion.description.source_exports.keys() {
                    self.ingestion_exports.insert(id.clone(), ingestion.id);
                }

                // Maybe update scheduling decisions.
                self.schedule_ingestions();
            }
        }
    }

    /// Schedule ingestions on replicas, if needed.
    ///
    /// Single-replica ingestions are scheduled on the last replica, if it's not
    /// already running, the rationale being that the latest replica is the one
    /// that was created to take over load from other replicas. Crucially, we
    /// never change the scheduling decision for single-replica ingestions
    /// unless we have to, that is unless the replica that they are running on
    /// goes away. We do this, so that we don't send a mix of "run"/"allow
    /// compaction"/"run" messages to replicas, which wouldn't deal well with
    /// this.
    ///
    /// For multi-replica ingestions, we ensure that each active ingestion is
    /// scheduled on all replicas.
    fn schedule_ingestions(&mut self) {
        // Ensure the command history is reduced before scheduling.
        self.history.reduce();

        for (ingestion_id, ingestion_state) in self.active_ingestions.iter_mut() {
            // Look up the corresponding RunIngestionCommand in the command history.
            let mut run_ingestion_cmd = None;
            for command in self.history.iter() {
                if let StorageCommand::RunIngestions(cmds) = command {
                    for cmd in cmds {
                        if cmd.id == *ingestion_id {
                            run_ingestion_cmd = Some(cmd.clone());
                        }
                    }
                }
            }

            let run_ingestion_cmd = match run_ingestion_cmd {
                Some(cmd) => cmd,
                None => {
                    panic!(
                        "missing ingestion command in history for ingestion id {:?}",
                        ingestion_id
                    );
                }
            };

            if ingestion_state.prefers_single_replica {
                // For single-replica ingestion, schedule only if it's not already running.
                if ingestion_state.active_replicas.is_empty() {
                    if let Some(last_replica_id) = self.replicas.keys().cloned().last() {
                        ingestion_state.active_replicas.insert(last_replica_id);
                        if let Some(replica) = self.replicas.get_mut(&last_replica_id) {
                            tracing::debug!(
                                ingestion_id = %ingestion_id,
                                replica_id = %last_replica_id,
                                "scheduling single-replica ingestion");
                            replica.send(StorageCommand::RunIngestions(vec![
                                run_ingestion_cmd.clone()
                            ]));
                        }
                    }
                } else {
                    tracing::debug!(
                        %ingestion_id,
                        active_replicas = %ingestion_state.active_replicas.iter().map(|id| id.to_string()).join(", "),
                        "single-replica ingestion already running, not scheduling again",
                    );
                }
            } else {
                // For multi-replica ingestion, ensure all replicas have received the command.
                let current_replica_ids: BTreeSet<_> = self.replicas.keys().copied().collect();
                let unscheduled_replicas: Vec<_> = current_replica_ids
                    .difference(&ingestion_state.active_replicas)
                    .copied()
                    .collect();
                for replica_id in unscheduled_replicas {
                    if let Some(replica) = self.replicas.get_mut(&replica_id) {
                        tracing::debug!(
                            %ingestion_id,
                            %replica_id,
                            "scheduling multi-replica ingestion"
                        );
                        replica.send(StorageCommand::RunIngestions(vec![
                            run_ingestion_cmd.clone()
                        ]));
                        ingestion_state.active_replicas.insert(replica_id);
                    }
                }
            }
        }
    }

    /// Updates internal state based on incoming compaction commands. Also sends
    /// out compaction commands to replicas.
    fn absorb_compactions(&mut self, cmds: Vec<(GlobalId, Antichain<T>)>) {
        tracing::debug!(?self.active_ingestions, ?cmds, "allow_compaction");

        self.send_compactions(&cmds);

        for (id, frontier) in cmds.iter() {
            if frontier.is_empty() {
                self.active_ingestions.remove(id);
                self.ingestion_exports.remove(id);
            }
        }
    }

    fn send_compactions(&mut self, cmds: &Vec<(GlobalId, Antichain<T>)>) {
        for (id, frontier) in cmds.iter() {
            let ingestion_id = self.ingestion_exports.get(id);

            if let Some(ingestion_id) = ingestion_id {
                // For ingestions, we send the compaction command only to
                // replicas that are actively running the ingestion. This is
                // relevant for single-replica ingestions.
                let active_replicas =
                    if let Some(ingestion) = self.active_ingestions.get(ingestion_id) {
                        ingestion.active_replicas.iter()
                    } else {
                        // The ingestion has already been compacted away (aka.
                        // stopped), so we don't need to send compaction
                        // commands to its exports anymore.
                        continue;
                    };

                for active_replica_id in active_replicas {
                    let active_replica = self
                        .replicas
                        .get_mut(active_replica_id)
                        .expect("missing replica");
                    active_replica.send(StorageCommand::AllowCompaction(vec![(
                        id.clone(),
                        frontier.clone(),
                    )]));
                }
            } else {
                for (_id, replica) in self.replicas.iter_mut() {
                    replica.send(StorageCommand::AllowCompaction(vec![(
                        id.clone(),
                        frontier.clone(),
                    )]));
                }
            }
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
