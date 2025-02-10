// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller for a storage instance.

use crate::CollectionMetadata;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroI64;
use std::time::Duration;

use anyhow::bail;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterReplicaLocation, ClusterStartupEpoch, TimelyConfig};
use mz_cluster_client::ReplicaId;
use mz_dyncfg::ConfigValHandle;
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
use mz_storage_types::sources::{IngestionDescription, SourceConnection};
use timely::order::TotalOrder;
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
    /// The exports currently running on this instance.
    ///
    /// While this is derivable from `history` on demand, keeping a denormalized
    /// list of running exports is quite a bit more convenient for the
    /// controller.
    active_exports: BTreeSet<GlobalId>,
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
    ///
    /// Responses are tagged with the [`ReplicaId`] of the replica that sent the
    /// response. Responses that don't originate from a replica (e.g. a "paused"
    /// status update, when no replicas are connected) are tagged with `None`.
    response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse<T>)>,
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
    T: Timestamp + Lattice + TotalOrder,
    StorageGrpcClient: StorageClient<T>,
{
    /// Creates a new [`Instance`].
    pub fn new(
        envd_epoch: NonZeroI64,
        metrics: InstanceMetrics,
        now: NowFn,
        instance_response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse<T>)>,
        enable_snapshot_frontier: ConfigValHandle<bool>,
    ) -> Self {
        let history = CommandHistory::new(metrics.for_history(), enable_snapshot_frontier);
        let epoch = ClusterStartupEpoch::new(envd_epoch, 0);

        let mut instance = Self {
            replicas: Default::default(),
            active_ingestions: Default::default(),
            ingestion_exports: Default::default(),
            active_exports: BTreeSet::new(),
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

        self.replicas.insert(id, replica);

        self.update_ingestion_scheduling(false);

        self.replay_commands(id);
    }

    /// Replays commands to the specified replica.
    pub fn replay_commands(&mut self, replica_id: ReplicaId) {
        let commands = self.history.iter().cloned();

        let filtered_commands = commands
            .filter_map(|command| match command {
                StorageCommand::RunIngestions(mut cmds) => {
                    cmds.retain(|cmd| self.is_active_replica(&cmd.id, &replica_id));
                    if cmds.len() > 0 {
                        Some(StorageCommand::RunIngestions(cmds))
                    } else {
                        None
                    }
                }
                StorageCommand::AllowCompaction(mut cmds) => {
                    cmds.retain(|cmd| self.is_active_replica(&cmd.0, &replica_id));
                    if cmds.len() > 0 {
                        Some(StorageCommand::AllowCompaction(cmds))
                    } else {
                        None
                    }
                }
                command => Some(command),
            })
            .collect::<Vec<_>>();

        let replica = self
            .replicas
            .get_mut(&replica_id)
            .expect("replica must exist");

        // Replay the commands at the new replica.
        for command in filtered_commands {
            replica.send(command);
        }
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

        tracing::info!(%id, %needs_rescheduling, "dropped replica");

        if needs_rescheduling {
            self.update_ingestion_scheduling(true);
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

    /// Returns the exports running on this instance.
    pub fn active_exports(&self) -> &BTreeSet<GlobalId> {
        &self.active_exports
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
            replica_id: None,
        };

        self.history.reduce();

        let mut status_updates = Vec::new();
        for command in self.history.iter() {
            match command {
                StorageCommand::RunIngestions(cmds) => {
                    for ingestion in cmds.iter() {
                        // NOTE(aljoscha): We filter out the remap collection because we
                        // don't get any status updates about it from the replica side. So
                        // we don't want to synthesize a 'paused' status here.
                        //
                        // TODO(aljoscha): I think we want to fix this eventually, and make
                        // sure we get status updates for the remap shard as well. Currently
                        // its handling in the source status collection is a bit difficult
                        // because we don't have updates for it in the status history
                        // collection.
                        let subsource_ids = ingestion
                            .description
                            .collection_ids()
                            .filter(|id| id != &ingestion.description.remap_collection_id);
                        for id in subsource_ids {
                            status_updates.push(make_update(id, "source"));
                        }
                    }
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
            // NOTE: If we lift this "inject paused status" logic to the
            // controller, we could instead return ReplicaId instead of an
            // Option<ReplicaId>.
            let _ = self.response_tx.send((None, response));
        }
    }

    /// Sends a command to this storage instance.
    pub fn send(&mut self, command: StorageCommand<T>) {
        // Record the command so that new replicas can be brought up to speed.
        self.history.push(command.clone());

        match command.clone() {
            StorageCommand::RunIngestions(ingestions) => {
                // First absorb into our state, because this might change
                // scheduling decisions, which need to be respected just below
                // when sending commands.
                self.absorb_ingestions(ingestions.clone());

                for cmd in ingestions.iter() {
                    for replica in self.active_replicas(&cmd.id) {
                        replica.send(StorageCommand::RunIngestions(vec![cmd.clone()]));
                    }
                }
            }
            StorageCommand::RunSinks(sinks) => {
                for sink in sinks.iter() {
                    self.active_exports.insert(sink.id);
                }
                for replica in self.replicas.values_mut() {
                    replica.send(command.clone());
                }
            }
            StorageCommand::AllowCompaction(cmds) => {
                // First send out commands and then absorb into our state since
                // absorbing them might remove entries from active_ingestions.
                for (id, frontier) in cmds.iter() {
                    for replica in self.active_replicas(id) {
                        replica.send(StorageCommand::AllowCompaction(vec![(
                            id.clone(),
                            frontier.clone(),
                        )]));
                    }
                }

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
    /// This does _not_ send commands to replicas, we only record the ingestion
    /// in state and potentially update scheduling decisions.
    fn absorb_ingestions(&mut self, ingestions: Vec<RunIngestionCommand>) {
        for ingestion in ingestions {
            let prefers_single_replica = ingestion
                .description
                .desc
                .connection
                .prefers_single_replica();

            let existing_ingestion_state = self.active_ingestions.get_mut(&ingestion.id);

            // Always update our mapping from export to their ingestion.
            for id in ingestion.description.source_exports.keys() {
                self.ingestion_exports.insert(id.clone(), ingestion.id);
            }

            if let Some(ingestion_state) = existing_ingestion_state {
                // It's an update for an existing ingestion. We don't need to
                // change anything about our scheduling decisions, no need to
                // update active_ingestions.

                assert!(
                    ingestion_state.prefers_single_replica == prefers_single_replica,
                    "single-replica preference changed"
                );
                tracing::debug!(
                    ingestion_id = %ingestion.id,
                    active_replicas = %ingestion_state.active_replicas.iter().map(|id| id.to_string()).join(", "),
                    "updating ingestion"
                );
            } else {
                // We create a new ingestion state for this ingestion.
                let ingestion_state = ActiveIngestion {
                    prefers_single_replica,
                    active_replicas: BTreeSet::new(),
                };
                self.active_ingestions.insert(ingestion.id, ingestion_state);

                // Maybe update scheduling decisions.
                self.update_ingestion_scheduling(false);
            }
        }
    }

    /// Update scheduling decisions for ingestions, that is what replicas should
    /// be running a given ingestion, if needed.
    ///
    /// An important property of this scheduling algorithm is that we never
    /// change the scheduling decision for single-replica ingestions unless we
    /// have to, that is unless the replica that they are running on goes away.
    /// We do this, so that we don't send a mix of "run"/"allow
    /// compaction"/"run" messages to replicas, which wouldn't deal well with
    /// this. When we _do_ have to make a scheduling decision we schedule a
    /// single-replica ingestion on the first replica, according to the sort
    /// order of `ReplicaId`. We do this latter so that the scheduling decision
    /// is stable across restarts of `environmentd`/the controller.
    ///
    /// For multi-replica ingestions (e.g. Kafka), each active ingestion is
    /// scheduled on all replicas.
    ///
    /// If `send_commands` is true, will send commands for newly-scheduled
    /// single-replica ingestions.
    fn update_ingestion_scheduling(&mut self, send_commands: bool) {
        // We have to collect first and then send later, because we're borrowing
        // self mutably just below.
        let mut commands_by_replica: BTreeMap<ReplicaId, Vec<GlobalId>> = BTreeMap::new();

        for (ingestion_id, ingestion_state) in self.active_ingestions.iter_mut() {
            if ingestion_state.prefers_single_replica {
                // For single-replica ingestion, schedule only if it's not already running.
                if ingestion_state.active_replicas.is_empty() {
                    let first_replica = self.replicas.keys().min().copied();
                    if let Some(first_replica_id) = first_replica {
                        tracing::info!(
                            ingestion_id = %ingestion_id,
                            replica_id = %first_replica_id,
                            "scheduling single-replica ingestion");
                        ingestion_state.active_replicas.insert(first_replica_id);

                        commands_by_replica
                            .entry(first_replica_id)
                            .or_default()
                            .push(ingestion_id.clone());
                    }
                } else {
                    tracing::info!(
                        %ingestion_id,
                        active_replicas = %ingestion_state.active_replicas.iter().map(|id| id.to_string()).join(", "),
                        "single-replica ingestion already running, not scheduling again",
                    );
                }
            } else {
                let current_replica_ids: BTreeSet<_> = self.replicas.keys().copied().collect();
                let unscheduled_replicas: Vec<_> = current_replica_ids
                    .difference(&ingestion_state.active_replicas)
                    .copied()
                    .collect();
                for replica_id in unscheduled_replicas {
                    tracing::info!(
                        %ingestion_id,
                        %replica_id,
                        "scheduling multi-replica ingestion"
                    );
                    ingestion_state.active_replicas.insert(replica_id);
                }
            }
        }

        // Now send all collected commands
        if send_commands {
            for (replica_id, ingestion_ids) in commands_by_replica {
                let commands: Vec<RunIngestionCommand> = ingestion_ids
                    .into_iter()
                    .map(|id| RunIngestionCommand {
                        id,
                        description: self
                            .get_ingestion_description(&id)
                            .expect("missing ingestion description")
                            .clone(),
                    })
                    .collect();

                if !commands.is_empty() {
                    let replica = self.replicas.get_mut(&replica_id).expect("missing replica");
                    replica.send(StorageCommand::RunIngestions(commands));
                }
            }
        }
    }

    /// Returns the ingestion description for the given ingestion ID, if it
    /// exists.
    ///
    /// This function searches through the command history to find the most
    /// recent RunIngestionCommand for the specified ingestion ID and returns
    /// its description.  Returns None if no ingestion with the given ID is
    /// found.
    pub fn get_ingestion_description(
        &self,
        id: &GlobalId,
    ) -> Option<IngestionDescription<CollectionMetadata>> {
        if !self.active_ingestions.contains_key(id) {
            return None;
        }

        self.history.iter().rev().find_map(|command| {
            if let StorageCommand::RunIngestions(cmds) = command {
                cmds.iter()
                    .find(|cmd| &cmd.id == id)
                    .map(|cmd| cmd.description.clone())
            } else {
                None
            }
        })
    }

    /// Updates internal state based on incoming compaction commands.
    fn absorb_compactions(&mut self, cmds: Vec<(GlobalId, Antichain<T>)>) {
        tracing::debug!(?self.active_ingestions, ?cmds, "allow_compaction");

        for (id, frontier) in cmds.iter() {
            if frontier.is_empty() {
                self.active_ingestions.remove(id);
                self.ingestion_exports.remove(id);
                self.active_exports.remove(id);
            }
        }
    }

    /// Returns the replicas that are actively running the given object (ingestion or export).
    fn active_replicas(&mut self, id: &GlobalId) -> Box<dyn Iterator<Item = &mut Replica<T>> + '_> {
        if let Some(ingestion_id) = self.ingestion_exports.get(id) {
            // Right now, only ingestions can have per-replica scheduling
            // decisions.
            match self.active_ingestions.get(ingestion_id) {
                Some(ingestion) => {
                    let active_replicas = ingestion.active_replicas.clone();
                    Box::new(
                        self.replicas
                            .iter_mut()
                            .filter_map(move |(replica_id, replica)| {
                                if active_replicas.contains(replica_id) {
                                    Some(replica)
                                } else {
                                    None
                                }
                            }),
                    )
                }
                None => {
                    // The ingestion has already been compacted away (aka. stopped).
                    Box::new(std::iter::empty())
                }
            }
        } else {
            Box::new(self.replicas.values_mut())
        }
    }

    /// Returns whether the given replica is actively running the given object (ingestion or export).
    fn is_active_replica(&self, id: &GlobalId, replica_id: &ReplicaId) -> bool {
        if let Some(ingestion_id) = self.ingestion_exports.get(id) {
            // Right now, only ingestions can have per-replica scheduling
            // decisions.
            match self.active_ingestions.get(ingestion_id) {
                Some(ingestion) => ingestion.active_replicas.contains(replica_id),
                None => {
                    // The ingestion has already been compacted away (aka. stopped).
                    false
                }
            }
        } else {
            // For non-ingestion objects, all replicas are active
            true
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
        response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse<T>)>,
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
    response_tx: mpsc::UnboundedSender<(Option<ReplicaId>, StorageResponse<T>)>,
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
                        tracing::debug!(%self.replica_id, "controller is no longer interested in this replica, shutting down message loop");
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

                    if self.response_tx.send((Some(self.replica_id), response)).is_err() {
                        tracing::debug!(%self.replica_id, "controller (receiver) is no longer interested in this replica, shutting down message loop");
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
                // Disable zero-copy by default.
                // TODO: Bring in line with compute.
                enable_zero_copy: false,
                // Do not use lgalloc to back zero-copy memory.
                enable_zero_copy_lgalloc: false,
                // No limit; zero-copy is disabled.
                zero_copy_limit: None,
            };
            *epoch = self.epoch;
        }
    }
}
