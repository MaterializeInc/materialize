// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client backed by multiple replicas.
//!
//! This client accepts commands and responds as would a correctly implemented client.
//! Its implementation is wrapped around clients that may fail at any point, and restart.
//! To accommodate this, it records the commands it accepts, and should a client restart
//! the commands are replayed at it, with some modification. As the clients respond, the
//! wrapper client tracks the responses and ensures that they are "logically deduplicated",
//! so that the receiver need not be aware of the replication and restarting.
//!
//! This tactic requires that dataflows be restartable, which they generally are not, due
//! to allowed compaction of their source data. This client must correctly observe commands
//! that allow for compaction of its assets, and only attempt to rebuild them as of those
//! compacted frontiers, as the underlying resources to rebuild them any earlier may not
//! exist any longer.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use anyhow::bail;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_ore::retry::Retry;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;

use crate::command::{CommunicationConfig, ComputeCommand, ComputeCommandHistory, Peek, ReplicaId};
use crate::logging::LoggingConfig;
use crate::response::{ComputeResponse, PeekResponse, SubscribeBatch, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};

/// Configuration for `replica_task`.
struct ReplicaTaskConfig<T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// The network addresses of the processes in the replica.
    addrs: Vec<String>,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: UnboundedSender<ComputeResponse<T>>,
}

/// Asynchronously forwards commands to and responses from a single replica.
async fn replica_task<T>(config: ReplicaTaskConfig<T>)
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let replica_id = config.replica_id;
    info!("starting replica task for {replica_id}");
    match run_replica_core(config).await {
        Ok(()) => info!("gracefully stopping replica task for {replica_id}"),
        Err(e) => warn!("replica task for {replica_id} failed: {e}"),
    }
}

async fn run_replica_core<T>(
    ReplicaTaskConfig {
        replica_id,
        addrs,
        build_info,
        mut command_rx,
        response_tx,
    }: ReplicaTaskConfig<T>,
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
                        warn!(
                            "error connecting to replica {replica_id}, retrying in {:?}: {e}",
                            state.next_backoff.unwrap()
                        );
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
                None => {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(())
                }
                Some(command) => client.send(command).await?,
            },
            // Response from replica to forward to controller.
            response = client.recv() => {
                let response = match response? {
                    None => bail!("replica unexpectedly gracefully terminated connection"),
                    Some(response) => response,
                };
                if response_tx.send(response).is_err() {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(());
                }
            }
        }
    }
}

/// Additional information to store with pending peeks.
#[derive(Debug)]
struct PendingPeek {
    /// Replicas that have yet to respond to this peek.
    unfinished: BTreeSet<ReplicaId>,
    /// The OpenTelemetry context for this peek.
    ///
    /// This value is `Some` as long as we have not yet passed a response up the chain, and `None`
    /// afterwards.
    otel_ctx: Option<OpenTelemetryContext>,
}

impl PendingPeek {
    /// Return whether this peek is finished and can be cleaned up.
    fn is_finished(&self) -> bool {
        // If we have not yet emitted a response for the peek, the peek is not finished, even if
        // the set of replicas we are waiting for is currently empty. It might be that the cluster
        // has no replicas or all replicas have been temporarily removed for re-hydration. In this
        // case, we wait for new replicas to be added to eventually serve the peek.
        self.otel_ctx.is_none() && self.unfinished.is_empty()
    }
}

/// Reported upper frontiers for a single compute collection.
///
/// The type maintains the following invariants:
///   * replica frontiers only advance
///   * `upper_bound` only advances
///   * `upper_bound` is the upper bound of the frontiers of all replicas (active or not)
#[derive(Debug)]
struct ReportedUppers<T> {
    /// The reported uppers per replica.
    per_replica: HashMap<ReplicaId, Antichain<T>>,
    /// The upper bound of all reported uppers.
    upper_bound: Antichain<T>,
}

impl<T> ReportedUppers<T>
where
    T: Timestamp + Lattice,
{
    /// Construct a [`ReportedUppers`] that tracks frontiers of the given replicas.
    fn new(replica_ids: &BTreeSet<ReplicaId>, initial_frontier: Antichain<T>) -> Self {
        let per_replica = replica_ids
            .iter()
            .map(|id| (*id, initial_frontier.clone()))
            .collect();

        Self {
            per_replica,
            upper_bound: initial_frontier,
        }
    }

    /// Start tracking the given replica.
    ///
    /// # Panics
    /// - If the given `replica_id` is already tracked.
    /// - If `initial_frontier` is beyond the current `upper_bound`.
    fn add_replica(&mut self, id: ReplicaId, initial_frontier: Antichain<T>) {
        assert!(PartialOrder::less_equal(
            &initial_frontier,
            &self.upper_bound
        ));

        let previous = self.per_replica.insert(id, initial_frontier);
        assert!(previous.is_none(), "replica already tracked");
    }

    /// Stop tracking the given replica.
    ///
    /// Returns the final upper frontier reported by this replica.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn remove_replica(&mut self, id: ReplicaId) -> Antichain<T> {
        self.per_replica.remove(&id).expect("replica not tracked")
    }

    /// Apply a frontier update from a single replica.
    ///
    /// Returns the changes caused by this update.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn update(&mut self, replica_id: ReplicaId, new_upper: Antichain<T>) -> ChangeBatch<T> {
        let replica_upper = self
            .per_replica
            .get_mut(&replica_id)
            .expect("replica not tracked");

        let mut changes = ChangeBatch::new();

        // Replica frontiers only advance.
        if PartialOrder::less_than(replica_upper, &new_upper) {
            changes.extend(replica_upper.iter().map(|t| (t.clone(), -1)));
            changes.extend(new_upper.iter().map(|t| (t.clone(), 1)));
            replica_upper.clone_from(&new_upper);

            // `upper_bound` only advances.
            if PartialOrder::less_than(&self.upper_bound, &new_upper) {
                self.upper_bound = new_upper;
            }
        }

        changes
    }
}

/// The internal state of the client.
///
/// This lives in a separate struct from the handles to the individual replica
/// tasks, so that we can call methods on it
/// while holding mutable borrows to those.
#[derive(Debug)]
struct ActiveReplicationState<T> {
    /// IDs of connected replicas.
    replica_ids: BTreeSet<ReplicaId>,
    /// Outstanding peeks, to guide responses (and which to suppress).
    peeks: HashMap<uuid::Uuid, PendingPeek>,
    /// Frontiers of in-progress subscribes, to guide responses (and which to suppress).
    subscribes: HashMap<GlobalId, Antichain<T>>,
    /// Reported upper frontiers for replicated collections and in-progress subscribes.
    uppers: HashMap<GlobalId, ReportedUppers<T>>,
    /// Reported upper frontiers for log collections (both arranged and persisted).
    ///
    /// Log collections are special in that they don't have dependencies and therefore don't
    /// require per-replica frontier tracking.
    log_uppers: HashMap<GlobalId, Antichain<T>>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: ComputeCommandHistory<T>,
    /// Responses that should be emitted on the next `recv` call.
    ///
    /// This is introduced to produce peek cancelation responses eagerly, without awaiting a replica
    /// responding with the response itself, which allows us to compact away the peek in `self.history`.
    pending_response: VecDeque<ActiveReplicationResponse<T>>,
}

impl<T> ActiveReplicationState<T>
where
    T: Timestamp + Lattice,
{
    fn add_replica(&mut self, id: ReplicaId, mut initial_uppers: BTreeMap<GlobalId, Antichain<T>>) {
        self.replica_ids.insert(id);

        for (collection_id, uppers) in self.uppers.iter_mut() {
            let initial_upper = initial_uppers
                .remove(collection_id)
                .expect("initial upper missing for collection {collection_id}");
            uppers.add_replica(id, initial_upper);
        }
        for peek in self.peeks.values_mut() {
            peek.unfinished.insert(id);
        }
    }

    fn remove_replica(&mut self, id: ReplicaId) -> BTreeMap<GlobalId, Antichain<T>> {
        self.replica_ids.remove(&id);

        let mut final_frontiers = BTreeMap::new();
        for (collection_id, uppers) in self.uppers.iter_mut() {
            final_frontiers.insert(*collection_id, uppers.remove_replica(id));
        }

        // Removing a replica might implicitly finish a peek, which we must report up the chain.
        self.peeks.retain(|uuid, peek| {
            peek.unfinished.remove(&id);
            let finished = peek.is_finished();
            if finished {
                self.pending_response
                    .push_back(ActiveReplicationResponse::PeekFinished(uuid.clone()));
            }
            !finished
        });

        final_frontiers
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn handle_command(&mut self, cmd: &ComputeCommand<T>) {
        // Update our tracking of peek and subscribe commands.
        match &cmd {
            ComputeCommand::Peek(Peek {
                uuid,
                otel_ctx,
                target_replica,
                ..
            }) => {
                let unfinished = match target_replica {
                    Some(target) => [*target].into(),
                    None => self.replica_ids.clone(),
                };
                self.peeks.insert(
                    *uuid,
                    PendingPeek {
                        unfinished,
                        // TODO(guswynn): can we just hold the `tracing::Span`
                        // here instead?
                        otel_ctx: Some(otel_ctx.clone()),
                    },
                );
            }
            ComputeCommand::CancelPeeks { uuids } => {
                // Enqueue the response to the cancelation.
                for uuid in uuids {
                    let otel_ctx = self
                        .peeks
                        .get_mut(uuid)
                        // Canceled peeks should not be further responded to.
                        .map(|pending| pending.otel_ctx.take())
                        .unwrap_or_else(|| {
                            tracing::warn!("did not find pending peek for {}", uuid);
                            None
                        });
                    if let Some(ctx) = otel_ctx {
                        self.pending_response
                            .push_back(ActiveReplicationResponse::PeekResponse(
                                *uuid,
                                PeekResponse::Canceled,
                                ctx,
                            ));
                    }
                }
            }
            ComputeCommand::CreateDataflows(dataflows) => {
                let subscribe_ids = dataflows.iter().flat_map(|df| df.subscribe_ids());
                for id in subscribe_ids {
                    self.subscribes
                        .insert(id, Antichain::from_elem(Timestamp::minimum()));
                }
            }
            _ => {}
        }

        // Initialize any necessary frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        cmd.frontier_tracking(&mut start, &mut cease);
        for (id, initial_frontier) in start.into_iter() {
            self.start_frontier_tracking(id, initial_frontier);
        }
        for id in cease.into_iter() {
            self.cease_frontier_tracking(id);
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());
    }

    fn start_frontier_tracking(&mut self, id: GlobalId, initial_frontier: Option<Antichain<T>>) {
        let initial_frontier =
            initial_frontier.unwrap_or_else(|| Antichain::from_elem(Timestamp::minimum()));
        let uppers = ReportedUppers::new(&self.replica_ids, initial_frontier);
        let previous = self.uppers.insert(id, uppers);
        assert!(previous.is_none());
    }

    fn cease_frontier_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id);
        assert!(previous.is_some());

        // If we cease tracking an in-progress subscribe, we should emit a `DroppedAt` response.
        if let Some(subscribe_frontier) = self.subscribes.remove(&id) {
            self.pending_response
                .push_back(ActiveReplicationResponse::SubscribeResponse(
                    id,
                    SubscribeResponse::DroppedAt(subscribe_frontier),
                ));
        }
    }

    fn handle_response(
        &mut self,
        message: ComputeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        self.pending_response
            .push_front(ActiveReplicationResponse::ReplicaHeartbeat(
                replica_id,
                Utc::now(),
            ));
        match message {
            ComputeResponse::PeekResponse(uuid, response, otel_ctx) => {
                self.handle_peek_response(uuid, response, otel_ctx, replica_id)
            }
            ComputeResponse::FrontierUppers(list) => self.handle_frontier_uppers(list, replica_id),
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id)
            }
        }
    }

    fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let mut peek = match self.peeks.remove(&uuid) {
            Some(peek) => peek,
            None => {
                tracing::warn!("did not find pending peek for {}", uuid);
                return None;
            }
        };

        // If this is the first response, forward it; otherwise do not.
        // TODO: we could collect the other responses to assert equivalence?
        // Trades resources (memory) for reassurances; idk which is best.
        //
        // NOTE: we use the `otel_ctx` from the response, not the
        // pending peek, because we currently want the parent
        // to be whatever the compute worker did with this peek. We
        // still `take` the pending peek's `otel_ctx` to mark it as
        // served.
        //
        // Additionally, we just use the `otel_ctx` from the first worker to
        // respond.
        if peek.otel_ctx.take().is_some() {
            self.pending_response
                .push_back(ActiveReplicationResponse::PeekResponse(
                    uuid, response, otel_ctx,
                ));
        }

        // Update the per-replica tracking and draw appropriate consequences.
        peek.unfinished.remove(&replica_id);
        if peek.is_finished() {
            self.pending_response
                .push_back(ActiveReplicationResponse::PeekFinished(uuid));
        } else {
            // Put the pending peek back, to await further responses.
            self.peeks.insert(uuid, peek);
        }

        self.pending_response.pop_front()
    }

    fn handle_frontier_uppers(
        &mut self,
        list: Vec<(GlobalId, Antichain<T>)>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let mut new_uppers = Vec::new();

        for (id, new_upper) in list {
            if let Some(reported) = self.uppers.get_mut(&id) {
                let mut replica_changes = reported.update(replica_id, new_upper);
                if !replica_changes.is_empty() {
                    new_uppers.push((
                        id,
                        FrontierUppers {
                            global_upper: reported.upper_bound.clone(),
                            replica_changes,
                        },
                    ));
                }
            } else if let Some(reported) = self.log_uppers.get_mut(&id) {
                if PartialOrder::less_than(reported, &new_upper) {
                    reported.clone_from(&new_upper);
                    new_uppers.push((
                        id,
                        FrontierUppers {
                            global_upper: reported.clone(),
                            replica_changes: ChangeBatch::new(),
                        },
                    ));
                }
            }
        }

        if !new_uppers.is_empty() {
            Some(ActiveReplicationResponse::FrontierUppers(new_uppers))
        } else {
            None
        }
    }

    fn handle_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let entry = self.uppers.get_mut(&subscribe_id)?;

        match response {
            SubscribeResponse::Batch(SubscribeBatch {
                lower: _,
                upper,
                mut updates,
            }) => {
                // We track the upper frontiers reported by all replicas.
                //  * If the upper bound of reported frontiers advances, we can emit all updates at
                //    times greater or equal to the last reported upper bound (to avoid emitting
                //    duplicate updates) as a `SubscribeResponse`.
                //  * If any frontier advances, we emit this information as a `FrontierUppers`
                //    response.

                let mut replica_changes = entry.update(replica_id, upper.clone());
                if replica_changes.is_empty() {
                    // There are no new updates to report.
                    return None;
                }

                if let Some(lower) = self.subscribes.get_mut(&subscribe_id) {
                    if PartialOrder::less_than(lower, &entry.upper_bound) {
                        updates.retain(|(time, _data, _diff)| lower.less_equal(time));
                        self.pending_response.push_back(
                            ActiveReplicationResponse::SubscribeResponse(
                                subscribe_id,
                                SubscribeResponse::Batch(SubscribeBatch {
                                    lower: lower.clone(),
                                    upper: entry.upper_bound.clone(),
                                    updates,
                                }),
                            ),
                        );
                        *lower = entry.upper_bound.clone();
                    }
                }

                let frontier_updates = vec![(
                    subscribe_id,
                    FrontierUppers {
                        global_upper: entry.upper_bound.clone(),
                        replica_changes,
                    },
                )];
                self.pending_response
                    .push_back(ActiveReplicationResponse::FrontierUppers(frontier_updates));

                if upper.is_empty() {
                    // This subscribe has finished producing all its data. Remove it from the
                    // in-progress subscribes, so we don't emit a `DroppedAt` for it.
                    self.subscribes.remove(&subscribe_id);
                }
            }
            SubscribeResponse::DroppedAt(_) => {
                // We should never get here. A replica emits `DroppedAt` only in response to a
                // subscribe being dropped by its client (via `AllowCompaction`). When we handle
                // the `AllowCompaction` command, we cease tracking the subscribe's frontier. And
                // without a tracked frontier, we return immediately at the beginning of this
                // method.
                tracing::error!("unexpected `DroppedAt` received for subscribe {subscribe_id}");
            }
        }

        self.pending_response.pop_front()
    }
}

/// A client backed by multiple replicas.
#[derive(Debug)]
pub(super) struct ActiveReplication<T> {
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// State for each replica.
    replicas: HashMap<ReplicaId, ReplicaState<T>>,
    /// All other internal state of the client
    state: ActiveReplicationState<T>,
}

impl<T> ActiveReplication<T> {
    pub(super) fn new(build_info: &'static BuildInfo) -> Self {
        Self {
            build_info,
            replicas: Default::default(),
            state: ActiveReplicationState {
                replica_ids: Default::default(),
                peeks: Default::default(),
                subscribes: Default::default(),
                uppers: Default::default(),
                log_uppers: Default::default(),
                history: Default::default(),
                pending_response: Default::default(),
            },
        }
    }
}

/// State for a single replica.
#[derive(Debug)]
struct ReplicaState<T> {
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
    /// The network addresses of the processes that make up the replica.
    addrs: Vec<String>,
    /// The logging config specific to this replica.
    logging_config: Option<LoggingConfig>,
    /// The communication config specific to this replica.
    communication_config: CommunicationConfig,
}

impl<T> ReplicaState<T> {
    /// Specialize a command for the given `Replica` and `ReplicaId`.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command(&self, command: &mut ComputeCommand<T>, replica_id: ReplicaId) {
        // Set new replica ID and obtain set the sinked logs specific to this replica
        if let ComputeCommand::CreateInstance(config) = command {
            config.replica_id = replica_id;
            config.logging = self.logging_config.clone();
        }

        if let ComputeCommand::CreateTimely(comm_config) = command {
            *comm_config = self.communication_config.clone();
        }
    }
}

impl<T> ActiveReplication<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    pub(super) fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        logging_config: Option<LoggingConfig>,
        communication_config: CommunicationConfig,
        initial_uppers: BTreeMap<GlobalId, Antichain<T>>,
    ) {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        let task = mz_ore::task::spawn(
            || format!("active-replication-replica-{id}"),
            replica_task(ReplicaTaskConfig {
                replica_id: id,
                build_info: self.build_info,
                addrs: addrs.clone(),
                command_rx,
                response_tx,
            }),
        );

        // Take this opportunity to clean up the history we should present.
        self.state.history.retain_peeks(&self.state.peeks);
        self.state.history.reduce();

        let replica_state = ReplicaState {
            command_tx,
            response_rx,
            _task: task.abort_on_drop(),
            addrs,
            logging_config,
            communication_config,
        };

        // Replay the commands at the client, creating new dataflow identifiers.
        for command in self.state.history.iter() {
            let mut command = command.clone();
            replica_state.specialize_command(&mut command, id);
            if replica_state.command_tx.send(command).is_err() {
                // We swallow the error here. On the next send, we will fail again, and
                // restart the connection as well as this rehydration.
                tracing::warn!("Replica {:?} connection terminated during rehydration", id);
                break;
            }
        }

        if let Some(logging) = &replica_state.logging_config {
            // Start tracking frontiers of persisted log collections.
            for (collection_id, _) in logging.sink_logs.values() {
                let frontier = Antichain::from_elem(Timestamp::minimum());
                let previous = self.state.log_uppers.insert(*collection_id, frontier);
                assert!(previous.is_none());
            }

            // Start tracking frontiers of arranged log collections.
            // Their IDs are shared among replicas, so we may already be tracking them.
            for collection_id in logging.active_logs.values() {
                self.state
                    .log_uppers
                    .entry(*collection_id)
                    .or_insert_with(|| Antichain::from_elem(Timestamp::minimum()));
            }
        }

        // Add replica to tracked state.
        self.replicas.insert(id, replica_state);
        self.state.add_replica(id, initial_uppers);
    }

    /// Returns an iterator over the IDs of the replicas.
    pub(super) fn get_replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Remove a replica by its identifier.
    pub(super) fn remove_replica(&mut self, id: ReplicaId) -> BTreeMap<GlobalId, Antichain<T>> {
        let final_uppers = self.state.remove_replica(id);
        let replica_state = self.replicas.remove(&id).expect("replica not found");

        // Cease tracking frontiers of persisted log collections.
        if let Some(logging) = &replica_state.logging_config {
            for (collection_id, _) in logging.sink_logs.values() {
                let previous = self.state.log_uppers.remove(collection_id);
                assert!(previous.is_some());
            }
        }

        final_uppers
    }

    fn rehydrate_replica(&mut self, id: ReplicaId) {
        let addrs = self.replicas[&id].addrs.clone();
        let logging_config = self.replicas[&id].logging_config.clone();
        let communication_config = self.replicas[&id].communication_config.clone();
        let final_uppers = self.remove_replica(id);
        self.add_replica(
            id,
            addrs,
            logging_config,
            communication_config,
            final_uppers,
        );
    }

    // We avoid implementing `GenericClient` here, because the protocol between
    // the compute controller and this client is subtly but meaningfully different:
    // this client is expected to handle errors, rather than propagate them, and therefore
    // it returns infallible values.

    /// Sends a command to all replicas.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) fn send(&mut self, cmd: ComputeCommand<T>) {
        self.state.handle_command(&cmd);

        // Clone the command for each active replica.
        let mut failed_replicas = vec![];
        for (id, replica) in self.replicas.iter_mut() {
            let mut command = cmd.clone();
            replica.specialize_command(&mut command, *id);
            // If sending the command fails, the replica requires rehydration.
            if replica.command_tx.send(command).is_err() {
                failed_replicas.push(*id);
            }
        }
        for id in failed_replicas {
            self.rehydrate_replica(id);
        }
    }

    /// Receives the next response from any replica.
    ///
    /// This method is cancellation safe.
    pub(super) async fn recv(&mut self) -> ActiveReplicationResponse<T> {
        // If we have a pending response, we should send it immediately.
        if let Some(response) = self.state.pending_response.pop_front() {
            return response;
        }

        // Receive responses from any of the replicas, and take appropriate
        // action.
        loop {
            let mut responses = self
                .replicas
                .iter_mut()
                .map(|(id, replica)| async { (*id, replica.response_rx.recv().await) })
                .collect::<FuturesUnordered<_>>();
            match responses.next().await {
                None => {
                    // There were no replicas in the set. Block forever to
                    // communicate that no response is ready.
                    future::pending().await
                }
                Some((replica_id, None)) => {
                    // A replica has failed and requires rehydration.
                    drop(responses);
                    self.rehydrate_replica(replica_id)
                }
                Some((replica_id, Some(response))) => {
                    // A replica has produced a response. Absorb it, possibly
                    // returning a response up the stack.
                    match self.state.handle_response(response, replica_id) {
                        Some(response) => return response,
                        None => { /* continue */ }
                    }
                }
            }
        }
    }
}

/// A response from the ActiveReplication client.
#[derive(Debug, Clone)]
pub(super) enum ActiveReplicationResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new lower and upper bounds of upper frontiers.
    FrontierUppers(Vec<(GlobalId, FrontierUppers<T>)>),
    /// The compute instance's response to the specified peek.
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// A notification that all replicas have finished processing the specified peek.
    ///
    /// This is different from `PeekResponse`, because we respond to a peek immediately upon seeing
    /// the first response for it. `PeekFinished` reports that it is now allowed to release any
    /// read holds installed for the peek.
    PeekFinished(Uuid),
    /// The compute instance's next response to the specified subscribe.
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// A notification that we heard a response from the given replica at the
    /// given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

#[derive(Debug, Clone)]
pub(super) struct FrontierUppers<T> {
    pub global_upper: Antichain<T>,
    pub replica_changes: ChangeBatch<T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_upper_bound {
        ($uppers:expr, $upper_bound:expr) => {
            assert_eq!(
                $uppers.upper_bound,
                Antichain::from_elem($upper_bound),
                "upper_bound mismatch"
            );
        };
    }

    macro_rules! assert_changes {
        ($changes:expr, ($minus:expr, $plus:expr)) => {
            let mut expected = ChangeBatch::new();
            expected.update($minus, -1);
            expected.update($plus, 1);
            assert_eq!($changes, expected, "changes mismatch");
        };
    }

    #[test]
    fn reported_uppers() {
        let replica_ids = [1, 2].into();
        let initial_frontier = Antichain::from_elem(Timestamp::minimum());
        let mut uppers = ReportedUppers::<u64>::new(&replica_ids, initial_frontier);
        assert_upper_bound!(uppers, 0);

        let changes = uppers.update(1, Antichain::from_elem(1));
        assert_changes!(changes, (0, 1));
        assert_upper_bound!(uppers, 1);

        let changes = uppers.update(2, Antichain::from_elem(2));
        assert_changes!(changes, (0, 2));
        assert_upper_bound!(uppers, 2);

        // Frontiers can only advance.
        let mut changes = uppers.update(2, Antichain::from_elem(1));
        assert!(changes.is_empty());
        assert_upper_bound!(uppers, 2);
        assert_eq!(uppers.per_replica[&2], Antichain::from_elem(2));

        uppers.add_replica(3, Antichain::from_elem(1));
        assert_upper_bound!(uppers, 2);
        assert_eq!(uppers.per_replica[&3], Antichain::from_elem(1));

        let changes = uppers.update(3, Antichain::from_elem(3));
        assert_changes!(changes, (1, 3));
        assert_upper_bound!(uppers, 3);

        // Removing the slowest replica doesn't affect upper bound.
        let final_frontier = uppers.remove_replica(1);
        assert_upper_bound!(uppers, 3);
        assert_eq!(final_frontier, Antichain::from_elem(1));

        // Removing the fastest replica doesn't affect upper bound.
        let final_frontier = uppers.remove_replica(3);
        assert_upper_bound!(uppers, 3);
        assert_eq!(final_frontier, Antichain::from_elem(3));

        // Removing the last replica doesn't affect upper bound.
        let final_frontier = uppers.remove_replica(2);
        assert_upper_bound!(uppers, 3);
        assert_eq!(final_frontier, Antichain::from_elem(2));
    }
}
