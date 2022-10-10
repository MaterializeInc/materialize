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

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use anyhow::bail;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use timely::progress::{Antichain, Timestamp};
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

/// Additional information to store with pening peeks.
#[derive(Debug)]
struct PendingPeek {
    /// The OpenTelemetry context for this peek.
    otel_ctx: OpenTelemetryContext,
}

/// Reported upper frontiers for a single compute collection.
///
/// The type maintains the following invariants:
///   * replica frontiers only advance
///   * frontier bounds only advance
///   * `bounds.lower` <= `bounds.upper`
///   * `bounds.lower` is the lower bound of the frontiers of all active replicas
///   * `bounds.upper` is the upper bound of the frontiers of all replicas
#[derive(Debug)]
struct ReportedUppers<T> {
    /// The reported uppers per replica.
    per_replica: HashMap<ReplicaId, Antichain<T>>,
    /// The lower and upper bound of all reported uppers.
    bounds: FrontierBounds<T>,
}

impl<T> ReportedUppers<T>
where
    T: Timestamp + Lattice,
{
    /// Construct a [`ReportedUppers`] that tracks frontiers of the given replicas.
    fn new(replica_ids: &BTreeSet<ReplicaId>) -> Self {
        let per_replica = replica_ids
            .iter()
            .map(|id| (*id, Antichain::from_elem(T::minimum())))
            .collect();

        Self {
            per_replica,
            bounds: FrontierBounds {
                lower: Antichain::from_elem(T::minimum()),
                upper: Antichain::from_elem(T::minimum()),
            },
        }
    }

    /// Start tracking the given replica.
    ///
    /// # Panics
    /// - If the given `replica_id` is already tracked.
    fn add_replica(&mut self, id: ReplicaId) {
        let previous = self.per_replica.insert(id, self.bounds.lower.clone());
        assert!(previous.is_none(), "replica already tracked");
    }

    /// Stop tracking the given replica.
    ///
    /// Returns `true` iff the update caused a change in any of the two bounds.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn remove_replica(&mut self, id: ReplicaId) -> bool {
        self.per_replica.remove(&id).expect("replica not tracked");

        self.update_lower_bound()
    }

    /// Apply a frontier update from a single replica.
    ///
    /// Returns `true` iff the update caused a change in any of the two bounds.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn update(&mut self, replica_id: ReplicaId, new_upper: Antichain<T>) -> bool {
        let replica_upper = self
            .per_replica
            .get_mut(&replica_id)
            .expect("replica not tracked");

        // Replica frontiers only advance.
        if PartialOrder::less_than(&new_upper, replica_upper) {
            return false;
        }

        replica_upper.clone_from(&new_upper);

        let upper_bound_changed = PartialOrder::less_than(&self.bounds.upper, &new_upper);
        if upper_bound_changed {
            self.bounds.upper = new_upper;
        }

        let lower_bound_changed = self.update_lower_bound();

        upper_bound_changed || lower_bound_changed
    }

    /// Update `bounds.lower` to restore its invariants.
    ///
    /// Returns `true` iff the update caused a change in the lower bound.
    fn update_lower_bound(&mut self) -> bool {
        // This operation is linear in the number of replicas. We could do better, but since the
        // number of replicas is expected to be small, this is fine.
        let mut new_lower_bound = self.bounds.upper.clone();
        for frontier in self.per_replica.values() {
            new_lower_bound.meet_assign(frontier);
        }

        let lower_bound_changed = PartialOrder::less_than(&self.bounds.lower, &new_lower_bound);
        if lower_bound_changed {
            self.bounds.lower = new_lower_bound;
        }

        lower_bound_changed
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
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashMap<uuid::Uuid, PendingPeek>,
    /// IDs of in-progress subscribes, to guide responses (and which to suppress).
    subscribes: BTreeSet<GlobalId>,
    /// Reported upper frontiers for replicated collections and in-progress subscribes.
    uppers: HashMap<GlobalId, ReportedUppers<T>>,
    /// Reported upper frontiers for log collections.
    ///
    /// Log collections are special in that they are replica-specific. We therefore track their
    /// frontiers separately from those of replicated collections.
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
    fn add_replica(&mut self, id: ReplicaId) {
        self.replica_ids.insert(id);

        for uppers in self.uppers.values_mut() {
            uppers.add_replica(id);
        }
    }

    fn remove_replica(&mut self, id: ReplicaId) {
        self.replica_ids.remove(&id);

        // Removing a replica might have elicit changes to collection frontiers, which we must
        // report up the chain.
        let mut new_uppers = Vec::new();
        for (collection_id, uppers) in self.uppers.iter_mut() {
            if uppers.remove_replica(id) {
                new_uppers.push((*collection_id, uppers.bounds.clone()));
            }
        }

        if !new_uppers.is_empty() {
            self.pending_response
                .push_back(ActiveReplicationResponse::FrontierUppers(new_uppers));
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn handle_command(&mut self, cmd: &ComputeCommand<T>) {
        // Update our tracking of peek and subscribe commands.
        match &cmd {
            ComputeCommand::Peek(Peek { uuid, otel_ctx, .. }) => {
                self.peeks.insert(
                    *uuid,
                    PendingPeek {
                        // TODO(guswynn): can we just hold the `tracing::Span`
                        // here instead?
                        otel_ctx: otel_ctx.clone(),
                    },
                );
            }
            ComputeCommand::CancelPeeks { uuids } => {
                // Enqueue the response to the cancelation.
                self.pending_response.extend(uuids.iter().map(|uuid| {
                    // Canceled peeks should not be further responded to.
                    let otel_ctx = self
                        .peeks
                        .remove(uuid)
                        .map(|pending| pending.otel_ctx)
                        .unwrap_or_else(|| {
                            tracing::warn!("did not find pending peek for {}", uuid);
                            OpenTelemetryContext::empty()
                        });
                    ActiveReplicationResponse::PeekResponse(*uuid, PeekResponse::Canceled, otel_ctx)
                }));
            }
            ComputeCommand::CreateDataflows(dataflows) => {
                let subscribe_ids = dataflows.iter().flat_map(|df| df.subscribe_ids());
                self.subscribes.extend(subscribe_ids);
            }
            _ => {}
        }

        // Initialize any necessary frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        cmd.frontier_tracking(&mut start, &mut cease);
        for id in start.into_iter() {
            self.start_frontier_tracking(id);
        }
        for id in cease.into_iter() {
            self.cease_frontier_tracking(id);
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());
    }

    fn start_frontier_tracking(&mut self, id: GlobalId) {
        let uppers = ReportedUppers::new(&self.replica_ids);
        let previous = self.uppers.insert(id, uppers);
        assert!(previous.is_none());
    }

    fn cease_frontier_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id).expect("untracked frontier");

        // If we cease tracking an in-progress subscribe, we should emit a `DroppedAt` response.
        if self.subscribes.remove(&id) {
            self.pending_response
                .push_back(ActiveReplicationResponse::SubscribeResponse(
                    id,
                    SubscribeResponse::DroppedAt(previous.bounds.upper),
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
                // If this is the first response, forward it; otherwise do not.
                // TODO: we could collect the other responses to assert equivalence?
                // Trades resources (memory) for reassurances; idk which is best.
                //
                // NOTE: we use the `otel_ctx` from the response, not the
                // pending peek, because we currently want the parent
                // to be whatever the compute worker did with this peek.
                //
                // Additionally, we just use the `otel_ctx` from the first worker to
                // respond.
                self.peeks
                    .remove(&uuid)
                    .map(|_| ActiveReplicationResponse::PeekResponse(uuid, response, otel_ctx))
            }
            ComputeResponse::FrontierUppers(list) => self.handle_frontier_uppers(list, replica_id),
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id)
            }
        }
    }

    fn handle_frontier_uppers(
        &mut self,
        list: Vec<(GlobalId, Antichain<T>)>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let mut new_uppers = Vec::new();

        for (id, new_upper) in list {
            if let Some(reported) = self.uppers.get_mut(&id) {
                if reported.update(replica_id, new_upper) {
                    new_uppers.push((id, reported.bounds.clone()));
                }
            } else if let Some(reported) = self.log_uppers.get_mut(&id) {
                if PartialOrder::less_than(reported, &new_upper) {
                    reported.clone_from(&new_upper);
                    new_uppers.push((
                        id,
                        FrontierBounds {
                            lower: new_upper.clone(),
                            upper: new_upper,
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
                // We track both the upper and the lower bound of all upper frontiers
                // reported by all replicas.
                //  * If the upper bound advances, we can emit all updates at times greater
                //    or equal to the last reported upper bound (to avoid emitting duplicate
                //    updates) as a `SubscribeResponse`.
                //  * If either the upper or the lower bound advances, we emit this
                //    information as a `FrontierUppers` response.

                let old_upper_bound = entry.bounds.upper.clone();
                if !entry.update(replica_id, upper.clone()) {
                    // There are no new updates to report.
                    return None;
                }

                if PartialOrder::less_than(&old_upper_bound, &entry.bounds.upper) {
                    // When we get here, the subscribe must still be in progress.
                    assert!(self.subscribes.get(&subscribe_id).is_some());

                    let new_lower = old_upper_bound;
                    updates.retain(|(time, _data, _diff)| new_lower.less_equal(time));
                    self.pending_response
                        .push_back(ActiveReplicationResponse::SubscribeResponse(
                            subscribe_id,
                            SubscribeResponse::Batch(SubscribeBatch {
                                lower: new_lower,
                                upper: entry.bounds.upper.clone(),
                                updates,
                            }),
                        ))
                }

                let frontier_updates = vec![(subscribe_id, entry.bounds.clone())];
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

        // Start tracking frontiers of log collections.
        if let Some(logging) = &replica_state.logging_config {
            for id in logging.log_identifiers() {
                let frontier = Antichain::from_elem(Timestamp::minimum());
                let previous = self.state.log_uppers.insert(id, frontier);
                assert!(previous.is_none());
            }
        }

        // Add replica to tracked state.
        self.replicas.insert(id, replica_state);
        self.state.add_replica(id);
    }

    /// Returns an iterator over the IDs of the replicas.
    pub(super) fn get_replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Remove a replica by its identifier.
    pub(super) fn remove_replica(&mut self, id: ReplicaId) {
        self.state.remove_replica(id);
        let replica_state = self.replicas.remove(&id).expect("replica not found");

        // Cease tracking frontiers of persisted_logs collections.
        if let Some(logging) = &replica_state.logging_config {
            for (id, _) in logging.sink_logs.values() {
                let previous = self.state.log_uppers.remove(id);
                assert!(previous.is_some());
            }
        }
    }

    fn rehydrate_replica(&mut self, id: ReplicaId) {
        let addrs = self.replicas[&id].addrs.clone();
        let logging_config = self.replicas[&id].logging_config.clone();
        let communication_config = self.replicas[&id].communication_config.clone();
        self.remove_replica(id);
        self.add_replica(id, addrs, logging_config, communication_config);
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
    FrontierUppers(Vec<(GlobalId, FrontierBounds<T>)>),
    /// The compute instance's response to the specified peek.
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The compute instance's next response to the specified subscribe.
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// A notification that we heard a response from the given replica at the
    /// given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

#[derive(Debug, Clone)]
pub(super) struct FrontierBounds<T> {
    #[allow(dead_code)]
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_bounds {
        ($uppers:expr, ($lower:expr, $upper:expr)) => {
            assert_eq!(
                $uppers.bounds.lower,
                Antichain::from_elem($lower),
                "lower mismatch"
            );
            assert_eq!(
                $uppers.bounds.upper,
                Antichain::from_elem($upper),
                "upper mismatch"
            );
        };
    }

    #[test]
    fn reported_uppers() {
        let mut uppers = ReportedUppers::<u64>::new(&[1, 2].into());
        assert_bounds!(uppers, (0, 0));

        let changed = uppers.update(1, Antichain::from_elem(1));
        assert!(changed);
        assert_bounds!(uppers, (0, 1));

        let changed = uppers.update(2, Antichain::from_elem(2));
        assert!(changed);
        assert_bounds!(uppers, (1, 2));

        // Frontiers can only advance.
        let changed = uppers.update(2, Antichain::from_elem(1));
        assert!(!changed);
        assert_bounds!(uppers, (1, 2));
        assert_eq!(uppers.per_replica[&2], Antichain::from_elem(2));

        // Adding a replica doesn't affect current bounds.
        uppers.add_replica(3);
        assert_bounds!(uppers, (1, 2));

        let changed = uppers.update(3, Antichain::from_elem(3));
        assert!(changed);
        assert_bounds!(uppers, (1, 3));

        // Removing the slowest replica advances the lower bound.
        let changed = uppers.remove_replica(1);
        assert!(changed);
        assert_bounds!(uppers, (2, 3));

        // Removing the fastest replica doesn't affect bounds.
        let changed = uppers.remove_replica(3);
        assert!(!changed);
        assert_bounds!(uppers, (2, 3));

        // Removing the last replica advances the lower bound to the upper.
        let changed = uppers.remove_replica(2);
        assert!(changed);
        assert_bounds!(uppers, (3, 3));

        // Bounds tracking resumes correctly with new replicas.
        uppers.add_replica(4);
        uppers.add_replica(5);
        uppers.update(5, Antichain::from_elem(5));
        uppers.update(4, Antichain::from_elem(4));
        assert_bounds!(uppers, (4, 5));
    }
}
