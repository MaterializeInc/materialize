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

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::Antichain;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, info, warn};

use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::GlobalId;
use mz_service::client::{GenericClient, Reconnect};

use crate::command::{ComputeCommand, Peek, ReplicaId};
use crate::response::{ComputeResponse, PeekResponse, TailBatch, TailResponse};
use crate::service::ComputeClient;

const INITIAL_CONNECTION_BACKOFF: Duration = Duration::from_millis(1);

/// A task that manages communication with a replica.
struct ReplicaTask<C, T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses are delivered.
    response_tx: UnboundedSender<Result<ComputeResponse<T>, anyhow::Error>>,
    /// The underlying client that communicates with the replica.
    client: C,
    // When the last reconnection attempt (if any) was made.
    last_successful_connection: Option<Instant>,
    /// The current backoff, for preventing crash loops.
    backoff: Duration,
}

enum ReplicaTaskState {
    /// The replica should be (re)connected to.
    Reconnect,
    /// Commands should be drained until the next `CreateInstance` command is
    /// observed.
    Drain,
    /// Communication with the replica is live. Commands and responses should
    /// be forwarded until an error occurs.
    Pump,
    /// The caller has asked us to shut down communication with this replica.
    Done,
}

impl<C, T> ReplicaTask<C, T>
where
    C: ComputeClient<T> + Reconnect + 'static,
    T: Send + std::fmt::Debug + 'static,
{
    fn spawn(
        id: ReplicaId,
        client: C,
    ) -> (
        UnboundedSender<ComputeCommand<T>>,
        UnboundedReceiver<Result<ComputeResponse<T>, anyhow::Error>>,
    ) {
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        let mut task = ReplicaTask {
            replica_id: id,
            command_rx,
            response_tx,
            client,
            last_successful_connection: None,
            backoff: INITIAL_CONNECTION_BACKOFF,
        };
        mz_ore::task::spawn(|| format!("active-replication-replica-{id}"), async move {
            task.run().await
        });
        (command_tx, response_rx)
    }

    async fn run(&mut self) {
        let mut state = ReplicaTaskState::Reconnect;
        loop {
            state = match state {
                ReplicaTaskState::Reconnect => self.step_reconnect().await,
                ReplicaTaskState::Drain => self.step_drain().await,
                ReplicaTaskState::Pump => self.step_pump().await,
                ReplicaTaskState::Done => break,
            }
        }
        debug!("task for replica {} exiting", self.replica_id);
    }

    async fn step_reconnect(&mut self) -> ReplicaTaskState {
        if let Some(last_successful_connection) = self.last_successful_connection {
            // If we were previously up for long enough (60 seconds chosen
            // arbitrarily), we consider the previous connection to have been
            // successful and reset the backoff.
            if Instant::now() - last_successful_connection > Duration::from_secs(60) {
                self.backoff = INITIAL_CONNECTION_BACKOFF;
            }
        }
        time::sleep(self.backoff).await;
        // 60 seconds is arbitrarily chosen as a maximum plausible backoff. If
        // Timely processes aren't realizing their buddies are down within that
        // time, something is seriously hosed with the network anyway and its
        // unlikely things will work.
        self.backoff = (self.backoff * 2).min(Duration::from_secs(60));
        match self.client.reconnect().await {
            Ok(()) => {
                self.last_successful_connection = Some(Instant::now());
                ReplicaTaskState::Drain
            }
            Err(e) => {
                info!(
                    "Reconnecting to replica {} failed (retrying in {:?}): {}",
                    self.replica_id, self.backoff, e
                );
                ReplicaTaskState::Reconnect
            }
        }
    }

    async fn step_drain(&mut self) -> ReplicaTaskState {
        match self.command_rx.recv().await {
            None => ReplicaTaskState::Done,
            Some(command) => match command {
                ComputeCommand::CreateInstance(_) => self.send_command(command).await,
                // Command intended for the prior incarnation of the replica.
                // Ignore it.
                _ => ReplicaTaskState::Drain,
            },
        }
    }

    async fn step_pump(&mut self) -> ReplicaTaskState {
        select! {
            // Command from controller to forward to replica.
            command = self.command_rx.recv() => match command {
                None => ReplicaTaskState::Done,
                Some(command) => self.send_command(command).await,
            },
            // Response from replica to forward to controller.
            response = self.client.recv() => {
                let response = match response.transpose() {
                    None => {
                        // In the future, if a replica politely hangs up, we
                        // might want to take it as a signal that a new
                        // controller has taken over. For now we just try to
                        // reconnect.
                        Err(anyhow!("replica unexpectedly gracefully terminated connection"))
                    }
                    Some(response) => response,
                };
                self.send_response(response).await
            }
        }
    }

    async fn send_command(&mut self, command: ComputeCommand<T>) -> ReplicaTaskState {
        match self.client.send(command).await {
            Ok(()) => ReplicaTaskState::Pump,
            Err(e) => self.send_response(Err(e)).await,
        }
    }

    async fn send_response(
        &mut self,
        response: Result<ComputeResponse<T>, anyhow::Error>,
    ) -> ReplicaTaskState {
        // We always forward the response verbatim, but if it's an error, we
        // switch into reconnection mode. The controller will notice the error
        // and eventually rehydrate the replica.
        let next_state = match &response {
            Ok(_) => ReplicaTaskState::Pump,
            Err(_) => ReplicaTaskState::Reconnect,
        };
        if self.response_tx.send(response).is_err() {
            ReplicaTaskState::Done
        } else {
            next_state
        }
    }
}

/// Additional information to store with pening peeks.
#[derive(Debug)]
pub struct PendingPeek {
    /// The OpenTelemetry context for this peek.
    otel_ctx: OpenTelemetryContext,
}

/// The internal state of the client.
///
/// This lives in a separate struct from the handles to the individual replica
/// tasks, so that we can call methods on it
/// while holding mutable borrows to those.
#[derive(Debug)]
pub struct ActiveReplicationState<T> {
    /// Outstanding peek identifiers, to guide responses (and which to suppress).
    peeks: HashMap<uuid::Uuid, PendingPeek>,
    /// Reported frontier of each in-progress tail.
    tails: HashMap<GlobalId, Antichain<T>>,
    /// Frontier information, both unioned across all replicas and from each individual replica.
    uppers: HashMap<GlobalId, (Antichain<T>, HashMap<ReplicaId, MutableAntichain<T>>)>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: ComputeCommandHistory<T>,
    /// Most recent count of the volume of unpacked commands (e.g. dataflows in `CreateDataflows`).
    last_command_count: usize,
    /// Responses that should be emitted on the next `recv` call.
    ///
    /// This is introduced to produce peek cancelation responses eagerly, without awaiting a replica
    /// responding with the response itself, which allows us to compact away the peek in `self.history`.
    pending_response: VecDeque<ActiveReplicationResponse<T>>,
}

impl<T> ActiveReplicationState<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    fn handle_command(
        &mut self,
        cmd: &ComputeCommand<T>,
        frontiers: HashMap<u64, MutableAntichain<T>>,
    ) {
        // Update our tracking of peek commands.
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
                    ActiveReplicationResponse::ComputeResponse(ComputeResponse::PeekResponse(
                        *uuid,
                        PeekResponse::Canceled,
                        otel_ctx,
                    ))
                }));
            }
            _ => {}
        }

        // Initialize any necessary frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        cmd.frontier_tracking(&mut start, &mut cease);
        for id in start.into_iter() {
            let frontier = timely::progress::Antichain::from_elem(T::minimum());

            let previous = self.uppers.insert(id, (frontier, frontiers.clone()));
            assert!(previous.is_none());
        }
        for id in cease.into_iter() {
            let previous = self.uppers.remove(&id);
            assert!(previous.is_some());
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());

        // If we have reached a point that justifies history reduction, do that.
        if self.history.len() > 2 * self.last_command_count {
            self.last_command_count = self.history.reduce(&self.peeks);
        }
    }

    fn handle_message(
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
                self.peeks.remove(&uuid).map(|_| {
                    ActiveReplicationResponse::ComputeResponse(ComputeResponse::PeekResponse(
                        uuid, response, otel_ctx,
                    ))
                })
            }
            ComputeResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some((frontier, frontiers)) = self.uppers.get_mut(id) {
                        // Apply changes to replica `replica_id`
                        frontiers
                            .get_mut(&replica_id)
                            .unwrap()
                            .update_iter(changes.drain());
                        // We can swap `frontier` into `changes, negated, and then use that to repopulate `frontier`.
                        // Working
                        changes.extend(frontier.iter().map(|t| (t.clone(), -1)));
                        frontier.clear();
                        for (time1, _neg_one) in changes.iter() {
                            for time2 in frontiers[&replica_id].frontier().iter() {
                                frontier.insert(time1.join(time2));
                            }
                        }
                        changes.extend(frontier.iter().map(|t| (t.clone(), 1)));
                        changes.compact();
                    }
                }
                if !list.is_empty() {
                    Some(ActiveReplicationResponse::ComputeResponse(
                        ComputeResponse::FrontierUppers(list),
                    ))
                } else {
                    None
                }
            }
            ComputeResponse::TailResponse(id, response) => {
                match response {
                    TailResponse::Batch(TailBatch {
                        lower: _,
                        upper,
                        mut updates,
                    }) => {
                        // It is sufficient to compare `upper` against the last reported frontier for `id`,
                        // and if `upper` is not less or equal to that frontier, some progress has happened.
                        // If so, we retain only the updates greater or equal to that last reported frontier,
                        // and announce a batch from that frontier to its join with `upper`.

                        // Ensure that we have a recorded frontier ready to go.
                        let entry = self
                            .tails
                            .entry(id)
                            .or_insert_with(|| Antichain::from_elem(T::minimum()));
                        // If the upper frontier has changed, we have a statement to make.
                        // This happens if there is any element of `entry` not greater or
                        // equal to some element of `upper`.
                        let new_upper = entry.join(&upper);
                        if &new_upper != entry {
                            let new_lower = entry.clone();
                            entry.clone_from(&new_upper);
                            updates.retain(|(time, _data, _diff)| new_lower.less_equal(time));
                            Some(ActiveReplicationResponse::ComputeResponse(
                                ComputeResponse::TailResponse(
                                    id,
                                    TailResponse::Batch(TailBatch {
                                        lower: new_lower,
                                        upper: new_upper,
                                        updates,
                                    }),
                                ),
                            ))
                        } else {
                            None
                        }
                    }
                    TailResponse::DroppedAt(frontier) => {
                        // Introduce a new terminal frontier to suppress all future responses.
                        // We cannot simply remove the entry, as we currently create new entries in response
                        // to observed responses; if we pre-load the entries in response to commands we can
                        // clean up the state here.
                        self.tails.insert(id, Antichain::new());
                        Some(ActiveReplicationResponse::ComputeResponse(
                            ComputeResponse::TailResponse(id, TailResponse::DroppedAt(frontier)),
                        ))
                    }
                }
            }
        }
    }
}

/// A client backed by multiple replicas.
#[derive(Debug)]
pub struct ActiveReplication<T> {
    /// Handles to the replicas themselves.
    replicas: HashMap<
        ReplicaId,
        (
            UnboundedSender<ComputeCommand<T>>,
            UnboundedReceiverStream<Result<ComputeResponse<T>, anyhow::Error>>,
        ),
    >,
    /// All other internal state of the client
    state: ActiveReplicationState<T>,
}

impl<T> Default for ActiveReplication<T> {
    fn default() -> Self {
        Self {
            replicas: Default::default(),
            state: ActiveReplicationState {
                peeks: Default::default(),
                tails: Default::default(),
                uppers: Default::default(),
                history: Default::default(),
                last_command_count: 0,
                pending_response: Default::default(),
            },
        }
    }
}

impl<T> ActiveReplication<T>
where
    T: timely::progress::Timestamp,
{
    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    pub fn add_replica<C>(&mut self, id: ReplicaId, client: C)
    where
        C: ComputeClient<T> + Reconnect + 'static,
    {
        for (_, frontiers) in self.state.uppers.values_mut() {
            frontiers.insert(id, {
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((T::minimum(), 1)));
                frontier
            });
        }
        let (cmd_tx, resp_rx) = ReplicaTask::spawn(id, client);
        self.replicas.insert(id, (cmd_tx, resp_rx.into()));
        self.hydrate_replica(id);
    }

    pub fn get_replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Remove a replica by its identifier.
    pub fn remove_replica(&mut self, id: ReplicaId) {
        self.replicas.remove(&id);
        for (_frontier, frontiers) in self.state.uppers.iter_mut() {
            frontiers.1.remove(&id);
        }
    }

    /// Pipes a command stream at the indicated replica, introducing new dataflow identifiers.
    fn hydrate_replica(&mut self, replica_id: ReplicaId) {
        // Zero out frontiers maintained by this replica.
        for (_id, (_, frontiers)) in self.state.uppers.iter_mut() {
            *frontiers.get_mut(&replica_id).unwrap() =
                timely::progress::frontier::MutableAntichain::new();
            frontiers
                .get_mut(&replica_id)
                .unwrap()
                .update_iter(Some((T::minimum(), 1)));
        }
        // Take this opportunity to clean up the history we should present.
        self.state.last_command_count = self.state.history.reduce(&self.state.peeks);

        // Replay the commands at the client, creating new dataflow identifiers.
        let (cmd_tx, _) = self.replicas.get_mut(&replica_id).unwrap();
        for command in self.state.history.iter() {
            let mut command = command.clone();
            specialize_command(&mut command, replica_id);

            cmd_tx
                .send(command)
                .expect("Channel to client has gone away!")
        }
    }
}

#[async_trait::async_trait]
impl<T> GenericClient<ComputeCommand<T>, ActiveReplicationResponse<T>> for ActiveReplication<T>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + std::fmt::Debug,
{
    /// The ADAPTER layer's isolation from COMPUTE depends on the fact that this
    /// function is essentially non-blocking, i.e. the ADAPTER blindly awaits
    /// calls to this function. This lets the ADAPTER continue operating even in
    /// the face of unhealthy or absent replicas.
    ///
    /// If this function every become blocking (e.g. making networking calls),
    /// the ADAPTER must amend its contract with COMPUTE.
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        let frontiers = self
            .replicas
            .keys()
            .map(|id| {
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((T::minimum(), 1)));
                (id.clone(), frontier)
            })
            .collect();

        self.state.handle_command(&cmd, frontiers);

        // Clone the command for each active replica.
        for (id, (tx, _)) in self.replicas.iter_mut() {
            let mut command = cmd.clone();
            specialize_command(&mut command, *id);

            // Errors are suppressed by this client, which awaits a reconnection
            // in `recv` and will rehydrate the client when that happens.
            //
            // NOTE: Broadcasting commands to replicas irrespective of their
            // presence or health is part of the isolation contract between
            // ADAPTER and COMPUTE. If this changes (e.g. awaiting responses
            // from replicas), ADAPTER needs to handle its interactions with
            // COMPUTE differently.
            let _ = tx.send(command);
        }

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<ActiveReplicationResponse<T>>, anyhow::Error> {
        // If we have a pending response, we should send it immediately.
        if let Some(response) = self.state.pending_response.pop_front() {
            return Ok(Some(response));
        }

        if self.replicas.is_empty() {
            // We want to communicate that the result is not ready
            futures::future::pending().await
        }

        // We may need to iterate, if a replica needs rehydration.
        loop {
            // Receive responses from any of the replicas, and take appropriate action.
            let mut stream: tokio_stream::StreamMap<_, _> = self
                .replicas
                .iter_mut()
                .map(|(id, (_, rx))| (id.clone(), rx))
                .collect();

            use futures::StreamExt;
            while let Some((replica_id, message)) = stream.next().await {
                match message {
                    Ok(message) => match self.state.handle_message(message, replica_id) {
                        Some(response) => return Ok(Some(response)),
                        None => { /* continue */ }
                    },
                    Err(e) => {
                        warn!("Rehydrating replica {}: {}", replica_id, e);
                        drop(stream);
                        self.hydrate_replica(replica_id);
                        break;
                    }
                }
            }
        }
    }
}

/// A response from the ActiveReplication client:
/// either a deduplicated compute response, or a notification
/// that we heard from a given replica and should update its recency status.
#[derive(Debug, Clone)]
pub enum ActiveReplicationResponse<T = mz_repr::Timestamp> {
    /// A response from the compute layer.
    ComputeResponse(ComputeResponse<T>),
    /// A notification that we heard a response
    /// from the given replica at the given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

#[derive(Debug)]
struct ComputeCommandHistory<T> {
    commands: Vec<ComputeCommand<T>>,
}

impl<T: timely::progress::Timestamp> ComputeCommandHistory<T> {
    pub fn push(&mut self, command: ComputeCommand<T>) {
        self.commands.push(command);
    }
    /// Reduces `self.history` to a minimal form.
    ///
    /// This action not only simplifies the issued history, but importantly reduces the instructions
    /// to only reference inputs from times that are still certain to be valid. Commands that allow
    /// compaction of a collection also remove certainty that the inputs will be available for times
    /// not greater or equal to that compaction frontier.
    ///
    /// The `peeks` argument should contain those peeks that have yet to be resolved, either through
    /// response or cancellation.
    ///
    /// Returns the number of distinct commands that remain.
    pub fn reduce<V>(&mut self, peeks: &std::collections::HashMap<uuid::Uuid, V>) -> usize {
        // First determine what the final compacted frontiers will be for each collection.
        // These will determine for each collection whether the command that creates it is required,
        // and if required what `as_of` frontier should be used for its updated command.
        let mut final_frontiers = std::collections::BTreeMap::new();
        let mut live_dataflows = Vec::new();
        let mut live_peeks = Vec::new();
        let mut live_cancels = std::collections::BTreeSet::new();

        let mut create_command = None;
        let mut drop_command = None;

        for command in self.commands.drain(..) {
            match command {
                create @ ComputeCommand::CreateInstance(_) => {
                    // We should be able to handle this, should this client need to be restartable.
                    assert!(create_command.is_none());
                    create_command = Some(create);
                }
                cmd @ ComputeCommand::DropInstance => {
                    assert!(drop_command.is_none());
                    drop_command = Some(cmd);
                }
                ComputeCommand::CreateDataflows(dataflows) => {
                    live_dataflows.extend(dataflows);
                }
                ComputeCommand::AllowCompaction(frontiers) => {
                    for (id, frontier) in frontiers {
                        final_frontiers.insert(id, frontier.clone());
                    }
                }
                ComputeCommand::Peek(peek) => {
                    // We could pre-filter here, but seems hard to access `uuid`
                    // and take ownership of `peek` at the same time.
                    live_peeks.push(peek);
                }
                ComputeCommand::CancelPeeks { mut uuids } => {
                    uuids.retain(|uuid| peeks.contains_key(uuid));
                    live_cancels.extend(uuids);
                }
            }
        }

        // Update dataflow `as_of` frontiers to the least of the final frontiers of their outputs.
        // One possible frontier is the empty frontier, indicating that the dataflow can be removed.
        for dataflow in live_dataflows.iter_mut() {
            let mut as_of = Antichain::new();
            for id in dataflow.export_ids() {
                if let Some(frontier) = final_frontiers.get(&id) {
                    as_of.extend(frontier.clone());
                } else {
                    as_of.extend(dataflow.as_of.clone().unwrap());
                }
            }

            // Remove compaction for any collection that brought us to `as_of`.
            for id in dataflow.export_ids() {
                if let Some(frontier) = final_frontiers.get(&id) {
                    if frontier == &as_of {
                        final_frontiers.remove(&id);
                    }
                }
            }

            dataflow.as_of = Some(as_of);
        }

        // Discard dataflows whose outputs have all been allowed to compact away.
        live_dataflows.retain(|dataflow| dataflow.as_of != Some(Antichain::new()));

        // Retain only those peeks that have not yet been processed.
        live_peeks.retain(|peek| peeks.contains_key(&peek.uuid));

        // Record the volume of post-compaction commands.
        let mut command_count = 1;
        command_count += live_dataflows.len();
        command_count += final_frontiers.len();
        command_count += live_peeks.len();
        command_count += live_cancels.len();
        if drop_command.is_some() {
            command_count += 1;
        }

        // Reconstitute the commands as a compact history.
        if let Some(create_command) = create_command {
            self.commands.push(create_command);
        }
        if !live_dataflows.is_empty() {
            self.commands
                .push(ComputeCommand::CreateDataflows(live_dataflows));
        }
        if !final_frontiers.is_empty() {
            self.commands.push(ComputeCommand::AllowCompaction(
                final_frontiers.into_iter().collect(),
            ));
        }
        self.commands
            .extend(live_peeks.into_iter().map(ComputeCommand::Peek));
        if !live_cancels.is_empty() {
            self.commands.push(ComputeCommand::CancelPeeks {
                uuids: live_cancels,
            });
        }
        if let Some(drop_command) = drop_command {
            self.commands.push(drop_command);
        }

        command_count
    }
    /// Iterate through the contained commands.
    pub fn iter(&self) -> impl Iterator<Item = &ComputeCommand<T>> {
        self.commands.iter()
    }

    /// Report the number of commands.
    ///
    /// Importantly, each command can be arbitrarily complicated, so this number could be small
    /// even while we have few commands that cause many actions to be taken.
    pub fn len(&self) -> usize {
        self.commands.len()
    }
}

impl<T> Default for ComputeCommandHistory<T> {
    fn default() -> Self {
        Self {
            commands: Vec::new(),
        }
    }
}

/// Specialize a command for the given `ReplicaId`.
///
/// Most `ComputeCommand`s are independent of the target replica, but some
/// contain replica-specific fields that must be adjusted before sending.
fn specialize_command<T>(command: &mut ComputeCommand<T>, replica_id: ReplicaId) {
    // Tell new instances their replica ID.
    if let ComputeCommand::CreateInstance(config) = command {
        config.replica_id = replica_id;
    }

    // Replace dataflow identifiers with new unique ids.
    if let ComputeCommand::CreateDataflows(dataflows) = command {
        for dataflow in dataflows.iter_mut() {
            dataflow.id = uuid::Uuid::new_v4();
        }
    }
}
