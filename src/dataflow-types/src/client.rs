// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for controller of the dataflow subsystem.

// This appears to be defective at the moment, with false positiives
// for each variant of the `Command` enum, each of which are documented.
// #![warn(missing_docs)]

use async_trait::async_trait;
use enum_iterator::IntoEnumIterator;
use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use tracing::trace;

use crate::logging::LoggingConfig;
use crate::{
    sources::{MzOffset, SourceDesc},
    DataflowDescription, PeekResponse, SourceInstanceDesc, TailResponse, Update,
};
use mz_expr::{GlobalId, PartitionId, RowSetFinishing};
use mz_repr::Row;

pub mod controller;
pub use controller::Controller;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command<T = mz_repr::Timestamp> {
    /// A compute command.
    Compute(ComputeCommand<T>, ComputeInstanceId),
    /// A storage command.
    Storage(StorageCommand<T>),
}

/// An abstraction allowing us to name difference compute instances.
pub type ComputeInstanceId = usize;
/// A default value whose use we can track down and remove later.
pub const DEFAULT_COMPUTE_INSTANCE_ID: ComputeInstanceId = 0;

/// Commands related to the computation and maintenance of views.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    ComputeCommandKind,
    derive(IntoEnumIterator),
    doc = "The kind of compute command that was received"
)]
pub enum ComputeCommand<T = mz_repr::Timestamp> {
    /// Indicates the creation of an instance, and is the first command for its compute instance.
    ///
    /// Optionally, request that the logging sources in the contained configuration are installed.
    CreateInstance(Option<LoggingConfig>),
    /// Indicates the termination of an instance, and is the last command for its compute instance.
    DropInstance,

    /// Create a sequence of dataflows.
    ///
    /// Each of the dataflows must contain `as_of` members that are valid
    /// for each of the referenced arrangements, meaning `AllowCompaction`
    /// should be held back to those values until the command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly maintain the dataflows.
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan<T>, T>>),
    /// Enable compaction in compute-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),

    /// Peek at an arrangement.
    ///
    /// This request elicits data from the worker, by naming an
    /// arrangement and some actions to apply to the results before
    /// returning them.
    ///
    /// The `timestamp` member must be valid for the arrangement that
    /// is referenced by `id`. This means that `AllowCompaction` for
    /// this arrangement should not pass `timestamp` before this command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly answer the `Peek`.
    Peek {
        /// The identifier of the arrangement.
        id: GlobalId,
        /// An optional key that should be used for the arrangement.
        key: Option<Row>,
        /// The identifier of this peek request.
        ///
        /// Used in responses and cancelation requests.
        conn_id: u32,
        /// The logical timestamp at which the arrangement is queried.
        timestamp: T,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: mz_expr::SafeMfpPlan,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek {
        /// The identifier of the peek request to cancel.
        conn_id: u32,
    },
}

impl ComputeCommandKind {
    /// Returns the name of this kind of command.
    ///
    /// Must remain unique over all variants of `Command`.
    pub fn metric_name(&self) -> &'static str {
        match self {
            // TODO: This breaks metrics. Not sure that's a problem.
            ComputeCommandKind::CreateInstance => "create_instance",
            ComputeCommandKind::DropInstance => "drop_instance",
            ComputeCommandKind::AllowCompaction => "allow_compute_compaction",
            ComputeCommandKind::CancelPeek => "cancel_peek",
            ComputeCommandKind::CreateDataflows => "create_dataflows",
            ComputeCommandKind::Peek => "peek",
        }
    }
}

/// A command creating a single source
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateSourceCommand<T> {
    /// The source identifier
    pub id: GlobalId,
    /// The source description
    pub desc: SourceDesc,
    /// The initial `since` frontier
    pub since: Antichain<T>,
    /// Any previously stored timestamp bindings
    pub ts_bindings: Vec<(PartitionId, T, crate::sources::MzOffset)>,
}

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    StorageCommandKind,
    derive(IntoEnumIterator),
    doc = "the kind of storage command that was received"
)]
pub enum StorageCommand<T = mz_repr::Timestamp> {
    /// Create the enumerated sources, each associated with its identifier.
    CreateSources(Vec<CreateSourceCommand<T>>),
    /// Render the enumerated sources.
    ///
    /// Each source has a name for debugging purposes, an optional "as of" frontier and collection
    /// of sources to import.
    RenderSources(
        Vec<(
            /* debug_name */ String,
            /* dataflow_id */ GlobalId,
            /* as_of */ Option<Antichain<T>>,
            /* source_imports*/ BTreeMap<GlobalId, SourceInstanceDesc<T>>,
        )>,
    ),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),

    /// Insert `updates` into the local input named `id`.
    Insert {
        /// Identifier of the local input.
        id: GlobalId,
        /// A list of updates to be introduced to the input.
        updates: Vec<Update<T>>,
    },
    /// Update durability information for sources.
    ///
    /// Each entry names a source and provides a frontier before which the source can
    /// be exactly replayed across restarts (i.e. we can assign the same timestamps to
    /// all the same data)
    DurabilityFrontierUpdates(Vec<(GlobalId, Antichain<T>)>),
    /// Advance all local inputs to the given timestamp.
    AdvanceAllLocalInputs {
        /// The timestamp to advance to.
        advance_to: T,
    },
}

impl StorageCommandKind {
    /// Returns the same of this kind of command.
    ///
    /// Must remain unique over all variants of `Command`.
    pub fn metric_name(&self) -> &'static str {
        match self {
            StorageCommandKind::AdvanceAllLocalInputs => "advance_all_local_inputs",
            StorageCommandKind::AllowCompaction => "allow_storage_compaction",
            StorageCommandKind::CreateSources => "create_sources",
            StorageCommandKind::DurabilityFrontierUpdates => "durability_frontier_updates",
            StorageCommandKind::Insert => "insert",
            StorageCommandKind::RenderSources => "render_sources",
        }
    }
}

impl<T: timely::progress::Timestamp> Command<T> {
    /// Partitions the command into `parts` many disjoint pieces.
    ///
    /// This is used to subdivide commands that can be sharded across workers,
    /// for example the `plan::Constant` stages of dataflow plans, and the
    /// `Command::Insert` commands that may contain multiple updates.
    pub fn partition_among(self, parts: usize) -> Vec<Self> {
        if parts == 0 {
            Vec::new()
        } else if parts == 1 {
            vec![self]
        } else {
            match self {
                Command::Compute(ComputeCommand::CreateDataflows(dataflows), instance) => {
                    let mut dataflows_parts = vec![Vec::new(); parts];

                    for dataflow in dataflows {
                        // A list of descriptions of objects for each part to build.
                        let mut builds_parts = vec![Vec::new(); parts];
                        // Partition each build description among `parts`.
                        for build_desc in dataflow.objects_to_build {
                            let build_part = build_desc.view.partition_among(parts);
                            for (view, objects_to_build) in
                                build_part.into_iter().zip(builds_parts.iter_mut())
                            {
                                objects_to_build.push(crate::BuildDesc {
                                    id: build_desc.id,
                                    view,
                                });
                            }
                        }
                        // Each list of build descriptions results in a dataflow description.
                        for (dataflows_part, objects_to_build) in
                            dataflows_parts.iter_mut().zip(builds_parts)
                        {
                            dataflows_part.push(DataflowDescription {
                                source_imports: dataflow.source_imports.clone(),
                                index_imports: dataflow.index_imports.clone(),
                                objects_to_build,
                                index_exports: dataflow.index_exports.clone(),
                                sink_exports: dataflow.sink_exports.clone(),
                                dependent_objects: dataflow.dependent_objects.clone(),
                                as_of: dataflow.as_of.clone(),
                                debug_name: dataflow.debug_name.clone(),
                                id: dataflow.id,
                            });
                        }
                    }
                    dataflows_parts
                        .into_iter()
                        .map(|x| Command::Compute(ComputeCommand::CreateDataflows(x), instance))
                        .collect()
                }
                Command::Storage(StorageCommand::Insert { id, updates }) => {
                    let mut updates_parts = vec![Vec::new(); parts];
                    for (index, update) in updates.into_iter().enumerate() {
                        updates_parts[index % parts].push(update);
                    }
                    updates_parts
                        .into_iter()
                        .map(|updates| Command::Storage(StorageCommand::Insert { id, updates }))
                        .collect()
                }
                command => vec![command; parts],
            }
        }
    }

    /// Indicates which global ids should start and cease frontier tracking.
    ///
    /// Identifiers added to `start` will install frontier tracking, and indentifiers
    /// added to `cease` will uninstall frontier tracking.
    pub fn frontier_tracking(&self, start: &mut Vec<GlobalId>, cease: &mut Vec<GlobalId>) {
        match self {
            Command::Storage(StorageCommand::CreateSources(sources)) => {
                for source in sources.iter() {
                    start.push(source.id);
                }
            }
            Command::Compute(ComputeCommand::CreateDataflows(dataflows), _instance) => {
                for dataflow in dataflows.iter() {
                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        start.push(*sink_id)
                    }
                    for (index_id, _, _) in dataflow.index_exports.iter() {
                        start.push(*index_id);
                    }
                }
            }
            Command::Compute(ComputeCommand::AllowCompaction(frontiers), _instance) => {
                for (id, frontier) in frontiers.iter() {
                    if frontier.is_empty() {
                        cease.push(*id);
                    }
                }
            }
            Command::Compute(ComputeCommand::CreateInstance(logging), _instance) => {
                if let Some(logging_config) = logging {
                    start.extend(logging_config.log_identifiers());
                }
            }
            _ => {
                // Other commands have no known impact on frontier tracking.
            }
        }
    }
    /// Returns the same of this kind of command.
    ///
    /// Must remain unique over all variants of `Command`.
    pub fn metric_name(&self) -> &'static str {
        match self {
            Command::Compute(command, _instance) => ComputeCommandKind::from(command).metric_name(),
            Command::Storage(command) => StorageCommandKind::from(command).metric_name(),
        }
    }
}

/// Data about timestamp bindings that dataflow workers send to the coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampBindingFeedback<T = mz_repr::Timestamp> {
    /// Durability frontier changes
    pub changes: Vec<(GlobalId, ChangeBatch<T>)>,
    /// Timestamp bindings for all of those frontier changes
    pub bindings: Vec<(GlobalId, PartitionId, T, MzOffset)>,
}

/// Responses that the worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response<T = mz_repr::Timestamp> {
    /// A compute response.
    Compute(ComputeResponse<T>, ComputeInstanceId),
    /// A storage response.
    Storage(StorageResponse<T>),
}

/// Responses that the compute nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComputeResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<T>)>),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(u32, PeekResponse),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
}

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// Timestamp bindings and prior and new frontiers for those bindings for all
    /// sources
    TimestampBindings(TimestampBindingFeedback<T>),
}

/// A client to a running dataflow server.
#[async_trait(?Send)]
pub trait Client<T = mz_repr::Timestamp> {
    /// Sends a command to the dataflow server.
    ///
    /// The command can error for various reasons.
    async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error>;

    /// Receives the next response from the dataflow server.
    ///
    /// This method blocks until the next response is available, or, if the
    /// dataflow server has been shut down, returns `None`.
    async fn recv(&mut self) -> Option<Response<T>>;
}

#[async_trait(?Send)]
impl Client for Box<dyn Client> {
    async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        (**self).recv().await
    }
}

/// A helper struct which allows us to interpret a `Client` as a `Stream` of `Response`.
pub struct ClientAsStream<'a, C: Client + 'a> {
    client: &'a mut C,
}

use std::pin::Pin;
use std::task::{Context, Poll};
impl<'a, C: Client + 'a> futures::stream::Stream for ClientAsStream<'a, C> {
    type Item = Response;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use futures::Future;
        Pin::new(&mut self.client.recv()).poll(cx)
    }
}

/// A wrapper for enumerating any available result from a client.
///
/// Maintains an internal counter, and round robins through clients for fairness.
pub struct SelectStream<'a, C: Client + 'a> {
    clients: &'a mut [C],
    cursor: usize,
}

impl<'a, C: Client + 'a> SelectStream<'a, C> {
    /// Create a new [SelectStream], starting from the client at position `cursor`.
    pub fn new(clients: &'a mut [C], cursor: usize) -> Self {
        Self { clients, cursor }
    }
}

impl<'a, C: Client + 'a> futures::stream::Stream for SelectStream<'a, C> {
    type Item = (usize, Response);
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut done = true;
        for _ in 0..self.clients.len() {
            let cursor = self.cursor;
            let result = {
                use futures::Future;
                Pin::new(&mut self.clients[cursor].recv()).poll(cx)
            };
            self.cursor = (self.cursor + 1) % self.clients.len();
            match result {
                Poll::Pending => {
                    done = false;
                }
                Poll::Ready(None) => {}
                Poll::Ready(Some(response)) => {
                    return Poll::Ready(Some((self.cursor, response)));
                }
            }
        }
        if done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// A convenience type for compatibility.
pub struct LocalClient {
    client: partitioned::Partitioned<process_local::ProcessLocal>,
}

impl LocalClient {
    pub fn new(
        feedback_rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<Response>>,
        worker_txs: Vec<crossbeam_channel::Sender<Command>>,
        worker_threads: Vec<std::thread::Thread>,
    ) -> Self {
        assert_eq!(feedback_rxs.len(), worker_threads.len());
        assert_eq!(worker_txs.len(), worker_threads.len());
        // assemble a list of process-local clients.
        let mut locals = Vec::with_capacity(worker_txs.len());
        for ((rx, tx), thread) in feedback_rxs.into_iter().zip(worker_txs).zip(worker_threads) {
            locals.push(process_local::ProcessLocal::new(rx, tx, thread));
        }
        LocalClient {
            client: partitioned::Partitioned::new(locals),
        }
    }
}

#[async_trait(?Send)]
impl Client for LocalClient {
    async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
        trace!("SEND dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;
        trace!("RECV dataflow response: {:?}", response);
        response
    }
}

/// Clients whose implementation is partitioned across a set of subclients (e.g. timely workers).
pub mod partitioned {

    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::iter::Fuse;

    use async_trait::async_trait;

    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row, Timestamp};
    use timely::order::PartialOrder;
    use timely::progress::{Antichain, ChangeBatch};
    use tracing::debug;

    use crate::client::ComputeInstanceId;
    use crate::TailResponse;

    use super::{Client, ComputeResponse, StorageResponse};
    use super::{Command, PeekResponse, Response};

    /// A client whose implementation is sharded across a number of other clients.
    ///
    /// Such a client needs to broadcast (partitioned) commands to all of its clients,
    /// and await responses from each of the client shards before it can respond.
    pub struct Partitioned<C: Client> {
        shards: Vec<C>,
        cursor: usize,
        state: PartitionedClientState,
    }

    impl<C: Client> Partitioned<C> {
        /// Create a client partitioned across multiple client shards.
        pub fn new(shards: Vec<C>) -> Self {
            Self {
                state: PartitionedClientState::new(shards.len()),
                cursor: 0,
                shards,
            }
        }
    }

    #[async_trait(?Send)]
    impl<C: Client> Client for Partitioned<C> {
        async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
            self.state.observe_command(&cmd);
            let cmd_parts = cmd.partition_among(self.shards.len());
            for (shard, cmd_part) in self.shards.iter_mut().zip(cmd_parts) {
                shard.send(cmd_part).await?;
            }
            Ok(())
        }

        async fn recv(&mut self) -> Option<Response> {
            use futures::StreamExt;
            if let Some(stashed) = self.state.stashed_responses.next() {
                return Some(stashed);
            }
            self.cursor = (self.cursor + 1) % self.shards.len();
            let mut stream = super::SelectStream::new(&mut self.shards[..], self.cursor);
            while let Some((index, response)) = stream.next().await {
                let messages = self.state.absorb_response(index, response).fuse();
                assert!(self.state.stashed_responses.next().is_none(), "We should have returned the next stashed element either at the beginning of this function, or in the last iteration of this loop!");
                self.state.stashed_responses = messages;
                if let Some(message) = self.state.stashed_responses.next() {
                    return Some(message);
                }
            }
            // Indicate completion of the communication.
            None
        }
    }

    use timely::progress::frontier::MutableAntichain;

    /// Buffer for partial results for a `TAIL`.
    /// This exists so we can consolidate rows and
    /// sort them by timestamp before passing them to the consumer
    /// (currently, coord.rs).
    struct PendingTail<T> {
        /// `frontiers[i]` is an antichain representing the times at which we may
        /// still receive results from worker `i`.
        frontiers: HashMap<usize, Antichain<T>>,
        /// Consolidated frontiers
        change_batch: ChangeBatch<T>,
        /// The results (possibly unsorted) which have not yet been delivered.
        buffer: Vec<(T, Row, Diff)>,
        /// The number of unique shard IDs expected.
        parts: usize,
        /// The last progress message that has been reported to the consumer.
        reported_frontier: Antichain<T>,
    }

    impl<T: timely::progress::Timestamp + Copy> PendingTail<T> {
        /// Return all the updates with time less than `upper`,
        /// in sorted and consolidated representation.
        pub fn consolidate_up_to(&mut self, upper: Antichain<T>) -> Vec<(T, Row, Diff)> {
            differential_dataflow::consolidation::consolidate_updates(&mut self.buffer);
            let split_point = self
                .buffer
                .partition_point(|(t, _, _)| !upper.less_equal(t));
            self.buffer.drain(0..split_point).collect()
        }

        pub fn push_data<I: IntoIterator<Item = (T, Row, Diff)>>(&mut self, data: I) {
            self.buffer.extend(data);
        }

        pub fn record_progress(
            &mut self,
            shard_id: usize,
            progress: Antichain<T>,
        ) -> Option<Antichain<T>> {
            // In the future, we can probably replace all this logic with the use of a `MutableAntichain`.
            // We would need to have the tail workers report both their old frontier and their new frontier, rather
            // than just the latter. But this will all be subsumed anyway by the proposed refactor to make TAILs
            // behave like PEEKs.
            match self.frontiers.entry(shard_id) {
                Entry::Occupied(mut oe) => {
                    for element in oe.get().elements() {
                        self.change_batch.update(*element, -1);
                    }
                    assert!(
                        PartialOrder::less_than(oe.get(), &progress),
                        "Timestamps should advance"
                    );
                    for element in progress.elements() {
                        self.change_batch.update(*element, 1);
                    }
                    oe.insert(progress);
                }
                Entry::Vacant(ve) => {
                    for element in progress.elements() {
                        self.change_batch.update(*element, 1);
                    }
                    ve.insert(progress);
                }
            }
            assert!(self.frontiers.len() <= self.parts);
            if self.frontiers.len() == self.parts {
                self.change_batch.compact();
                let mut min_frontier = Antichain::new();
                if let Some(time) = self.change_batch.iter().next().map(|(time, _)| *time) {
                    min_frontier.insert(time);
                }

                // assert!(self.reported_frontier.less_equal(&min_frontier));
                assert!(PartialOrder::less_equal(
                    &self.reported_frontier,
                    &min_frontier
                ));
                if PartialOrder::less_than(&self.reported_frontier, &min_frontier) {
                    self.reported_frontier = min_frontier.clone();
                    Some(min_frontier)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }

    /// Maintained state for sharded dataflow clients.
    ///
    /// This helper type unifies the responses of multiple partitioned
    /// workers in order to present as a single worker.
    pub struct PartitionedClientState<T = mz_repr::Timestamp> {
        /// Upper frontiers for indexes, sources, and sinks.
        ///
        /// The `Option<ComputeInstanceId>` uses `None` to represent Storage.
        uppers: HashMap<(GlobalId, Option<ComputeInstanceId>), MutableAntichain<T>>,
        /// Pending responses for a peek; returnable once all are available.
        peek_responses: HashMap<u32, HashMap<usize, PeekResponse>>,
        /// Number of parts the state machine represents.
        parts: usize,
        /// Tracks in-progress `TAIL`s, and the stashed rows we are holding
        /// back until their timestamps are complete.
        ///
        /// `None` is a sentinel value indicating that the tail has been dropped and no
        /// further messages should be forwarded.
        ///
        /// The `Option<ComputeInstanceId>` uses `None` to represent Storage.
        pending_tails: HashMap<(GlobalId, Option<ComputeInstanceId>), Option<PendingTail<T>>>,
        /// The next responses to return immediately, if any
        stashed_responses: Fuse<Box<dyn Iterator<Item = Response<T>> + Send>>,
    }

    impl PartitionedClientState {
        /// Instantiates a new client state machine wrapping a number of parts.
        pub fn new(parts: usize) -> Self {
            let stashed_responses: Box<dyn Iterator<Item = Response> + Send> =
                Box::new(None.into_iter());
            Self {
                uppers: Default::default(),
                peek_responses: Default::default(),
                parts,
                pending_tails: Default::default(),
                stashed_responses: stashed_responses.fuse(),
            }
        }

        /// Observes commands that move past, and prepares state for responses.
        ///
        /// In particular, this method installs and removes upper frontier maintenance.
        pub fn observe_command(&mut self, command: &Command) {
            // Temporary storage for identifiers to add to and remove from frontier tracking.
            let mut start = Vec::new();
            let mut cease = Vec::new();
            let instance = if let Command::Compute(_, instance) = command {
                Some(*instance)
            } else {
                None
            };
            command.frontier_tracking(&mut start, &mut cease);
            // Apply the determined effects of the command to `self.uppers`.
            for id in start.into_iter() {
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((
                    <Timestamp as timely::progress::Timestamp>::minimum(),
                    self.parts as i64,
                )));
                let previous = self.uppers.insert((id, instance), frontier);
                assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", id, command);
            }
            for id in cease.into_iter() {
                let previous = self.uppers.remove(&(id, instance));
                if previous.is_none() {
                    debug!("Protocol error: ceasing frontier tracking for absent identifier {:?} due to command {:?}", id, command);
                }
            }
        }

        /// Absorbs a response, and produces response that should be emitted.
        pub fn absorb_response(
            &mut self,
            shard_id: usize,
            message: Response,
        ) -> Box<dyn Iterator<Item = Response> + Send> {
            match message {
                Response::Compute(ComputeResponse::FrontierUppers(mut list), instance) => {
                    for (id, changes) in list.iter_mut() {
                        if let Some(frontier) = self.uppers.get_mut(&(*id, Some(instance))) {
                            let iter = frontier.update_iter(changes.drain());
                            changes.extend(iter);
                        } else {
                            changes.clear();
                        }
                    }

                    // The following block implements a `list.retain()` of non-empty change batches.
                    // This is more verbose than `list.retain()` because that method cannot mutate
                    // its argument, and `is_empty()` may need to do this (as it is lazily compacted).
                    let mut cursor = 0;
                    while let Some((_id, changes)) = list.get_mut(cursor) {
                        if changes.is_empty() {
                            list.swap_remove(cursor);
                        } else {
                            cursor += 1;
                        }
                    }

                    Box::new(
                        Some(Response::Compute(
                            ComputeResponse::FrontierUppers(list),
                            instance,
                        ))
                        .into_iter(),
                    )
                }
                // Avoid multiple retractions of minimum time, to present as updates from one worker.
                Response::Storage(StorageResponse::TimestampBindings(mut feedback)) => {
                    for (id, changes) in feedback.changes.iter_mut() {
                        if let Some(frontier) = self.uppers.get_mut(&(*id, None)) {
                            let iter = frontier.update_iter(changes.drain());
                            changes.extend(iter);
                        } else {
                            changes.clear();
                        }
                    }
                    // The following block implements a `list.retain()` of non-empty change batches.
                    // This is more verbose than `list.retain()` because that method cannot mutate
                    // its argument, and `is_empty()` may need to do this (as it is lazily compacted).
                    let mut cursor = 0;
                    while let Some((_id, changes)) = feedback.changes.get_mut(cursor) {
                        if changes.is_empty() {
                            feedback.changes.swap_remove(cursor);
                        } else {
                            cursor += 1;
                        }
                    }

                    Box::new(
                        Some(Response::Storage(StorageResponse::TimestampBindings(
                            feedback,
                        )))
                        .into_iter(),
                    )
                }
                Response::Compute(
                    ComputeResponse::PeekResponse(connection, response),
                    instance,
                ) => {
                    // Incorporate new peek responses; awaiting all responses.
                    let entry = self
                        .peek_responses
                        .entry(connection)
                        .or_insert_with(Default::default);
                    let novel = entry.insert(shard_id, response);
                    assert!(novel.is_none(), "Duplicate peek response");
                    // We may be ready to respond.
                    if entry.len() == self.parts {
                        let mut response = PeekResponse::Rows(Vec::new());
                        for (_part, r) in std::mem::take(entry).into_iter() {
                            response = match (response, r) {
                                (_, PeekResponse::Canceled) => PeekResponse::Canceled,
                                (PeekResponse::Canceled, _) => PeekResponse::Canceled,
                                (_, PeekResponse::Error(e)) => PeekResponse::Error(e),
                                (PeekResponse::Error(e), _) => PeekResponse::Error(e),
                                (PeekResponse::Rows(mut rows), PeekResponse::Rows(r)) => {
                                    rows.extend(r.into_iter());
                                    PeekResponse::Rows(rows)
                                }
                            };
                        }
                        self.peek_responses.remove(&connection);
                        Box::new(
                            Some(Response::Compute(
                                ComputeResponse::PeekResponse(connection, response),
                                instance,
                            ))
                            .into_iter(),
                        )
                    } else {
                        Box::new(None.into_iter())
                    }
                }
                Response::Compute(ComputeResponse::TailResponse(id, response), instance) => {
                    let maybe_entry = self
                        .pending_tails
                        .entry((id, Some(instance)))
                        .or_insert_with(|| {
                            Some(PendingTail {
                                frontiers: HashMap::new(),
                                change_batch: Default::default(),
                                buffer: Vec::new(),
                                parts: self.parts,
                                reported_frontier: Antichain::from_elem(0),
                            })
                        });

                    let entry = match maybe_entry {
                        None => {
                            // This tail has been dropped;
                            // we should permanently block
                            // any messages from it
                            return Box::new(None.into_iter());
                        }
                        Some(entry) => entry,
                    };

                    let responses: Box<dyn Iterator<Item = Response> + Send> = match response {
                        TailResponse::Progress(frontier) => {
                            if let Some(new_frontier) = entry.record_progress(shard_id, frontier) {
                                let data = entry.consolidate_up_to(new_frontier.clone());
                                let progress_response = TailResponse::Progress(new_frontier);
                                if data.is_empty() {
                                    Box::new(
                                        Some(Response::Compute(
                                            ComputeResponse::TailResponse(id, progress_response),
                                            instance,
                                        ))
                                        .into_iter(),
                                    )
                                } else {
                                    // Return the data first, then the progress message.
                                    Box::new(
                                        [
                                            Response::Compute(
                                                ComputeResponse::TailResponse(
                                                    id,
                                                    TailResponse::Rows(data),
                                                ),
                                                instance,
                                            ),
                                            Response::Compute(
                                                ComputeResponse::TailResponse(
                                                    id,
                                                    progress_response,
                                                ),
                                                instance,
                                            ),
                                        ]
                                        .into_iter(),
                                    )
                                }
                            } else {
                                Box::new(None.into_iter())
                            }
                        }
                        TailResponse::Rows(rows) => {
                            entry.push_data(rows);
                            Box::new(None.into_iter())
                        }
                        TailResponse::Dropped => {
                            *maybe_entry = None;
                            Box::new(
                                Some(Response::Compute(
                                    ComputeResponse::TailResponse(id, TailResponse::Dropped),
                                    instance,
                                ))
                                .into_iter(),
                            )
                        }
                    };
                    responses
                }
            }
        }
    }
}

/// A client backed by a process-local timely worker thread.
pub mod process_local {
    use async_trait::async_trait;

    use super::{Client, Command, Response};

    /// A client to a dataflow server running in the current process.
    pub struct ProcessLocal {
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,
        worker_tx: crossbeam_channel::Sender<Command>,
        worker_thread: std::thread::Thread,
    }

    #[async_trait(?Send)]
    impl Client for ProcessLocal {
        async fn send(&mut self, cmd: Command) -> Result<(), anyhow::Error> {
            self.worker_tx
                .send(cmd)
                .expect("worker command receiver should not drop first");
            self.worker_thread.unpark();
            Ok(())
        }

        async fn recv(&mut self) -> Option<Response> {
            self.feedback_rx.recv().await
        }
    }

    impl ProcessLocal {
        /// Create a new instance of [ProcessLocal] from its parts.
        pub fn new(
            feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,
            worker_tx: crossbeam_channel::Sender<Command>,
            worker_thread: std::thread::Thread,
        ) -> Self {
            Self {
                feedback_rx,
                worker_tx,
                worker_thread,
            }
        }
    }

    // We implement `Drop` so that we can wake each of the workers and have them notice the drop.
    impl Drop for ProcessLocal {
        fn drop(&mut self) {
            // Drop the worker handle.
            let (tx, _rx) = crossbeam_channel::unbounded();
            self.worker_tx = tx;
            // Unpark the thread once the handle is dropped, so that it can observe the emptiness.
            self.worker_thread.unpark();
        }
    }
}
