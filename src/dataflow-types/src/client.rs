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

use enum_iterator::IntoEnumIterator;
use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;

use crate::logging::LoggingConfig;
use crate::{
    sources::MzOffset, sources::SourceConnector, DataflowDescription, PeekResponse, TailResponse,
    Update,
};
use expr::{GlobalId, PartitionId, RowSetFinishing};
use repr::{Row, Timestamp};

pub mod controller;
pub use controller::Controller;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// A compute command.
    Compute(ComputeCommand),
    /// A storage command.
    Storage(StorageCommand),
}

/// Commands related to the computation and maintenance of views.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    ComputeCommandKind,
    derive(IntoEnumIterator),
    doc = "The kind of compute command that was received"
)]
pub enum ComputeCommand {
    /// Create a sequence of dataflows.
    ///
    /// Each of the dataflows must contain `as_of` members that are valid
    /// for each of the referenced arrangements, meaning `AllowCompaction`
    /// should be held back to those values until the command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly maintain the dataflows.
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan>>),
    /// Drop the sinks bound to these names.
    DropSinks(Vec<GlobalId>),
    /// Drop the indexes bound to these namees.
    DropIndexes(Vec<GlobalId>),

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
        timestamp: Timestamp,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: expr::SafeMfpPlan,
    },
    /// Cancel the peek associated with the given `conn_id`.
    CancelPeek {
        /// The identifier of the peek request to cancel.
        conn_id: u32,
    },
    /// Enable compaction in views.
    ///
    /// Each entry in the vector names a view and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    // TODO: Could be called `AllowTraceCompaction` or `AllowArrangementCompaction`?
    AllowIndexCompaction(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Request that the logging sources in the contained configuration are
    /// installed.
    EnableLogging(LoggingConfig),
}

impl ComputeCommandKind {
    /// Returns the name of this kind of command.
    ///
    /// Must remain unique over all variants of `Command`.
    pub fn metric_name(&self) -> &'static str {
        match self {
            // TODO: This breaks metrics. Not sure that's a problem.
            ComputeCommandKind::AllowIndexCompaction => "allow_index_compaction",
            ComputeCommandKind::CancelPeek => "cancel_peek",
            ComputeCommandKind::CreateDataflows => "create_dataflows",
            ComputeCommandKind::DropIndexes => "drop_indexes",
            ComputeCommandKind::DropSinks => "drop_sinks",
            ComputeCommandKind::EnableLogging => "enable_logging",
            ComputeCommandKind::Peek => "peek",
        }
    }
}

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize, EnumKind)]
#[enum_kind(
    StorageCommandKind,
    derive(IntoEnumIterator),
    doc = "the kind of storage command that was received"
)]
pub enum StorageCommand {
    /// Create the enumerated sources, each associated with its identifier.
    ///
    /// For each identifier, there is a source description and a valid `since` frontier.
    CreateSources(
        Vec<(
            GlobalId,
            (crate::types::sources::SourceDesc, Antichain<Timestamp>),
        )>,
    ),

    /// Drop the sources bound to these names.
    DropSources(Vec<GlobalId>),

    /// Insert `updates` into the local input named `id`.
    Insert {
        /// Identifier of the local input.
        id: GlobalId,
        /// A list of updates to be introduced to the input.
        updates: Vec<Update>,
    },
    /// Update durability information for sources.
    ///
    /// Each entry names a source and provides a frontier before which the source can
    /// be exactly replayed across restarts (i.e. we can assign the same timestamps to
    /// all the same data)
    DurabilityFrontierUpdates(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Add a new source to be aware of for timestamping.
    AddSourceTimestamping {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The connector for the timestamped source.
        connector: SourceConnector,
        /// Previously stored timestamp bindings.
        bindings: Vec<(PartitionId, Timestamp, crate::sources::MzOffset)>,
    },
    /// Advance worker timestamp
    AdvanceSourceTimestamp {
        /// The ID of the timestamped source
        id: GlobalId,
        /// The associated update
        update: crate::types::sources::persistence::TimestampSourceUpdate,
    },
    /// Enable compaction in sources.
    ///
    /// Each entry in the vector names a source and provides a frontier after which
    /// accumulations must be correct.
    AllowSourceCompaction(Vec<(GlobalId, Antichain<Timestamp>)>),
    /// Drop all timestamping info for a source
    DropSourceTimestamping {
        /// The ID id of the formerly timestamped source.
        id: GlobalId,
    },
    /// Advance all local inputs to the given timestamp.
    AdvanceAllLocalInputs {
        /// The timestamp to advance to.
        advance_to: Timestamp,
    },
}

impl StorageCommandKind {
    /// Returns the same of this kind of command.
    ///
    /// Must remain unique over all variants of `Command`.
    pub fn metric_name(&self) -> &'static str {
        match self {
            StorageCommandKind::AddSourceTimestamping => "add_source_timestamping",
            StorageCommandKind::AdvanceAllLocalInputs => "advance_all_local_inputs",
            StorageCommandKind::AdvanceSourceTimestamp => "advance_source_timestamp",
            StorageCommandKind::DropSourceTimestamping => "drop_source_timestamping",
            StorageCommandKind::AllowSourceCompaction => "allows_source_compaction",
            StorageCommandKind::CreateSources => "create_sources",
            StorageCommandKind::DropSources => "drop_sources",
            StorageCommandKind::DurabilityFrontierUpdates => "durability_frontier_updates",
            StorageCommandKind::Insert => "insert",
        }
    }
}

impl Command {
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
                Command::Compute(ComputeCommand::CreateDataflows(dataflows)) => {
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
                            });
                        }
                    }
                    dataflows_parts
                        .into_iter()
                        .map(|x| Command::Compute(ComputeCommand::CreateDataflows(x)))
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
            Command::Compute(ComputeCommand::CreateDataflows(dataflows)) => {
                for dataflow in dataflows.iter() {
                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        start.push(*sink_id)
                    }
                    for (index_id, _, _) in dataflow.index_exports.iter() {
                        start.push(*index_id);
                    }
                }
            }
            Command::Compute(ComputeCommand::DropIndexes(index_ids)) => {
                for id in index_ids.iter() {
                    cease.push(*id);
                }
            }
            Command::Compute(ComputeCommand::DropSinks(sink_ids)) => {
                for id in sink_ids.iter() {
                    cease.push(*id);
                }
            }
            Command::Storage(StorageCommand::AddSourceTimestamping { id, .. }) => {
                start.push(*id);
            }
            Command::Storage(StorageCommand::DropSourceTimestamping { id }) => {
                cease.push(*id);
            }
            Command::Compute(ComputeCommand::EnableLogging(logging_config)) => {
                start.extend(logging_config.log_identifiers());
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
            Command::Compute(command) => ComputeCommandKind::from(command).metric_name(),
            Command::Storage(command) => StorageCommandKind::from(command).metric_name(),
        }
    }
}

/// Methods that reflect actions that can be performed against a compute instance.
#[async_trait::async_trait]
pub trait ComputeClient: Client {
    async fn create_dataflows(&mut self, dataflows: Vec<DataflowDescription<crate::plan::Plan>>) {
        self.send(Command::Compute(ComputeCommand::CreateDataflows(dataflows)))
            .await
    }
    async fn drop_sinks(&mut self, sink_identifiers: Vec<GlobalId>) {
        self.send(Command::Compute(ComputeCommand::DropSinks(
            sink_identifiers,
        )))
        .await
    }
    async fn drop_indexes(&mut self, index_identifiers: Vec<GlobalId>) {
        self.send(Command::Compute(ComputeCommand::DropIndexes(
            index_identifiers,
        )))
        .await
    }
    async fn peek(
        &mut self,
        id: GlobalId,
        key: Option<Row>,
        conn_id: u32,
        timestamp: Timestamp,
        finishing: RowSetFinishing,
        map_filter_project: expr::SafeMfpPlan,
    ) {
        self.send(Command::Compute(ComputeCommand::Peek {
            id,
            key,
            conn_id,
            timestamp,
            finishing,
            map_filter_project,
        }))
        .await
    }
    async fn cancel_peek(&mut self, conn_id: u32) {
        self.send(Command::Compute(ComputeCommand::CancelPeek { conn_id }))
            .await
    }
    async fn allow_index_compaction(&mut self, frontiers: Vec<(GlobalId, Antichain<Timestamp>)>) {
        self.send(Command::Compute(ComputeCommand::AllowIndexCompaction(
            frontiers,
        )))
        .await
    }
    async fn enable_logging(&mut self, logging_config: LoggingConfig) {
        self.send(Command::Compute(ComputeCommand::EnableLogging(
            logging_config,
        )))
        .await
    }
}

/// Methods that reflect actions that can be performed against the storage layer.
#[async_trait::async_trait]
pub trait StorageClient: Client {
    async fn create_sources(
        &mut self,
        source_descriptions: Vec<(GlobalId, (crate::sources::SourceDesc, Antichain<Timestamp>))>,
    ) {
        self.send(Command::Storage(StorageCommand::CreateSources(
            source_descriptions,
        )))
        .await
    }
    async fn drop_sources(&mut self, source_identifiers: Vec<GlobalId>) {
        self.send(Command::Storage(StorageCommand::DropSources(
            source_identifiers,
        )))
        .await
    }
    async fn table_insert(&mut self, id: GlobalId, updates: Vec<Update>) {
        self.send(Command::Storage(StorageCommand::Insert { id, updates }))
            .await
    }
    async fn update_durability_frontiers(
        &mut self,
        updates: Vec<(GlobalId, Antichain<Timestamp>)>,
    ) {
        self.send(Command::Storage(StorageCommand::DurabilityFrontierUpdates(
            updates,
        )))
        .await
    }
    async fn add_source_timestamping(
        &mut self,
        id: GlobalId,
        connector: SourceConnector,
        bindings: Vec<(PartitionId, Timestamp, crate::sources::MzOffset)>,
    ) {
        self.send(Command::Storage(StorageCommand::AddSourceTimestamping {
            id,
            connector,
            bindings,
        }))
        .await
    }
    async fn advance_source_timestamp(
        &mut self,
        id: GlobalId,
        update: crate::types::sources::persistence::TimestampSourceUpdate,
    ) {
        self.send(Command::Storage(StorageCommand::AdvanceSourceTimestamp {
            id,
            update,
        }))
        .await
    }
    async fn allow_source_compaction(&mut self, frontiers: Vec<(GlobalId, Antichain<Timestamp>)>) {
        self.send(Command::Storage(StorageCommand::AllowSourceCompaction(
            frontiers,
        )))
        .await
    }
    async fn drop_source_timestamping(&mut self, id: GlobalId) {
        self.send(Command::Storage(StorageCommand::DropSourceTimestamping {
            id,
        }))
        .await
    }
    async fn advance_all_table_timestamps(&mut self, advance_to: Timestamp) {
        self.send(Command::Storage(StorageCommand::AdvanceAllLocalInputs {
            advance_to,
        }))
        .await
    }
}

impl<C: Client> ComputeClient for C {}
impl<C: Client> StorageClient for C {}

/// Data about timestamp bindings that dataflow workers send to the coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampBindingFeedback {
    /// Durability frontier changes
    pub changes: Vec<(GlobalId, ChangeBatch<Timestamp>)>,
    /// Timestamp bindings for all of those frontier changes
    pub bindings: Vec<(GlobalId, PartitionId, Timestamp, MzOffset)>,
}

/// Responses that the worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Response {
    /// A compute response.
    Compute(ComputeResponse),
    /// A storage response.
    Storage(StorageResponse),
}

/// Responses that the compute nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComputeResponse {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(u32, PeekResponse),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse),
}

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageResponse {
    /// A list of identifiers, with prior and new upper frontiers.
    Frontiers(Vec<(GlobalId, ChangeBatch<Timestamp>)>),
    /// Timestamp bindings and prior and new frontiers for those bindings for all
    /// sources
    TimestampBindings(TimestampBindingFeedback),
}

/// A client to a running dataflow server.
#[async_trait::async_trait]
pub trait Client: Send {
    /// Sends a command to the dataflow server.
    async fn send(&mut self, cmd: Command);

    /// Receives the next response from the dataflow server.
    ///
    /// This method blocks until the next response is available, or, if the
    /// dataflow server has been shut down, returns `None`.
    async fn recv(&mut self) -> Option<Response>;
}

#[async_trait::async_trait]
impl Client for Box<dyn Client> {
    async fn send(&mut self, cmd: Command) {
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

#[async_trait::async_trait]
impl Client for LocalClient {
    async fn send(&mut self, cmd: Command) {
        tracing::trace!("SEND dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Option<Response> {
        let response = self.client.recv().await;
        tracing::trace!("RECV dataflow response: {:?}", response);
        response
    }
}

/// Clients whose implementation is partitioned across a set of subclients (e.g. timely workers).
pub mod partitioned {

    use std::collections::HashMap;

    use expr::GlobalId;
    use repr::Timestamp;

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

    #[async_trait::async_trait]
    impl<C: Client> Client for Partitioned<C> {
        async fn send(&mut self, cmd: Command) {
            self.state.observe_command(&cmd);
            let cmd_parts = cmd.partition_among(self.shards.len());
            for (shard, cmd_part) in self.shards.iter_mut().zip(cmd_parts) {
                shard.send(cmd_part).await;
            }
        }

        async fn recv(&mut self) -> Option<Response> {
            use futures::StreamExt;
            self.cursor = (self.cursor + 1) % self.shards.len();
            let mut stream = super::SelectStream::new(&mut self.shards[..], self.cursor);
            while let Some((index, response)) = stream.next().await {
                let message = self.state.absorb_response(index, response);
                if let Some(message) = message {
                    return Some(message);
                }
            }
            // Indicate completion of the communication.
            None
        }
    }

    use timely::progress::{frontier::MutableAntichain, ChangeBatch};

    /// Maintained state for sharded dataflow clients.
    ///
    /// This helper type unifies the responses of multiple partitioned
    /// workers in order to present as a single worker.
    pub struct PartitionedClientState {
        /// Upper frontiers for indexes, sources, and sinks.
        uppers: HashMap<GlobalId, MutableAntichain<Timestamp>>,
        /// Pending responses for a peek; returnable once all are available.
        peek_responses: HashMap<u32, HashMap<usize, PeekResponse>>,
        /// Number of parts the state machine represents.
        parts: usize,
    }

    impl PartitionedClientState {
        /// Instantiates a new client state machine wrapping a number of parts.
        pub fn new(parts: usize) -> Self {
            Self {
                uppers: Default::default(),
                peek_responses: Default::default(),
                parts,
            }
        }

        /// Observes commands that move past, and prepares state for responses.
        ///
        /// In particular, this method installs and removes upper frontier maintenance.
        pub fn observe_command(&mut self, command: &Command) {
            // Temporary storage for identifiers to add to and remove from frontier tracking.
            let mut start = Vec::new();
            let mut cease = Vec::new();
            command.frontier_tracking(&mut start, &mut cease);
            // Apply the determined effects of the command to `self.uppers`.
            for id in start.into_iter() {
                let mut frontier = timely::progress::frontier::MutableAntichain::new();
                frontier.update_iter(Some((
                    <Timestamp as timely::progress::Timestamp>::minimum(),
                    self.parts as i64,
                )));
                let previous = self.uppers.insert(id, frontier);
                assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", id, command);
            }
            for id in cease.into_iter() {
                let previous = self.uppers.remove(&id);
                if previous.is_none() {
                    tracing::debug!("Protocol error: ceasing frontier tracking for absent identifier {:?} due to command {:?}", id, command);
                }
            }
        }

        /// Absorbs a response, and produces response that should be emitted.
        pub fn absorb_response(&mut self, shard_id: usize, message: Response) -> Option<Response> {
            match message {
                Response::Compute(ComputeResponse::FrontierUppers(list)) => {
                    let mut list = fold_changes(&mut self.uppers, list);

                    // Only produce a result if there are any changes to report.
                    if !list.is_empty() {
                        // Put changes in order of `id` for ease of understanding.
                        list.sort_by_key(|(id, _)| *id);
                        Some(Response::Compute(ComputeResponse::FrontierUppers(list)))
                    } else {
                        None
                    }
                }
                // TODO: We could also split `self.uppers` into a `compute_uppers` and
                // `storage_uppers`, but then we'd also have to change `frontier_tracking` to add
                // tracking antichains to the right hash map.
                Response::Storage(StorageResponse::Frontiers(list)) => {
                    let mut list = fold_changes(&mut self.uppers, list);

                    // Only produce a result if there are any changes to report.
                    if !list.is_empty() {
                        // Put changes in order of `id` for ease of understanding.
                        list.sort_by_key(|(id, _)| *id);
                        Some(Response::Storage(StorageResponse::Frontiers(list)))
                    } else {
                        None
                    }
                }
                Response::Compute(ComputeResponse::PeekResponse(connection, response)) => {
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
                        Some(Response::Compute(ComputeResponse::PeekResponse(
                            connection, response,
                        )))
                    } else {
                        None
                    }
                }
                message => {
                    // TimestampBindings and TailResponses are mirrored out,
                    // as they do not seem to contain worker-specific information.
                    Some(message)
                }
            }
        }
    }

    /// Folds changes given in `list` into the matching `MutableAntichain`s in `frontiers`. Only
    /// returns changes when any of the maintained `MutableAntichain` advances.
    fn fold_changes(
        frontiers: &mut HashMap<GlobalId, MutableAntichain<Timestamp>>,
        mut list: Vec<(GlobalId, ChangeBatch<Timestamp>)>,
    ) -> Vec<(GlobalId, ChangeBatch<Timestamp>)> {
        // Fold updates into the maintained antichain, and report
        // any net changes to the minimal antichain itself.
        let mut reactions = ChangeBatch::new();
        for (id, changes) in list.iter_mut() {
            // We may receive changes to identifiers that are no longer tracked;
            // do not worry about them in that case (a benign race condition).
            if let Some(frontier) = frontiers.get_mut(id) {
                for (time, diff) in frontier.update_iter(changes.drain()) {
                    reactions.update(time, diff);
                }
                std::mem::swap(changes, &mut reactions);
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

        list
    }
}

/// A client backed by a process-local timely worker thread.
pub mod process_local {

    use super::{Client, Command, Response};

    /// A client to a dataflow server running in the current process.
    pub struct ProcessLocal {
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response>,
        worker_tx: crossbeam_channel::Sender<Command>,
        worker_thread: std::thread::Thread,
    }

    #[async_trait::async_trait]
    impl Client for ProcessLocal {
        async fn send(&mut self, cmd: Command) {
            self.worker_tx
                .send(cmd)
                .expect("worker command receiver should not drop first");
            self.worker_thread.unpark();
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
