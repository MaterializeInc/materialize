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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use differential_dataflow::Hashable;
use enum_iterator::IntoEnumIterator;
use enum_kinds::EnumKind;
use futures::Stream;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use tokio::sync::mpsc;
use tracing::{error, trace};
use uuid::Uuid;

use crate::logging::LoggingConfig;
use crate::{
    sources::{MzOffset, SourceDesc},
    DataflowDescription, PeekResponse, SourceInstanceDesc, TailResponse, Update,
};
use mz_expr::{GlobalId, PartitionId, RowSetFinishing};
use mz_ore::cast::CastFrom;
use mz_repr::Row;

pub mod controller;
pub use controller::Controller;

pub mod replicated;

/// Explicit instructions for timely dataflow workers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command<T = mz_repr::Timestamp> {
    /// A compute command.
    Compute(ComputeCommand<T>, ComputeInstanceId),
    /// A storage command.
    Storage(StorageCommand<T>),
}

/// An abstraction allowing us to name difference compute instances.
// TODO(benesch): this is an `i64` rather than a `u64` because SQLite does not
// support natively storing `u64`. Revisit this before shipping Platform, as we
// might not like to bake in this decision based on a SQLite limitation.
// See #11123.
pub type ComputeInstanceId = i64;
/// A default value whose use we can track down and remove later.
pub const DEFAULT_COMPUTE_INSTANCE_ID: ComputeInstanceId = 1;

/// Instance configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InstanceConfig {
    /// In-process virtual instance, likely the default instance
    Virtual,
    /// Out-of-process named instance
    Remote {
        /// A map from replica name to hostnames.
        replicas: BTreeMap<String, BTreeSet<String>>,
    },
    /// A remote but managed instance.
    Managed {
        /// The size of the cluster.
        size: String,
    },
}

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
        /// Used in responses and cancellation requests.
        uuid: Uuid,
        /// The logical timestamp at which the arrangement is queried.
        timestamp: T,
        /// Actions to apply to the result set before returning them.
        finishing: RowSetFinishing,
        /// Linear operation to apply in-line on each result.
        map_filter_project: mz_expr::SafeMfpPlan,
    },
    /// Cancel the peeks associated with the given `uuids`.
    CancelPeeks {
        /// The identifiers of the peek requests to cancel.
        uuids: BTreeSet<Uuid>,
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
            ComputeCommandKind::CancelPeeks => "cancel_peeks",
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
            /* dataflow_id */ uuid::Uuid,
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
                            let build_part = build_desc.plan.partition_among(parts);
                            for (plan, objects_to_build) in
                                build_part.into_iter().zip(builds_parts.iter_mut())
                            {
                                objects_to_build.push(crate::BuildDesc {
                                    id: build_desc.id,
                                    plan,
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
                    for update in updates {
                        let part = usize::cast_from(update.row.hashed()) % parts;
                        updates_parts[part].push(update);
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
            Command::Compute(command, _instance) => command.frontier_tracking(start, cease),
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

    /// Obtain the instance for this command, or `None` to indicate a storage command.
    pub fn instance(&self) -> Option<ComputeInstanceId> {
        match self {
            Command::Compute(_, instance) => Some(*instance),
            Command::Storage(_) => None,
        }
    }
}

impl<T> ComputeCommand<T> {
    /// Indicates which global ids should start and cease frontier tracking.
    ///
    /// Identifiers added to `start` will install frontier tracking, and indentifiers
    /// added to `cease` will uninstall frontier tracking.
    pub fn frontier_tracking(&self, start: &mut Vec<GlobalId>, cease: &mut Vec<GlobalId>) {
        match self {
            ComputeCommand::CreateDataflows(dataflows) => {
                for dataflow in dataflows.iter() {
                    for (sink_id, _) in dataflow.sink_exports.iter() {
                        start.push(*sink_id)
                    }
                    for (index_id, _) in dataflow.index_exports.iter() {
                        start.push(*index_id);
                    }
                }
            }
            ComputeCommand::AllowCompaction(frontiers) => {
                for (id, frontier) in frontiers.iter() {
                    if frontier.is_empty() {
                        cease.push(*id);
                    }
                }
            }
            ComputeCommand::CreateInstance(logging) => {
                if let Some(logging_config) = logging {
                    start.extend(logging_config.log_identifiers());
                }
            }
            _ => {
                // Other commands have no known impact on frontier tracking.
            }
        }
    }
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
    /// response or cancelation.
    ///
    /// Returns the number of distinct commands that remain.
    pub fn reduce(&mut self, peeks: &std::collections::HashSet<uuid::Uuid>) -> usize {
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
                peek @ ComputeCommand::Peek { .. } => {
                    // We could pre-filter here, but seems hard to access `uuid`
                    // and take ownership of `peek` at the same time.
                    live_peeks.push(peek);
                }
                ComputeCommand::CancelPeeks { mut uuids } => {
                    uuids.retain(|uuid| peeks.contains(uuid));
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
        live_peeks.retain(|peek| {
            if let ComputeCommand::Peek { uuid, .. } = peek {
                peeks.contains(uuid)
            } else {
                unreachable!()
            }
        });

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
        self.commands.extend(live_peeks);
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

impl<T> Response<T> {
    /// Obtain the instance this response comes from, or `None` to indicate a storage response.
    pub fn instance(&self) -> Option<ComputeInstanceId> {
        match self {
            Response::Compute(_, instance) => Some(*instance),
            Response::Storage(_) => None,
        }
    }
}

/// Responses that the compute nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComputeResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<T>)>),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(Uuid, PeekResponse),
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
#[async_trait]
pub trait GenericClient<C, R>: fmt::Debug + Send {
    /// Sends a command to the dataflow server.
    ///
    /// The command can error for various reasons.
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error>;

    /// Receives the next response from the dataflow server.
    ///
    /// This method blocks until the next response is available, or, if the
    /// dataflow server has been shut down, returns `None`.
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error>;

    /// Returns an adapter that treats the client as a stream.
    ///
    /// The stream produces the responses that would be produced by repeated
    /// calls to `recv`.
    fn as_stream<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<R, anyhow::Error>> + Send + 'a>>
    where
        R: Send + 'a,
    {
        Box::pin(async_stream::stream! {
            loop {
                match self.recv().await {
                    Ok(Some(response)) => yield Ok(response),
                    Err(error) => yield Err(error),
                    Ok(None) => { return; }
                }
            }
        })
    }
}

/// A client to a combined storage and compute server.
pub trait Client<T = mz_repr::Timestamp>: GenericClient<Command<T>, Response<T>> {}

impl<C, T> Client<T> for C where C: GenericClient<Command<T>, Response<T>> {}

/// A client to a storage server.
pub trait StorageClient<T = mz_repr::Timestamp>:
    GenericClient<StorageCommand<T>, StorageResponse<T>>
{
}

impl<C, T> StorageClient<T> for C where C: GenericClient<StorageCommand<T>, StorageResponse<T>> {}

/// A client to a compute server.
pub trait ComputeClient<T = mz_repr::Timestamp>:
    GenericClient<ComputeCommand<T>, ComputeResponse<T>>
{
}

impl<C, T> ComputeClient<T> for C where C: GenericClient<ComputeCommand<T>, ComputeResponse<T>> {}

#[async_trait]
impl<C, R> GenericClient<C, R> for Box<dyn GenericClient<C, R>>
where
    C: Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        (**self).recv().await
    }
}

#[async_trait]
impl<T: Send> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for Box<dyn ComputeClient<T>> {
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        (**self).recv().await
    }
}

/// A convenience type for compatibility.
#[derive(Debug)]
pub struct LocalClient<T = mz_repr::Timestamp> {
    client: partitioned::Partitioned<process_local::ProcessLocal<T>, T>,
}

impl<T> LocalClient<T>
where
    T: timely::progress::Timestamp + Copy,
{
    pub fn new(
        feedback_rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<Response<T>>>,
        worker_txs: Vec<crossbeam_channel::Sender<Command<T>>>,
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

#[async_trait]
impl<T> GenericClient<Command<T>, Response<T>> for LocalClient<T>
where
    T: timely::progress::Timestamp + Copy,
{
    async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error> {
        trace!("SEND dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("RECV dataflow response: {:?}", response);
        response
    }
}

/// A convenience type for compatibility.
#[derive(Debug)]
pub struct RemoteClient<T> {
    client: partitioned::Partitioned<tcp::TcpClient<T>, T>,
}

impl<T> RemoteClient<T>
where
    T: timely::progress::Timestamp + Copy,
{
    /// Construct a client backed by multiple tcp connections
    pub fn new(addrs: &[impl tokio::net::ToSocketAddrs + std::fmt::Display]) -> Self {
        let mut remotes = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            remotes.push(tcp::TcpClient::new(addr.to_string()));
        }
        Self {
            client: partitioned::Partitioned::new(remotes),
        }
    }

    /// Construct a client backed by multiple tcp connections
    pub async fn connect(&mut self) {
        // TODO: initiate connections concurrently.
        for remote in self.client.shards.iter_mut() {
            remote.connect().await;
        }
    }
}

#[async_trait]
impl<T> GenericClient<Command<T>, Response<T>> for RemoteClient<T>
where
    T: timely::progress::Timestamp + Copy + Unpin,
{
    async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error> {
        trace!("Sending dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("Receiving dataflow response: {:?}", response);
        response
    }
}

/// An adapter that converts a client to a compute client.
#[derive(Debug)]
pub struct ComputeWrapperClient<C> {
    client: C,
    instance: ComputeInstanceId,
}

impl<C> ComputeWrapperClient<C> {
    /// Constructs a new compute wrapper client for the specified compute
    /// instance ID.
    pub fn new(client: C, instance: ComputeInstanceId) -> ComputeWrapperClient<C> {
        ComputeWrapperClient { instance, client }
    }
}

#[async_trait]
impl<C, T> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for ComputeWrapperClient<C>
where
    C: GenericClient<Command<T>, Response<T>>,
    T: fmt::Debug + Send + 'static,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        self.client.send(Command::Compute(cmd, self.instance)).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        match self.client.recv().await? {
            Some(Response::Compute(res, instance)) => {
                assert_eq!(self.instance, instance);
                Ok(Some(res))
            }
            res @ Some(Response::Storage(_)) => {
                panic!(
                    "ComputeWrapperClient unexpectedly received storage response: {:?}",
                    res
                );
            }
            None => Ok(None),
        }
    }
}

/// An adapter that converts a client to a storage client.
#[derive(Debug)]
pub struct StorageWrapperClient<C> {
    client: C,
}

impl<C> StorageWrapperClient<C> {
    /// Constructs a new storage wrapper client.
    pub fn new(client: C) -> StorageWrapperClient<C> {
        StorageWrapperClient { client }
    }
}

#[async_trait]
impl<C, T> GenericClient<StorageCommand<T>, StorageResponse<T>> for StorageWrapperClient<C>
where
    C: GenericClient<Command<T>, Response<T>>,
    T: fmt::Debug + Send + 'static,
{
    async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
        self.client.send(Command::Storage(cmd)).await
    }

    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        match self.client.recv().await? {
            Some(Response::Storage(res)) => Ok(Some(res)),
            res @ Some(Response::Compute(..)) => {
                panic!(
                    "StorageWrapperClient unexpectedly received compute response: {:?}",
                    res
                );
            }
            None => Ok(None),
        }
    }
}

/// Clients whose implementation is partitioned across a set of subclients (e.g. timely workers).
pub mod partitioned {

    use std::collections::HashMap;

    use async_trait::async_trait;
    use futures::StreamExt;

    use mz_expr::GlobalId;
    use mz_repr::{Diff, Row};
    use tokio_stream::StreamMap;
    use tracing::debug;

    use crate::client::{ComputeInstanceId, GenericClient};
    use crate::TailResponse;

    use super::{Client, ComputeResponse, StorageResponse};
    use super::{Command, ComputeCommand, PeekResponse, Response};

    /// A client whose implementation is sharded across a number of other clients.
    ///
    /// Such a client needs to broadcast (partitioned) commands to all of its clients,
    /// and await responses from each of the client shards before it can respond.
    #[derive(Debug)]
    pub struct Partitioned<C, T> {
        /// The individual shards representing per-worker clients.
        pub shards: Vec<C>,
        /// The number of errors observed from underlying clients.
        seen_errors: usize,
        /// The state per compute instance. `None` represents Storage.
        state: HashMap<Option<ComputeInstanceId>, PartitionedClientState<T>>,
    }

    impl<C, T> Partitioned<C, T>
    where
        T: timely::progress::Timestamp + Copy,
    {
        /// Create a client partitioned across multiple client shards.
        pub fn new(shards: Vec<C>) -> Self {
            // TODO: Have Storage announce its creation like compute.
            let mut state: HashMap<_, _> = Default::default();
            state.insert(None, PartitionedClientState::new(shards.len()));

            Self {
                shards,
                state,
                seen_errors: 0,
            }
        }
    }

    #[async_trait]
    impl<C, T> GenericClient<Command<T>, Response<T>> for Partitioned<C, T>
    where
        C: Client<T>,
        T: timely::progress::Timestamp + Copy,
    {
        async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error> {
            let instance = cmd.instance();
            // A `CreateInstance` command indicates re-initialization.
            if let Command::Compute(ComputeCommand::CreateInstance(_), _instance) = &cmd {
                self.state
                    .insert(instance, PartitionedClientState::new(self.shards.len()));
            }
            self.state
                .get_mut(&instance)
                .expect("command for non-existing instance")
                .observe_command(&cmd);
            if let Command::Compute(ComputeCommand::DropInstance, _instance) = &cmd {
                self.state.remove(&instance);
            }
            let cmd_parts = cmd.partition_among(self.shards.len());
            for (shard, cmd_part) in self.shards.iter_mut().zip(cmd_parts) {
                shard.send(cmd_part).await?;
            }
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
            let parts = self.shards.len();
            let mut stream: StreamMap<_, _> = self
                .shards
                .iter_mut()
                .map(|shard| shard.as_stream())
                .enumerate()
                .collect();
            while let Some((index, response)) = stream.next().await {
                match response {
                    Err(e) => {
                        // Only emit one out of every `parts` errors. (If one
                        // underlying client observes an error, we expect all of
                        // the other clients to observe the same error.)
                        self.seen_errors += 1;
                        if (self.seen_errors % parts) == 0 {
                            return Err(e);
                        } else {
                            return Ok(None);
                        }
                    }
                    Ok(response) => match self.state.get_mut(&response.instance()) {
                        Some(state) => {
                            if let Some(response) = state.absorb_response(index, response) {
                                return response.map(Some);
                            }
                        }
                        None => {
                            debug!(
                                "Response for missing instance {instance:?}",
                                instance = response.instance()
                            );
                        }
                    },
                }
            }
            // Indicate completion of the communication.
            Ok(None)
        }
    }

    use timely::progress::frontier::MutableAntichain;
    use uuid::Uuid;

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
        peek_responses: HashMap<Uuid, HashMap<usize, PeekResponse>>,
        /// Number of parts the state machine represents.
        parts: usize,
        /// Tracks in-progress `TAIL`s, and the stashed rows we are holding
        /// back until their timestamps are complete.
        pending_tails: HashMap<
            (GlobalId, Option<ComputeInstanceId>),
            Option<(MutableAntichain<T>, Vec<(T, Row, Diff)>)>,
        >,
    }

    // Custom Debug implementation to account for `Box<dyn Iterator>>` not being `Debug`.
    impl<T: std::fmt::Debug> std::fmt::Debug for PartitionedClientState<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PartitionedClientState<T>")
                .field("uppers", &self.uppers)
                .field("peek_responses", &self.peek_responses)
                .field("parts", &self.parts)
                .field("pending_tails", &self.pending_tails)
                .finish_non_exhaustive()
        }
    }

    impl<T> PartitionedClientState<T>
    where
        T: timely::progress::Timestamp + Copy,
    {
        /// Instantiates a new client state machine wrapping a number of parts.
        pub fn new(parts: usize) -> Self {
            Self {
                uppers: Default::default(),
                peek_responses: Default::default(),
                parts,
                pending_tails: Default::default(),
            }
        }

        /// Observes commands that move past, and prepares state for responses.
        ///
        /// In particular, this method installs and removes upper frontier maintenance.
        pub fn observe_command(&mut self, command: &Command<T>) {
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
                frontier.update_iter(Some((T::minimum(), self.parts as i64)));
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
            message: Response<T>,
        ) -> Option<Result<Response<T>, anyhow::Error>> {
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

                    if list.is_empty() {
                        None
                    } else {
                        Some(Ok(Response::Compute(
                            ComputeResponse::FrontierUppers(list),
                            instance,
                        )))
                    }
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

                    Some(Ok(Response::Storage(StorageResponse::TimestampBindings(
                        feedback,
                    ))))
                }
                Response::Compute(ComputeResponse::PeekResponse(uuid, response), instance) => {
                    // Incorporate new peek responses; awaiting all responses.
                    let entry = self
                        .peek_responses
                        .entry(uuid)
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
                        self.peek_responses.remove(&uuid);
                        Some(Ok(Response::Compute(
                            ComputeResponse::PeekResponse(uuid, response),
                            instance,
                        )))
                    } else {
                        None
                    }
                }
                Response::Compute(ComputeResponse::TailResponse(id, response), instance) => {
                    let maybe_entry = self
                        .pending_tails
                        .entry((id, Some(instance)))
                        .or_insert_with(|| {
                            let mut frontier = MutableAntichain::new();
                            frontier
                                .update_iter(std::iter::once((T::minimum(), self.parts as i64)));
                            Some((frontier, Vec::new()))
                        });

                    let entry = match maybe_entry {
                        None => {
                            // This tail has been dropped;
                            // we should permanently block
                            // any messages from it
                            return None;
                        }
                        Some(entry) => entry,
                    };

                    use crate::TailBatch;
                    use differential_dataflow::consolidation::consolidate_updates;
                    match response {
                        TailResponse::Batch(TailBatch {
                            lower,
                            upper,
                            mut updates,
                        }) => {
                            let old_frontier = entry.0.frontier().to_owned();
                            entry.0.update_iter(lower.iter().map(|t| (t.clone(), -1)));
                            entry.0.update_iter(upper.iter().map(|t| (t.clone(), 1)));
                            entry.1.append(&mut updates);
                            let new_frontier = entry.0.frontier().to_owned();
                            if old_frontier != new_frontier {
                                consolidate_updates(&mut entry.1);
                                let mut ship = Vec::new();
                                let mut keep = Vec::new();
                                for (time, data, diff) in entry.1.drain(..) {
                                    if new_frontier.less_equal(&time) {
                                        keep.push((time, data, diff));
                                    } else {
                                        ship.push((time, data, diff));
                                    }
                                }
                                entry.1 = keep;
                                Some(Ok(Response::Compute(
                                    ComputeResponse::TailResponse(
                                        id,
                                        TailResponse::Batch(TailBatch {
                                            lower: old_frontier,
                                            upper: new_frontier,
                                            updates: ship,
                                        }),
                                    ),
                                    instance,
                                )))
                            } else {
                                None
                            }
                        }
                        TailResponse::Dropped => {
                            *maybe_entry = None;
                            Some(Ok(Response::Compute(
                                ComputeResponse::TailResponse(id, TailResponse::Dropped),
                                instance,
                            )))
                        }
                    }
                }
            }
        }
    }
}

/// A client backed by a process-local timely worker thread.
pub mod process_local {
    use std::fmt;

    use async_trait::async_trait;

    use super::{Command, GenericClient, Response};

    /// A client to a dataflow server running in the current process.
    #[derive(Debug)]
    pub struct ProcessLocal<T> {
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response<T>>,
        worker_tx: crossbeam_channel::Sender<Command<T>>,
        worker_thread: std::thread::Thread,
    }

    #[async_trait]
    impl<T> GenericClient<Command<T>, Response<T>> for ProcessLocal<T>
    where
        T: fmt::Debug + Send,
    {
        async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error> {
            self.worker_tx
                .send(cmd)
                .expect("worker command receiver should not drop first");
            self.worker_thread.unpark();
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
            Ok(self.feedback_rx.recv().await)
        }
    }

    impl<T> ProcessLocal<T> {
        /// Create a new instance of [ProcessLocal] from its parts.
        pub fn new(
            feedback_rx: tokio::sync::mpsc::UnboundedReceiver<Response<T>>,
            worker_tx: crossbeam_channel::Sender<Command<T>>,
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
    impl<T> Drop for ProcessLocal<T> {
        fn drop(&mut self) {
            // Drop the worker handle.
            let (tx, _rx) = crossbeam_channel::unbounded();
            self.worker_tx = tx;
            // Unpark the thread once the handle is dropped, so that it can observe the emptiness.
            self.worker_thread.unpark();
        }
    }
}

/// A client to a remote dataflow server.
pub mod tcp {
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use tokio::io::{self, AsyncRead, AsyncWrite};
    use tokio::net::TcpStream;
    use tokio::time::{self, Instant};
    use tokio_serde::formats::Bincode;
    use tokio_util::codec::LengthDelimitedCodec;

    use crate::client::{Command, GenericClient, Response};

    enum TcpConn<T> {
        Disconnected,
        Connecting(Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>),
        Backoff(Instant),
        Connected(FramedClient<TcpStream, T>),
    }

    impl<T> fmt::Debug for TcpConn<T> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("TcpConn")
        }
    }

    /// A client to a remote dataflow server.
    ///
    /// If the client experiences errors, it will attempt a reconnection in the `recv` method and
    /// produce an error for the call in which that reconnection happens, allowing a bearer to
    /// re-issue commands. As the reconnection happens in `recv()`, the bearer is advised to use
    /// a `select` style construct to avoid suspending their task by a call to `recv()`.
    #[derive(Debug)]
    pub struct TcpClient<T> {
        connection: TcpConn<T>,
        addr: String,
    }

    impl<T> TcpClient<T> {
        /// Creates a new `TcpClient` initially in a disconnected state.
        ///
        /// Use the `connect()` method to put the client into a connected state.
        pub fn new(addr: String) -> TcpClient<T> {
            Self {
                connection: TcpConn::Disconnected,
                addr,
            }
        }

        /// Reports whether the client is actively connected.
        pub fn connected(&self) -> bool {
            matches!(self.connection, TcpConn::Connected(_))
        }

        /// Connects the underlying `connection`.
        pub async fn connect(&mut self) {
            // This is written in state-machine style to be cancellation safe.
            loop {
                match &mut self.connection {
                    TcpConn::Disconnected => {
                        let connecting = Box::pin(TcpStream::connect(self.addr.clone()));
                        self.connection = TcpConn::Connecting(connecting);
                    }
                    TcpConn::Connecting(connecting) => match connecting.await {
                        Ok(connection) => {
                            tracing::info!("Reconnected to {}", self.addr);
                            self.connection = TcpConn::Connected(framed_client(connection));
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Error connecting to {}: {e}; reconnecting in 1s",
                                self.addr
                            );
                            let deadline = Instant::now() + Duration::from_secs(1);
                            self.connection = TcpConn::Backoff(deadline);
                        }
                    },
                    TcpConn::Backoff(deadline) => {
                        time::sleep_until(*deadline).await;
                        self.connection = TcpConn::Disconnected;
                    }
                    TcpConn::Connected(_) => break,
                }
            }
        }
    }

    #[async_trait]
    impl<T> GenericClient<Command<T>, Response<T>> for TcpClient<T>
    where
        T: timely::progress::Timestamp + Copy + Unpin,
    {
        async fn send(&mut self, cmd: Command<T>) -> Result<(), anyhow::Error> {
            if let TcpConn::Connected(connection) = &mut self.connection {
                let result = connection.send(cmd).await;
                if result.is_err() {
                    self.connection = TcpConn::Disconnected;
                }
                Ok(result?)
            } else {
                Err(anyhow::anyhow!("Sent into disconnected channel"))
            }
        }

        async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
            if let TcpConn::Connected(connection) = &mut self.connection {
                match connection.next().await {
                    Some(Ok(response)) => Ok(Some(response)),
                    _ => {
                        self.connection = TcpConn::Disconnected;
                        self.connect().await;
                        Err(anyhow::anyhow!("Connection severed; reconnected"))
                    }
                }
            } else {
                self.connect().await;
                Err(anyhow::anyhow!("Connection severed; reconnected"))
            }
        }
    }

    /// A framed connection to a dataflowd server.
    pub type Framed<C, T, U> = tokio_serde::Framed<
        tokio_util::codec::Framed<C, LengthDelimitedCodec>,
        T,
        U,
        Bincode<T, U>,
    >;

    /// A framed connection from the server's perspective.
    pub type FramedServer<C, T> = Framed<C, Command<T>, Response<T>>;

    /// A framed connection from the client's perspective.
    pub type FramedClient<C, T> = Framed<C, Response<T>, Command<T>>;

    fn length_delimited_codec() -> LengthDelimitedCodec {
        // NOTE(benesch): using an unlimited maximum frame length is problematic
        // because Tokio never shrinks its buffer. Sending or receiving one large
        // message of size N means the client will hold on to a buffer of size
        // N forever. We should investigate alternative transport protocols that
        // do not have this limitation.
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(usize::MAX);
        codec
    }

    /// Constructs a framed connection for the server.
    pub fn framed_server<C, T>(conn: C) -> FramedServer<C, T>
    where
        C: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }

    /// Constructs a framed connection for the client.
    pub fn framed_client<C, T>(conn: C) -> FramedClient<C, T>
    where
        C: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }
}

/// A client composed of channels.
mod channel {
    use std::fmt;

    use async_trait::async_trait;
    use derivative::Derivative;

    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

    use crate::client::GenericClient;

    /// A client composed of channels.
    #[derive(Derivative)]
    #[derivative(Debug)]
    pub struct ChannelClient<C1, C2, R> {
        tx: UnboundedSender<C2>,
        rx: UnboundedReceiver<R>,
        #[derivative(Debug = "ignore")]
        command_map: Box<dyn Fn(C1) -> C2 + Send>,
    }

    /// Create a new channel client.
    ///
    /// Commands sent to the client are transformed via `command_map` and then
    /// sent over `tx`.
    ///
    /// Returns the channel client and a channel transmitter that sends
    /// responses to the client.
    impl<C1, C2, R> ChannelClient<C1, C2, R> {
        pub fn new(
            tx: UnboundedSender<C2>,
            command_map: Box<dyn Fn(C1) -> C2 + Send>,
        ) -> (ChannelClient<C1, C2, R>, UnboundedSender<R>) {
            let (response_tx, response_rx) = mpsc::unbounded_channel();
            let client = ChannelClient {
                tx,
                rx: response_rx,
                command_map,
            };
            (client, response_tx)
        }
    }

    #[async_trait]
    impl<C1, C2, R> GenericClient<C1, R> for ChannelClient<C1, C2, R>
    where
        C1: fmt::Debug + Send + 'static,
        C2: fmt::Debug + Send + Sync + 'static,
        R: fmt::Debug + Send,
    {
        async fn send(&mut self, cmd: C1) -> Result<(), anyhow::Error> {
            let cmd = (self.command_map)(cmd);
            self.tx.send(cmd)?;
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
            Ok(self.rx.recv().await)
        }
    }
}

/// A handle to a compute host for virtual compute instances.
pub struct VirtualComputeHost<T> {
    command_tx: mpsc::UnboundedSender<Command<T>>,
    compute_host_tx: mpsc::UnboundedSender<ComputeHostCommand<T>>,
}

impl<T> VirtualComputeHost<T>
where
    T: fmt::Debug + Send + Sync + 'static,
{
    /// Creates a new instance in the virtual compute host.
    ///
    /// The behavior is undefined if a compute instance with the same ID already
    /// exists on the host.
    pub fn create_instance(&self, instance: ComputeInstanceId) -> Box<dyn ComputeClient<T>> {
        let (client, response_tx) = channel::ChannelClient::new(
            self.command_tx.clone(),
            Box::new(move |command| Command::Compute(command, instance)),
        );
        self.compute_host_tx
            .send(ComputeHostCommand::CreateInstance(instance, response_tx))
            .unwrap();
        Box::new(client)
    }

    /// Drops an existing instance on the virtual compute host.
    pub fn drop_instance(&self, instance: ComputeInstanceId) {
        self.compute_host_tx
            .send(ComputeHostCommand::DropInstance(instance))
            .unwrap();
    }
}

#[derive(Debug)]
enum ComputeHostCommand<T> {
    CreateInstance(ComputeInstanceId, mpsc::UnboundedSender<ComputeResponse<T>>),
    DropInstance(ComputeInstanceId),
}

/// Splits a dataflow client into a storage half and a compute half.
///
/// The compute half is a `VirtualComputeHost` which can be used to create
/// and destroy virtual clusters dynamically.
///
/// If the underlying dataflow server is not running both a storage and compute
/// runtime, it is the caller's responsibility to ignore the inactive half.
pub fn split_client<C, T>(mut client: C) -> (Box<dyn StorageClient<T>>, VirtualComputeHost<T>)
where
    C: Client<T> + Send + 'static,
    T: fmt::Debug + Send + Sync + 'static,
{
    let (command_tx, mut command_rx) = mpsc::unbounded_channel();
    let (compute_host_tx, mut compute_host_rx) = mpsc::unbounded_channel();
    let (storage_client, storage_response_tx) = channel::ChannelClient::new(
        command_tx.clone(),
        Box::new(|command| Command::Storage(command)),
    );
    let virtual_compute_host = VirtualComputeHost {
        command_tx,
        compute_host_tx,
    };
    mz_ore::task::spawn(|| "split_client", async move {
        let mut instances = HashMap::new();
        let mut compute_host_alive = true;
        let mut clients_alive = true;
        while compute_host_alive && clients_alive {
            tokio::select! {
                biased;

                cmd = compute_host_rx.recv() => match cmd {
                    Some(ComputeHostCommand::CreateInstance(instance, response_tx)) => {
                        instances.insert(instance, response_tx);
                    }
                    Some(ComputeHostCommand::DropInstance(instance)) => {
                        instances.remove(&instance);
                    }
                    None => compute_host_alive = false,
                },

                Some(cmd) = command_rx.recv() => match client.send(cmd).await {
                    Ok(_) => (),
                    Err(_) => clients_alive = false,
                },

                response = client.recv() => match response {
                    Ok(Some(Response::Storage(res))) => {
                        let _ = storage_response_tx.send(res);
                    }
                    Ok(Some(Response::Compute(res, instance))) => match instances.get(&instance) {
                        None => {
                            error!("received response {res:?} for non-existent compute instance {instance:?}");
                        }
                        Some(response_tx) => {
                            let _ = response_tx.send(res);
                        }
                    },
                    error @ Err(_) => {
                        error.unwrap();
                    },
                    Ok(None) => {
                        // Nothing, is what the prior code did.
                    }
                },
            }
        }
    });
    (Box::new(storage_client), virtual_compute_host)
}
