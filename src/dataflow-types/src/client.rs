// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits and types for controller of the dataflow subsystem.

// This appears to be defective at the moment, with false positives
// for each variant of the `Command` enum, each of which are documented.
// #![warn(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use mz_repr::proto::{ProtoRepr, TryFromProtoError, TryIntoIfSome};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use tracing::trace;
use uuid::Uuid;

use mz_expr::{PartitionId, RowSetFinishing};
use mz_repr::{GlobalId, Row};

use crate::logging::LoggingConfig;
use crate::{
    sources::{MzOffset, SourceDesc},
    DataflowDescription, PeekResponse, SourceInstanceDesc, TailResponse, Update,
};

pub mod controller;
pub use controller::Controller;

use self::controller::storage::CollectionMetadata;
use self::controller::ClusterReplicaSizeConfig;

pub mod partitioned;
pub mod replicated;

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.client.rs"));

/// An abstraction allowing us to name different compute instances.
// TODO(benesch): this is an `i64` rather than a `u64` because SQLite does not
// support natively storing `u64`. Revisit this before shipping Platform, as we
// might not like to bake in this decision based on a SQLite limitation.
// See #11123.
pub type ComputeInstanceId = i64;

/// An abstraction allowing us to name different replicas.
pub type ReplicaId = i64;

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConcreteComputeInstanceReplicaConfig {
    /// Out-of-process replica
    Remote {
        /// A map from replica name to hostnames.
        replicas: BTreeSet<String>,
    },
    /// A remote but managed replica
    Managed {
        /// The size of the replica
        size_config: ClusterReplicaSizeConfig,
    },
}

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
#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peek<T = mz_repr::Timestamp> {
    /// The identifier of the arrangement.
    pub id: GlobalId,
    /// An optional key that should be used for the arrangement.
    pub key: Option<Row>,
    /// The identifier of this peek request.
    ///
    /// Used in responses and cancellation requests.
    #[proptest(strategy = "any_uuid()")]
    pub uuid: Uuid,
    /// The logical timestamp at which the arrangement is queried.
    pub timestamp: T,
    /// Actions to apply to the result set before returning them.
    pub finishing: RowSetFinishing,
    /// Linear operation to apply in-line on each result.
    pub map_filter_project: mz_expr::SafeMfpPlan,
}

fn any_uuid() -> impl Strategy<Value = Uuid> {
    (0..u128::MAX).prop_map(Uuid::from_u128)
}

impl From<&Peek> for ProtoPeek {
    fn from(x: &Peek) -> Self {
        ProtoPeek {
            id: Some((&x.id).into()),
            key: x.key.clone().map(|x| (&x).into()),
            uuid: Some(x.uuid.into_proto()),
            timestamp: x.timestamp,
            finishing: Some((&x.finishing).into()),
            map_filter_project: Some((&x.map_filter_project).into()),
        }
    }
}

impl TryFrom<ProtoPeek> for Peek {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoPeek) -> Result<Self, Self::Error> {
        Ok(Self {
            id: x.id.try_into_if_some("ProtoPeek::id")?,
            key: x.key.map(|x| x.try_into()).transpose()?,
            uuid: Uuid::from_proto(
                x.uuid
                    .ok_or_else(|| TryFromProtoError::missing_field("ProtoPeek::uuid"))?,
            )?,
            timestamp: x.timestamp,
            finishing: x.finishing.try_into_if_some("ProtoPeek::finishing")?,
            map_filter_project: x
                .map_filter_project
                .try_into_if_some("ProtoPeek::map_filter_project")?,
        })
    }
}

/// Commands related to the computation and maintenance of views.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan<T>, CollectionMetadata, T>>),
    /// Enable compaction in compute-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),

    /// Peek at an arrangement.
    Peek(Peek<T>),
    /// Cancel the peeks associated with the given `uuids`.
    CancelPeeks {
        /// The identifiers of the peek requests to cancel.
        uuids: BTreeSet<Uuid>,
    },
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
    /// Additional storage controller metadata needed to ingest this source
    pub storage_metadata: CollectionMetadata,
}

/// A command to render a single source
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderSourcesCommand<T> {
    /// A human-readable name.
    pub debug_name: String,
    /// The dataflow's ID.
    pub dataflow_id: uuid::Uuid,
    /// An optional frontier to which the input should be advanced.
    pub as_of: Option<Antichain<T>>,
    /// Sources instantiations made available to the dataflow.
    pub source_imports: BTreeMap<GlobalId, SourceInstanceDesc<CollectionMetadata>>,
}

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageCommand<T = mz_repr::Timestamp> {
    /// Create the enumerated sources, each associated with its identifier.
    CreateSources(Vec<CreateSourceCommand<T>>),
    /// Render the enumerated sources.
    RenderSources(Vec<RenderSourcesCommand<T>>),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
    /// Append data and advance the frontier of the enumerated collections
    ///
    /// Each entry in the vector names a collection and provides a list of updates to insert and a
    /// frontier that the collection must be advanced to. The times of the updates not be beyond
    /// the given frontier.
    Append(Vec<(GlobalId, Vec<Update<T>>, T)>),
    /// Update durability information for sources.
    ///
    /// Each entry names a source and provides a frontier before which the source can
    /// be exactly replayed across restarts (i.e. we can assign the same timestamps to
    /// all the same data)
    DurabilityFrontierUpdates(Vec<(GlobalId, Antichain<T>)>),
}

impl<T> ComputeCommand<T> {
    /// Indicates which global ids should start and cease frontier tracking.
    ///
    /// Identifiers added to `start` will install frontier tracking, and identifiers
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
    /// response or cancellation.
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
                ComputeCommand::Peek(peek) => {
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
        live_peeks.retain(|peek| peeks.contains(&peek.uuid));

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

/// Data about timestamp bindings, sent to the coordinator, in service
/// of a specific "linearized" read request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinearizedTimestampBindingFeedback<T = mz_repr::Timestamp> {
    /// The _minimum_ viable timestamp that will produce a "linearized" read...
    pub timestamp: T,
    /// ... for this peek
    pub peek_id: Uuid,
}

/// Data about timestamp bindings that dataflow workers send to the coordinator
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimestampBindingFeedback<T = mz_repr::Timestamp> {
    /// Durability frontier changes
    pub changes: Vec<(GlobalId, ChangeBatch<T>)>,
    /// Timestamp bindings for all of those frontier changes
    pub bindings: Vec<(GlobalId, Vec<(PartitionId, T, MzOffset)>)>,
}

/// Responses that the controller can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(Uuid, PeekResponse),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
    /// Data about timestamp bindings, sent to the coordinator, in service
    /// of a specific "linearized" read request.
    // TODO(benesch,gus): update language to avoid the term "linearizability".
    LinearizedTimestamps(LinearizedTimestampBindingFeedback<T>),
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

    /// Data about timestamp bindings, sent to the coordinator, in service
    /// of a specific "linearized" read request
    LinearizedTimestamps(LinearizedTimestampBindingFeedback<T>),
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

#[async_trait]
impl<T: Send> GenericClient<StorageCommand<T>, StorageResponse<T>> for Box<dyn StorageClient<T>> {
    async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        (**self).recv().await
    }
}

/// A convenience type for compatibility.
#[derive(Debug)]
pub struct LocalClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug,
    R: fmt::Debug,
{
    client: partitioned::Partitioned<process_local::ProcessLocal<C, R>, C, R>,
}

impl<C, R> LocalClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug,
    R: fmt::Debug,
{
    pub fn new(
        feedback_rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<R>>,
        worker_txs: Vec<crossbeam_channel::Sender<C>>,
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
impl<C, R> GenericClient<C, R> for LocalClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        trace!("SEND dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("RECV dataflow response: {:?}", response);
        response
    }
}

/// A [`LocalClient`] for the storage layer.
pub type LocalStorageClient = LocalClient<StorageCommand, StorageResponse>;

/// A [`LocalClient`] for the compute layer.
pub type LocalComputeClient = LocalClient<ComputeCommand, ComputeResponse>;

/// A convenience type for compatibility.
#[derive(Debug)]
pub struct RemoteClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    client: partitioned::Partitioned<tcp::TcpClient<C, R>, C, R>,
}

impl<C, R> RemoteClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
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
        for remote in self.client.parts.iter_mut() {
            remote.connect().await;
        }
    }
}

#[async_trait]
impl<C, R> GenericClient<C, R> for RemoteClient<C, R>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: Serialize + fmt::Debug + Unpin + Send,
    R: DeserializeOwned + fmt::Debug + Unpin + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        trace!("Sending dataflow command: {:?}", cmd);
        self.client.send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        let response = self.client.recv().await;
        trace!("Receiving dataflow response: {:?}", response);
        response
    }
}

/// A client backed by a process-local timely worker thread.
pub mod process_local {
    use std::fmt;

    use async_trait::async_trait;

    use super::GenericClient;

    /// A client to a dataflow server running in the current process.
    #[derive(Debug)]
    pub struct ProcessLocal<C, R> {
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<R>,
        worker_tx: crossbeam_channel::Sender<C>,
        worker_thread: std::thread::Thread,
    }

    #[async_trait]
    impl<C, R> GenericClient<C, R> for ProcessLocal<C, R>
    where
        C: fmt::Debug + Send,
        R: fmt::Debug + Send,
    {
        async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
            self.worker_tx
                .send(cmd)
                .expect("worker command receiver should not drop first");
            self.worker_thread.unpark();
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
            Ok(self.feedback_rx.recv().await)
        }
    }

    impl<C, R> ProcessLocal<C, R> {
        /// Create a new instance of [ProcessLocal] from its parts.
        pub fn new(
            feedback_rx: tokio::sync::mpsc::UnboundedReceiver<R>,
            worker_tx: crossbeam_channel::Sender<C>,
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
    impl<C, R> Drop for ProcessLocal<C, R> {
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
    use std::cmp;
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    use async_trait::async_trait;
    use futures::sink::SinkExt;
    use futures::stream::StreamExt;
    use serde::de::DeserializeOwned;
    use serde::ser::Serialize;
    use tokio::io::{self, AsyncRead, AsyncWrite};
    use tokio::net::TcpStream;
    use tokio::time::{self, Instant};
    use tokio_serde::formats::Bincode;
    use tokio_util::codec::LengthDelimitedCodec;
    use tracing::error;

    use crate::client::GenericClient;

    enum TcpConn<C, R> {
        Disconnected,
        Connecting(Pin<Box<dyn Future<Output = io::Result<TcpStream>> + Send>>),
        Backoff(Instant),
        Connected(FramedClient<TcpStream, C, R>),
    }

    impl<C, R> fmt::Debug for TcpConn<C, R> {
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
    pub struct TcpClient<C, R> {
        connection: TcpConn<C, R>,
        backoff: Duration,
        addr: String,
    }

    impl<C, R> TcpClient<C, R> {
        /// Creates a new `TcpClient` initially in a disconnected state.
        ///
        /// Use the `connect()` method to put the client into a connected state.
        pub fn new(addr: String) -> TcpClient<C, R> {
            Self {
                connection: TcpConn::Disconnected,
                backoff: Duration::from_millis(10),
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
                                "Error connecting to {}: {e}; reconnecting in {:?}",
                                self.addr,
                                self.backoff,
                            );
                            let deadline = Instant::now() + self.backoff;
                            self.backoff = cmp::min(self.backoff * 2, Duration::from_secs(1));
                            self.connection = TcpConn::Backoff(deadline);
                        }
                    },
                    TcpConn::Backoff(deadline) => {
                        time::sleep_until(*deadline).await;
                        self.connection = TcpConn::Disconnected;
                    }
                    TcpConn::Connected(_) => {
                        self.backoff = Duration::from_millis(10);
                        break;
                    }
                }
            }
        }
    }

    #[async_trait]
    impl<C, R> GenericClient<C, R> for TcpClient<C, R>
    where
        C: Serialize + fmt::Debug + Send + Unpin,
        R: DeserializeOwned + fmt::Debug + Send + Unpin,
    {
        async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
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

        async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
            if let TcpConn::Connected(connection) = &mut self.connection {
                match connection.next().await {
                    Some(Ok(response)) => Ok(Some(response)),
                    other => {
                        match other {
                            Some(Ok(_)) => unreachable!("handled above"),
                            None => error!("connection unexpectedly terminated cleanly"),
                            Some(Err(e)) => error!("connection unexpectedly errored: {}", e),
                        }
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
    pub type FramedServer<A, C, R> = Framed<A, C, R>;

    /// A framed connection from the client's perspective.
    pub type FramedClient<A, C, R> = Framed<A, R, C>;

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
    pub fn framed_server<A, C, R>(conn: A) -> FramedServer<A, C, R>
    where
        A: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }

    /// Constructs a framed connection for the client.
    pub fn framed_client<A, C, R>(conn: A) -> FramedClient<A, C, R>
    where
        A: AsyncRead + AsyncWrite,
    {
        tokio_serde::Framed::new(
            tokio_util::codec::Framed::new(conn, length_delimited_codec()),
            Bincode::default(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn peek_protobuf_roundtrip(expect in any::<Peek>() ) {
            let actual = protobuf_roundtrip::<_, ProtoPeek>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
