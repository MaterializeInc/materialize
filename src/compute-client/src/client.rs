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

// Tonic generates code that calls clone on an Arc. Allow this here.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::clone_on_ref_ptr)]

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use uuid::Uuid;

use futures::stream::StreamExt;
use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::any_uuid;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_service::client::GenericClient;
use mz_service::grpc::{BidiProtoClient, GrpcClient, GrpcServer, ResponseStream};
use mz_storage::client::controller::CollectionMetadata;
use mz_storage::client::{LinearizedTimestampBindingFeedback, ProtoAllowCompaction};
use mz_timely_util::progress::any_change_batch;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};
use tracing::debug;

use crate::client::proto_compute_client::ProtoComputeClient;
use crate::client::proto_compute_server::ProtoCompute;
use crate::logging::LoggingConfig;
use crate::{DataflowDescription, PeekResponse, TailResponse};

pub mod controller;

pub mod partitioned;
pub mod replicated;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.client.rs"));

/// An abstraction allowing us to name different compute instances.
pub type ComputeInstanceId = u64;

/// An abstraction allowing us to name different replicas.
pub type ReplicaId = u64;

/// Identifier of a process within a replica.
pub type ProcessId = i64;

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configuration sent to new compute instances.
pub struct InstanceConfig {
    /// The instance's replica ID.
    pub replica_id: ReplicaId,
    /// Optionally, request the installation of logging sources.
    pub logging: Option<LoggingConfig>,
}

impl RustType<ProtoInstanceConfig> for InstanceConfig {
    fn into_proto(&self) -> ProtoInstanceConfig {
        ProtoInstanceConfig {
            replica_id: self.replica_id,
            logging: self.logging.into_proto(),
        }
    }

    fn from_proto(proto: ProtoInstanceConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            replica_id: proto.replica_id,
            logging: proto.logging.into_rust()?,
        })
    }
}

fn empty_otel_ctx() -> impl Strategy<Value = OpenTelemetryContext> {
    (0..1).prop_map(|_| OpenTelemetryContext::empty())
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
    /// Target replica of this peek.
    ///
    /// If `Some`, the peek is only handled by the given replica.
    /// If `None`, the peek is handled by all replicas.
    pub target_replica: Option<ReplicaId>,
    /// An `OpenTelemetryContext` to forward trace information along
    /// to the compute worker to allow associating traces between
    /// the compute controller and the compute worker.
    #[proptest(strategy = "empty_otel_ctx()")]
    pub otel_ctx: OpenTelemetryContext,
}

impl RustType<ProtoPeek> for Peek {
    fn into_proto(&self) -> ProtoPeek {
        ProtoPeek {
            id: Some(self.id.into_proto()),
            key: self.key.into_proto(),
            uuid: Some(self.uuid.into_proto()),
            timestamp: self.timestamp,
            finishing: Some(self.finishing.into_proto()),
            map_filter_project: Some(self.map_filter_project.into_proto()),
            target_replica: self.target_replica,
            otel_ctx: self.otel_ctx.clone().into(),
        }
    }

    fn from_proto(x: ProtoPeek) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: x.id.into_rust_if_some("ProtoPeek::id")?,
            key: x.key.into_rust()?,
            uuid: x.uuid.into_rust_if_some("ProtoPeek::uuid")?,
            timestamp: x.timestamp,
            finishing: x.finishing.into_rust_if_some("ProtoPeek::finishing")?,
            map_filter_project: x
                .map_filter_project
                .into_rust_if_some("ProtoPeek::map_filter_project")?,
            target_replica: x.target_replica,
            otel_ctx: x.otel_ctx.into(),
        })
    }
}

/// Commands related to the computation and maintenance of views.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeCommand<T = mz_repr::Timestamp> {
    /// Indicates the creation of an instance, and is the first command for its compute instance.
    CreateInstance(InstanceConfig),
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

impl RustType<ProtoComputeCommand> for ComputeCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeCommand {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        ProtoComputeCommand {
            kind: Some(match self {
                ComputeCommand::CreateInstance(config) => CreateInstance(config.into_proto()),
                ComputeCommand::DropInstance => DropInstance(()),
                ComputeCommand::CreateDataflows(dataflows) => {
                    CreateDataflows(ProtoCreateDataflows {
                        dataflows: dataflows.into_proto(),
                    })
                }
                ComputeCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                ComputeCommand::Peek(peek) => Peek(peek.into_proto()),
                ComputeCommand::CancelPeeks { uuids } => CancelPeeks(ProtoCancelPeeks {
                    uuids: uuids.into_proto(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeCommand) -> Result<Self, TryFromProtoError> {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        match proto.kind {
            Some(CreateInstance(config)) => Ok(ComputeCommand::CreateInstance(config.into_rust()?)),
            Some(DropInstance(())) => Ok(ComputeCommand::DropInstance),
            Some(CreateDataflows(ProtoCreateDataflows { dataflows })) => {
                Ok(ComputeCommand::CreateDataflows(dataflows.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(ComputeCommand::AllowCompaction(collections.into_rust()?))
            }
            Some(Peek(peek)) => Ok(ComputeCommand::Peek(peek.into_rust()?)),
            Some(CancelPeeks(ProtoCancelPeeks { uuids })) => Ok(ComputeCommand::CancelPeeks {
                uuids: uuids.into_rust()?,
            }),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeCommand::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<InstanceConfig>().prop_map(ComputeCommand::CreateInstance),
            Just(ComputeCommand::DropInstance),
            proptest::collection::vec(
                any::<DataflowDescription<crate::plan::Plan, CollectionMetadata, mz_repr::Timestamp>>(),
                1..4
            )
            .prop_map(ComputeCommand::CreateDataflows),
            proptest::collection::vec(
                (
                    any::<GlobalId>(),
                    proptest::collection::vec(any::<u64>(), 1..4)
                ),
                1..4
            )
            .prop_map(|collections| ComputeCommand::AllowCompaction(
                collections
                    .into_iter()
                    .map(|(id, frontier_vec)| { (id, Antichain::from(frontier_vec)) })
                    .collect()
            )),
            any::<Peek>().prop_map(ComputeCommand::Peek),
            proptest::collection::vec(any_uuid(), 1..6).prop_map(|uuids| {
                ComputeCommand::CancelPeeks {
                    uuids: BTreeSet::from_iter(uuids.into_iter()),
                }
            })
        ]
        .boxed()
    }
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
            ComputeCommand::CreateInstance(config) => {
                if let Some(logging_config) = &config.logging {
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

/// Responses that the controller can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// The worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
    /// Data about timestamp bindings, sent to the coordinator, in service
    /// of a specific "linearized" read request.
    // TODO(benesch,gus): update language to avoid the term "linearizability".
    LinearizedTimestamps(LinearizedTimestampBindingFeedback<T>),
    /// Notification that we have received a message from the given compute replica
    /// at the given time.
    ComputeReplicaHeartbeat(ReplicaId, DateTime<Utc>),
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

/// Responses that the compute nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<T>)>),
    /// The worker's response to a specified (by connection id) peek.
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
}

impl RustType<ProtoComputeResponse> for ComputeResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeResponse {
        use proto_compute_response::Kind::*;
        use proto_compute_response::*;
        ProtoComputeResponse {
            kind: Some(match self {
                ComputeResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
                ComputeResponse::PeekResponse(id, resp, otel_ctx) => {
                    PeekResponse(ProtoPeekResponseKind {
                        id: Some(id.into_proto()),
                        resp: Some(resp.into_proto()),
                        otel_ctx: otel_ctx.clone().into(),
                    })
                }
                ComputeResponse::TailResponse(id, resp) => TailResponse(ProtoTailResponseKind {
                    id: Some(id.into_proto()),
                    resp: Some(resp.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeResponse) -> Result<Self, TryFromProtoError> {
        use proto_compute_response::Kind::*;
        match proto.kind {
            Some(FrontierUppers(traces)) => {
                Ok(ComputeResponse::FrontierUppers(traces.into_rust()?))
            }
            Some(PeekResponse(resp)) => Ok(ComputeResponse::PeekResponse(
                resp.id.into_rust_if_some("ProtoPeekResponseKind::id")?,
                resp.resp.into_rust_if_some("ProtoPeekResponseKind::resp")?,
                resp.otel_ctx.into(),
            )),
            Some(TailResponse(resp)) => Ok(ComputeResponse::TailResponse(
                resp.id.into_rust_if_some("ProtoTailResponseKind::id")?,
                resp.resp.into_rust_if_some("ProtoTailResponseKind::resp")?,
            )),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeResponse::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeResponse<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec((any::<GlobalId>(), any_change_batch()), 1..4)
                .prop_map(ComputeResponse::FrontierUppers),
            (any_uuid(), any::<PeekResponse>()).prop_map(|(id, resp)| {
                ComputeResponse::PeekResponse(id, resp, OpenTelemetryContext::empty())
            }),
            (any::<GlobalId>(), any::<TailResponse>())
                .prop_map(|(id, resp)| ComputeResponse::TailResponse(id, resp)),
        ]
        .boxed()
    }
}

/// A client to a compute server.
pub trait ComputeClient<T = mz_repr::Timestamp>:
    GenericClient<ComputeCommand<T>, ComputeResponse<T>>
{
}

impl<C, T> ComputeClient<T> for C where C: GenericClient<ComputeCommand<T>, ComputeResponse<T>> {}

#[async_trait]
impl<T: Send> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for Box<dyn ComputeClient<T>> {
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        (**self).recv().await
    }
}

pub type ComputeGrpcClient =
    GrpcClient<ProtoComputeClient<Channel>, ProtoComputeCommand, ProtoComputeResponse>;

#[async_trait]
impl BidiProtoClient for ProtoComputeClient<Channel> {
    type ProtoCommand = ProtoComputeCommand;
    type ProtoResponse = ProtoComputeResponse;

    async fn connect(addr: String) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(ProtoComputeClient::connect(addr).await?)
    }

    async fn create_stream(
        &mut self,
        rx: UnboundedReceiver<Self::ProtoCommand>,
    ) -> Result<Streaming<Self::ProtoResponse>, anyhow::Error> {
        match self
            .command_response_stream(UnboundedReceiverStream::new(rx))
            .await
        {
            Ok(resp_rx_wrap) => Ok(resp_rx_wrap.into_inner()),
            Err(err) => Err(err)?,
        }
    }
}

// The following traits and impls are here because of limitations in prost; namely,
// it does not provide traits that are generic over the request/response types,
// for clients and servers.

/// The implementations of this trait MUST be identical minus the types.
#[async_trait]
impl ProtoCompute for GrpcServer<ProtoComputeCommand, ProtoComputeResponse> {
    type CommandResponseStreamStream = ResponseStream<ProtoComputeResponse>;

    async fn command_response_stream(
        &self,
        req: Request<Streaming<ProtoComputeCommand>>,
    ) -> Result<Response<Self::CommandResponseStreamStream>, Status> {
        debug!("GrpcServer: remote client connected");

        // Consistent with the ActiveReplication client, we use unbounded channels.
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();

        // Store channels in state
        *self.shared.queue.lock().await = Some((req.into_inner(), resp_tx));
        self.shared.queue_change.notify_waiters();

        let receiver_stream = UnboundedReceiverStream::new(resp_rx).map(Ok);

        Ok(Response::new(
            Box::pin(receiver_stream) as Self::CommandResponseStreamStream
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_proto::protobuf_roundtrip;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn peek_protobuf_roundtrip(expect in any::<Peek>() ) {
            let actual = protobuf_roundtrip::<_, ProtoPeek>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn compute_command_protobuf_roundtrip(expect in any::<ComputeCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeCommand>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn compute_response_protobuf_roundtrip(expect in any::<ComputeResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeResponse>(&expect);
            assert!(actual.is_ok());
            let actual = actual.unwrap();
            if let ComputeResponse::FrontierUppers(expected_traces) = expect {
                if let ComputeResponse::FrontierUppers(actual_traces) = actual {
                    assert_eq!(actual_traces.len(), expected_traces.len());
                    for ((actual_id, mut actual_changes), (expected_id, mut expected_changes)) in actual_traces.into_iter().zip(expected_traces.into_iter()) {
                        assert_eq!(actual_id, expected_id);
                        // `ChangeBatch`es representing equivalent sets of
                        // changes could have different internal
                        // representations, so they need to be compacted before comparing.
                        actual_changes.compact();
                        expected_changes.compact();
                        assert_eq!(actual_changes, expected_changes);
                    }
                } else {
                    assert_eq!(actual, ComputeResponse::FrontierUppers(expected_traces));
                }
            } else {
                assert_eq!(actual, expect);
            }
        }
    }
}
