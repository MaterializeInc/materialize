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
use std::fmt;
use std::pin::Pin;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use tracing::trace;
use uuid::Uuid;

use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::any_uuid;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};

use crate::logging::LoggingConfig;
use crate::{sources::IngestionDescription, DataflowDescription, PeekResponse, TailResponse};

pub mod controller;
pub use controller::Controller;

use self::controller::storage::CollectionMetadata;
use self::controller::ClusterReplicaSizeConfig;

pub mod partitioned;
pub mod replicated;

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.client.rs"));

/// An abstraction allowing us to name different compute instances.
pub type ComputeInstanceId = u64;

/// An abstraction allowing us to name different replicas.
pub type ReplicaId = u64;

/// Identifier of a process within a replica.
pub type ProcessId = i64;

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
        /// The size configuration of the replica.
        size_config: ClusterReplicaSizeConfig,
        /// A readable name for the replica size.
        size_name: String,
        /// The replica's availability zone, if `Some`.
        availability_zone: Option<String>,
    },
}

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

impl RustType<ProtoCompaction> for (GlobalId, Antichain<u64>) {
    fn into_proto(&self) -> ProtoCompaction {
        ProtoCompaction {
            id: Some(self.0.into_proto()),
            frontier: Some((&self.1).into()),
        }
    }

    fn from_proto(proto: ProtoCompaction) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.id.into_rust_if_some("ProtoCompaction::id")?,
            proto
                .frontier
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoCompaction::frontier"))?,
        ))
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

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageCommand<T = mz_repr::Timestamp> {
    /// Create the enumerated sources, each associated with its identifier.
    IngestSources(Vec<IngestSourceCommand>),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
}

/// A command that starts ingesting the given ingestion description
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestSourceCommand {
    /// The id of the storage collection being ingested.
    pub id: GlobalId,
    /// The description of what source type should be ingested and what post-processing steps must
    /// be applied to the data before writing them down into the storage collection
    pub description: IngestionDescription<CollectionMetadata>,
}
impl RustType<ProtoIngestSourceCommand> for IngestSourceCommand {
    fn into_proto(&self) -> ProtoIngestSourceCommand {
        ProtoIngestSourceCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoIngestSourceCommand) -> Result<Self, TryFromProtoError> {
        Ok(IngestSourceCommand {
            id: proto.id.into_rust_if_some("ProtoIngestSourceCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoIngestSourceCommand::description")?,
        })
    }
}

impl RustType<ProtoStorageCommand> for StorageCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageCommand {
        use proto_storage_command::Kind::*;
        ProtoStorageCommand {
            kind: Some(match self {
                StorageCommand::IngestSources(ingestions) => IngestSources(ProtoIngestSources {
                    ingestions: ingestions.into_proto(),
                }),
                StorageCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
            }),
        }
    }

    fn from_proto(proto: ProtoStorageCommand) -> Result<Self, TryFromProtoError> {
        use proto_storage_command::Kind::*;
        match proto.kind {
            Some(IngestSources(ProtoIngestSources { ingestions })) => {
                Ok(StorageCommand::IngestSources(ingestions.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(StorageCommand::AllowCompaction(collections.into_rust()?))
            }
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageCommand::kind",
            )),
        }
    }
}

impl Arbitrary for StorageCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec(any::<IngestSourceCommand>(), 1..4)
                .prop_map(StorageCommand::IngestSources),
            proptest::collection::vec(
                (
                    any::<GlobalId>(),
                    proptest::collection::vec(any::<u64>(), 1..4)
                ),
                1..4
            )
            .prop_map(|collections| StorageCommand::AllowCompaction(
                collections
                    .into_iter()
                    .map(|(id, frontier_vec)| { (id, Antichain::from(frontier_vec)) })
                    .collect()
            )),
        ]
        .boxed()
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

/// Data about timestamp bindings, sent to the coordinator, in service
/// of a specific "linearized" read request
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LinearizedTimestampBindingFeedback<T = mz_repr::Timestamp> {
    /// The _minimum_ viable timestamp that will produce a "linearized" read...
    pub timestamp: T,
    /// ... for this peek
    pub peek_id: Uuid,
}

impl RustType<ProtoTrace> for (GlobalId, ChangeBatch<mz_repr::Timestamp>) {
    fn into_proto(&self) -> ProtoTrace {
        ProtoTrace {
            id: Some(self.0.into_proto()),
            updates: self
                .1
                // Clone because the `iter()` expects
                // `trace` to be mutable.
                .clone()
                .iter()
                .map(|(t, d)| ProtoUpdate {
                    timestamp: *t,
                    diff: *d,
                })
                .collect(),
        }
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        let mut batch = ChangeBatch::new();
        batch.extend(
            proto
                .updates
                .into_iter()
                .map(|update| (update.timestamp, update.diff)),
        );
        Ok((proto.id.into_rust_if_some("ProtoTrace::id")?, batch))
    }
}

impl RustType<ProtoFrontierUppersKind> for Vec<(GlobalId, ChangeBatch<mz_repr::Timestamp>)> {
    fn into_proto(&self) -> ProtoFrontierUppersKind {
        ProtoFrontierUppersKind {
            traces: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoFrontierUppersKind) -> Result<Self, TryFromProtoError> {
        proto.traces.into_rust()
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

fn any_change_batch() -> impl Strategy<Value = ChangeBatch<u64>> {
    proptest::collection::vec((any::<mz_repr::Timestamp>(), any::<i64>()), 1..11).prop_map(
        |changes| {
            let mut batch = ChangeBatch::new();
            batch.extend(changes.into_iter());
            batch
        },
    )
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

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<T>)>),

    /// Data about timestamp bindings, sent to the coordinator, in service
    /// of a specific "linearized" read request
    LinearizedTimestamps(LinearizedTimestampBindingFeedback<T>),
}

impl RustType<ProtoStorageResponse> for StorageResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageResponse {
        use proto_storage_response::Kind::*;
        use proto_storage_response::*;
        ProtoStorageResponse {
            kind: Some(match self {
                // TODO: share this impl with `ComputeResponse`
                StorageResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
                StorageResponse::LinearizedTimestamps(LinearizedTimestampBindingFeedback {
                    timestamp,
                    peek_id,
                }) => LinearizedTimestamps(ProtoLinearizedTimestampBindingFeedback {
                    timestamp: *timestamp,
                    peek_id: Some(peek_id.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageResponse) -> Result<Self, TryFromProtoError> {
        use proto_storage_response::Kind::*;
        match proto.kind {
            // TODO: share this impl with `ComputeResponse`
            Some(FrontierUppers(traces)) => {
                Ok(StorageResponse::FrontierUppers(traces.into_rust()?))
            }
            Some(LinearizedTimestamps(resp)) => Ok(StorageResponse::LinearizedTimestamps(
                LinearizedTimestampBindingFeedback {
                    timestamp: resp.timestamp,
                    peek_id: resp
                        .peek_id
                        .into_rust_if_some("ProtoLinearizedTimestampBindingFeedback::peek_id")?,
                },
            )),
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageResponse::kind",
            )),
        }
    }
}

impl Arbitrary for StorageResponse<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec((any::<GlobalId>(), any_change_batch()), 1..4)
                .prop_map(StorageResponse::FrontierUppers),
            (any::<u64>(), any_uuid()).prop_map(|(timestamp, peek_id)| {
                StorageResponse::LinearizedTimestamps(LinearizedTimestampBindingFeedback {
                    timestamp,
                    peek_id,
                })
            }),
        ]
        .boxed()
    }
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
        Box::pin(async_stream::stream!({
            loop {
                match self.recv().await {
                    Ok(Some(response)) => yield Ok(response),
                    Err(error) => yield Err(error),
                    Ok(None) => {
                        return;
                    }
                }
            }
        }))
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

/// Trait for clients that can be disconnected and reconnected.
#[async_trait]
pub trait Reconnect {
    fn disconnect(&mut self);
    async fn reconnect(&mut self);
}

/// Trait for clients that can connect to an address
pub trait FromAddr {
    fn from_addr(addr: String) -> Self;
}

/// A convenience type for compatibility.
#[derive(Debug)]
pub struct RemoteClient<C, R, G>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
{
    client: partitioned::Partitioned<G, C, R>,
}

pub type ComputedRemoteClient<T> = RemoteClient<
    ComputeCommand<T>,
    ComputeResponse<T>,
    grpc::GrpcClient<
        proto_compute_client::ProtoComputeClient<tonic::transport::Channel>,
        ProtoComputeCommand,
        ProtoComputeResponse,
    >,
>;

pub type StoragedRemoteClient<T> = RemoteClient<
    StorageCommand<T>,
    StorageResponse<T>,
    grpc::GrpcClient<
        proto_storage_client::ProtoStorageClient<tonic::transport::Channel>,
        ProtoStorageCommand,
        ProtoStorageResponse,
    >,
>;

impl<C, R, G> RemoteClient<C, R, G>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
{
    /// Construct a client backed by multiple tcp connections
    pub fn new(addrs: &[impl tokio::net::ToSocketAddrs + std::fmt::Display]) -> Self {
        let mut remotes = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            remotes.push(G::from_addr(addr.to_string()));
        }
        Self {
            client: partitioned::Partitioned::new(remotes),
        }
    }

    pub async fn connect(&mut self) {
        for part in &mut self.client.parts {
            part.reconnect().await;
        }
    }
}

#[async_trait]
impl<C, R, G> GenericClient<C, R> for RemoteClient<C, R, G>
where
    (C, R): partitioned::Partitionable<C, R>,
    C: Serialize + fmt::Debug + Unpin + Send,
    R: DeserializeOwned + fmt::Debug + Unpin + Send,
    G: GenericClient<C, R> + Reconnect + FromAddr,
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

    use super::{GenericClient, Reconnect};

    /// A client to a dataflow server running in the current process.
    #[derive(Debug)]
    pub struct ProcessLocal<C, R> {
        feedback_rx: tokio::sync::mpsc::UnboundedReceiver<R>,
        worker_tx: crossbeam_channel::Sender<C>,
        worker_thread: std::thread::Thread,
    }

    #[async_trait]
    impl<C: Send, R: Send> Reconnect for ProcessLocal<C, R> {
        fn disconnect(&mut self) {
            panic!("Disconnecting and reconnecting local clients is currently impossible");
        }
        async fn reconnect(&mut self) {
            panic!("Disconnecting and reconnecting local clients is currently impossible");
        }
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
pub mod grpc {
    use mz_proto::ProtoType;
    use mz_proto::RustType;
    use std::cmp;
    use std::fmt;
    use std::net::ToSocketAddrs;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;
    use tracing::{debug, error, info, warn};

    use async_trait::async_trait;
    use futures::stream::StreamExt;
    use futures::Stream;
    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
    use tokio::sync::Mutex;
    use tokio::sync::Notify;
    use tokio::time::{self, Instant};
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Server;
    use tonic::Request;
    use tonic::Response;
    use tonic::Streaming;

    use super::proto_compute_client::ProtoComputeClient;
    use super::proto_storage_client::ProtoStorageClient;
    use super::FromAddr;
    use super::GenericClient;
    use super::Reconnect;

    /// A client to a remote dataflow server using gRPC and protobuf based communication.
    ///
    /// The client opens a connection using the proto client stubs that are generated by
    /// tonic from the service definition in `client.proto`. After creation, the client is in
    /// disconnected state. To connect it, `connect` has to be called. Once the client is
    /// connected, it will call automatically the only RPC defined in the service description,
    /// encapsulated by the `BidiProtoClient` trait. This trait bound is not on the `Client`
    /// type parameter here, but it IS on the impl blocks.
    /// Bidirectional protobuf RPC sets up two streams that persist after the
    /// RPC has returned: A Request (Command) stream (for us, backed by a unbounded mpsc queue)
    /// going from this instance to the server and a response stream coming back
    /// (represented directly as a Streaming<Response> instance). The recv and send functions
    /// interact with the two mpsc channels or the streaming instance respectively.
    #[derive(Debug)]
    pub struct GrpcClient<Client, PC, PR> {
        addr: String,
        state: GrpcTcpConn<Client, PC, PR>,
        backoff: Duration,
    }

    /// The connection state of the GrpcClient.
    #[derive(Debug)]
    enum GrpcTcpConn<Client, PC, PR> {
        // Initial, disconnected state
        Disconnected,

        // We have a TCP client connection, but we have to wait for RPC answer for setting up the streams
        AwaitResponse(Client),

        // Ready to go!
        Connected((UnboundedSender<PC>, Streaming<PR>)),

        // Unable to connect, wait on next connect
        Backoff(Instant),
    }

    impl<Client, PC, PR> FromAddr for GrpcClient<Client, PC, PR> {
        fn from_addr(addr: String) -> Self {
            GrpcClient {
                addr: format!("http://{}", addr),
                state: GrpcTcpConn::Disconnected,
                backoff: Duration::from_millis(10),
            }
        }
    }

    impl<Client, PC, PR> GrpcClient<Client, PC, PR>
    where
        Client: BidiProtoClient<ProtoCommand = PC, ProtoResponse = PR>,
    {
        // This is and must be cancellation safe
        pub async fn connect(&mut self) -> () {
            loop {
                match &mut self.state {
                    GrpcTcpConn::Disconnected => {
                        debug!("GrpcClient {}: Attempt to connect", &self.addr);
                        match Client::connect(self.addr.clone()).await {
                            Ok(client) => {
                                self.backoff = Duration::from_millis(10);
                                self.state = GrpcTcpConn::AwaitResponse(client);
                            }
                            Err(e) => {
                                self.backoff = cmp::min(self.backoff * 2, Duration::from_secs(1));
                                warn!(
                                    "GrpcClient {}: Connection refused: {}. Backoff {}ms",
                                    &self.addr,
                                    e,
                                    self.backoff.as_millis()
                                );
                                self.state = GrpcTcpConn::Backoff(Instant::now() + self.backoff);
                            }
                        }
                    }
                    GrpcTcpConn::AwaitResponse(client) => {
                        // The channel size is a arbitrary, but it should be a
                        // small bounded channel such that backpressure is applied early.
                        let (tx, rx) = mpsc::unbounded_channel();
                        match client.create_stream(rx).await {
                            Ok(stream) => {
                                info!("GrpcClient {}: connected", &self.addr);
                                self.state = GrpcTcpConn::Connected((tx, stream));
                            }
                            Err(err) => {
                                debug!("GrpcClient {}: Connection refused: {}", &self.addr, err);
                                self.state = GrpcTcpConn::Disconnected;
                            }
                        }
                    }
                    GrpcTcpConn::Connected(_) => break,
                    GrpcTcpConn::Backoff(deadline) => {
                        time::sleep_until(*deadline).await;
                        self.state = GrpcTcpConn::Disconnected;
                    }
                }
            }
        }
    }

    #[async_trait]
    impl<Client, C, R, PC, PR> GenericClient<C, R> for GrpcClient<Client, PC, PR>
    where
        C: RustType<PC> + Send + Sync + 'static,
        R: RustType<PR> + Send + Sync,
        Client: BidiProtoClient<ProtoCommand = PC, ProtoResponse = PR> + Send + fmt::Debug,
        PC: Send + Sync + fmt::Debug,
        PR: Send + Sync + fmt::Debug,
    {
        async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
            let sender = if let GrpcTcpConn::Connected((sender, _)) = &self.state {
                sender
            } else {
                return Err(anyhow::anyhow!("Sent into disconnected channel"));
            };
            if sender.send(cmd.into_proto()).is_err() {
                self.state = GrpcTcpConn::Disconnected;
                Err(anyhow::anyhow!("Sent into disconnected channel"))
            } else {
                Ok(())
            }
        }

        async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
            if let GrpcTcpConn::Connected(channels) = &mut self.state {
                match channels.1.next().await {
                    Some(Ok(x)) => match x.into_rust() {
                        Ok(r) => return Ok(Some(r)),
                        Err(e) => {
                            error!(
                                "could not decode protobuf message, terminating connection: {}",
                                e
                            );
                            self.state = GrpcTcpConn::Disconnected;
                            anyhow::bail!("Connection severed");
                        }
                    },
                    other => {
                        match other {
                            Some(Ok(_)) => unreachable!("handled above"),
                            None => error!("connection unexpectedly terminated cleanly"),
                            Some(Err(e)) => error!("connection unexpectedly errored: {}", e),
                        }

                        self.state = GrpcTcpConn::Disconnected;
                        anyhow::bail!("Connection severed")
                    }
                }
            } else {
                Err(anyhow::anyhow!("Connection severed"))
            }
        }
    }

    #[async_trait]
    impl<
            Client: BidiProtoClient<ProtoCommand = PC, ProtoResponse = PR> + Send + Sync,
            PC: Send,
            PR,
        > Reconnect for GrpcClient<Client, PC, PR>
    {
        fn disconnect(&mut self) {
            debug!("GrpcClient {}: disconnect called", &self.addr);
            self.state = GrpcTcpConn::Disconnected;
        }

        async fn reconnect(&mut self) {
            debug!("GrpcClient {}: reconnect called", &self.addr);
            self.connect().await
        }
    }

    /// The server side gRPC implementation that will run in computed or storaged.
    ///
    /// There are two main tasks involved: The gRPC callback implementations will execute in their own tasks,
    /// receive commands from the network and send responses to the network. Upon reception of a command,
    /// the implementation will put it in a mpsc queue, out of which the consumer (running in another
    /// task) will read it with a recv call. The same goes for the send path: The consumer calls `send` which
    /// puts the response in a mpsc queue, from which the gRPC stubs will read and send it over the network.
    ///
    /// If an error occurs, the consumer receives an error from the recv call. If no client is
    /// connected recv will block until a client is available. To implement the "waiting for
    /// client" the queue_change notification is used. recv will check first if a client is connected using
    /// queue. If queue is None, no client is connected and recv will await on the queue_change notification.
    /// The server does this vice-versa. Upon connection of a client, it will insert the endpoints
    /// into queue and trigger the queue_change, which will wake up the waiting clients.
    ///
    /// This is the shared datastructure between server and consumer.
    ///
    pub struct GrpcShared<PC, PR> {
        // These are endpoints for the consumer. The other end of these queues is consumed by the
        // stream implementation. `queue` is None if no client is connected, otherwise the
        // endpoints are forwarded to the single client. If a client is connecting but a client is
        // already connected, this mutex is used to block. If the consumer calls send or recv and
        // queue is None, the consumer will wait on the queue_change notification to proceed only
        // when a client has connected.
        queue: Mutex<Option<(Streaming<PC>, UnboundedSender<PR>)>>,

        // If queue changes, the server side will publish a notification here.
        queue_change: Notify,
    }

    /// Server side implementation.
    pub struct GrpcServer<PC, PR> {
        shared: Arc<GrpcShared<PC, PR>>,
    }

    /// Consumer side functions such as send and recv. These will not directly interact with the
    /// network, but put messages in a mpsc queue which will be read and sent to the network in a
    /// separate server task.
    pub struct GrpcServerInterface<PC, PR> {
        shared: Arc<GrpcShared<PC, PR>>,
    }

    impl<PC: Send + Sync, PR: Send + Sync + fmt::Debug + 'static> GrpcServerInterface<PC, PR> {
        pub async fn send<R: RustType<PR> + Send + Sync>(
            &self,
            resp: R,
        ) -> Result<(), anyhow::Error> {
            loop {
                let res = match self.shared.queue.lock().await.as_ref() {
                    Some(x) => Some(x.1.send((&resp).into_proto()).map_err(Into::into)),

                    // Other end absent, wait for connection, can't inline queue reset
                    // here as we are still holding the lock.
                    None => None,
                };

                match res {
                    Some(r) => {
                        if r.is_err() {
                            *self.shared.queue.lock().await = None;
                        }
                        return r;
                    }
                    None => self.shared.queue_change.notified().await,
                }
            }
        }

        // Recv returns an error if a faulty connection is detected (for example when
        // the other queue endpoint has been dropped).
        // If there is no current connection, it will await a connection from the coordinator.
        //
        // This function is and must be cancellation safe.
        // If a message is received from the streaming instance and it does not cause an error,
        // the message will be delivered, as there are no await points on the path.
        // If there is an error more await point will be passed, however the hope is that in the
        // error case, the next interaction with the queue will produce another error, such that
        // the errors are not lost due to cancellation.
        //
        // There is no race between the creation of new queue pair, as the Notify will store
        // internally a permit: If a notifier comes first, and this function calls notified().await,
        // it will immediately return (and clear the permit).
        // See https://docs.rs/tokio/0.2.12/tokio/sync/struct.Notify.html
        pub async fn recv<C: RustType<PC>>(&mut self) -> Result<C, anyhow::Error> {
            loop {
                let res = match self.shared.queue.lock().await.as_mut() {
                    Some(x) => {
                        let res = match x.0.next().await {
                            Some(Ok(x)) => x.into_rust().map_err(Into::into),
                            Some(Err(e)) => {
                                info!("Connection severed: {}", e);
                                Err(e.into())
                            }
                            None => {
                                info!("Connection severed: Endpoint gone");
                                Err(anyhow::anyhow!("Connection severed: Endpoint gone"))
                            }
                        };
                        Some(res)
                    }

                    // Other end absent, wait for connection
                    None => {
                        debug!("recv called while no coordinator connected, waiting for coordinator connection.");
                        None
                    }
                };

                // Don't move queue reset in block above as we are still holding the queue lock
                // there.
                match res {
                    None => {
                        // No queue
                        self.shared.queue_change.notified().await;
                    }
                    Some(res) => {
                        if res.is_err() {
                            *self.shared.queue.lock().await = None;
                        }
                        return res;
                    }
                }
            }
        }
    }

    type ResponseStream<PR> = Pin<Box<dyn Stream<Item = Result<PR, tonic::Status>> + Send>>;

    // The following traits and impls are here because of limitations in prost; namely,
    // it does not provide traits that are generic over the request/response types,
    // for clients and servers.

    /// The implementations of this trait MUST be identical minus the types.
    #[tonic::async_trait]
    impl super::proto_compute_server::ProtoCompute
        for GrpcServer<super::ProtoComputeCommand, super::ProtoComputeResponse>
    {
        type CommandResponseStreamStream = ResponseStream<super::ProtoComputeResponse>;

        async fn command_response_stream(
            &self,
            req: Request<Streaming<super::ProtoComputeCommand>>,
        ) -> Result<tonic::Response<Self::CommandResponseStreamStream>, tonic::Status> {
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

    #[tonic::async_trait]
    impl super::proto_storage_server::ProtoStorage
        for GrpcServer<super::ProtoStorageCommand, super::ProtoStorageResponse>
    {
        type CommandResponseStreamStream = ResponseStream<super::ProtoStorageResponse>;

        async fn command_response_stream(
            &self,
            req: Request<Streaming<super::ProtoStorageCommand>>,
        ) -> Result<tonic::Response<Self::CommandResponseStreamStream>, tonic::Status> {
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

    /// Creates a running gRPC based server that can receive `PC`
    /// and sends `PR`. Returns a a tuple of GrpcServerInterface that
    /// can be used to recv and send from. As well as a shutdown signal, which should
    /// be used to terminate the mainloop if a message is sent.
    ///
    /// The trait bounds here are intimidating, but the `f` parameter is
    /// _a function that turns a `GrpcServer<ProtoCommandType, ProtoResponseType>`
    /// into a `tower::Service` that represents a grpc server. This is _always_
    /// encapsulated by the `ExportedFromTonicServer::new` for a specific protobuf
    /// `service`.
    pub fn grpc_server<PC, PR, F, S>(listen_addr: String, f: F) -> GrpcServerInterface<PC, PR>
    where
        PC: Send + 'static,
        PR: Send + 'static,
        S: tower::Service<
                http::request::Request<tonic::transport::Body>,
                Response = http::response::Response<tonic::body::BoxBody>,
                Error = std::convert::Infallible,
            > + tonic::transport::NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        F: FnOnce(GrpcServer<PC, PR>) -> S + Send + 'static,
    {
        // The channel size is a arbitrary, but it should be a
        // small bounded channel such that backpressure is applied early.
        let shared = Arc::new(GrpcShared {
            queue: Mutex::new(None),
            queue_change: Notify::new(),
        });

        let server = GrpcServer {
            shared: Arc::clone(&shared),
        };

        mz_ore::task::spawn(|| "ComputedProtoServer", async move {
            info!("Starting to listen on {}", listen_addr);
            Server::builder()
                .add_service(f(server))
                .serve(listen_addr.to_socket_addrs().unwrap().next().unwrap())
                .await
                .unwrap();
        });

        GrpcServerInterface { shared }
    }

    // TODO(guswynn): if prost ever presents the client api as a trait, move to that
    // instead of this verbose
    /// Encapsulates the core functionality of a prost client for a bidirectional stream.
    ///
    /// The implementations for this trait MUST be identical minus the types
    #[async_trait::async_trait]
    pub trait BidiProtoClient {
        type ProtoCommand;
        type ProtoResponse;
        async fn connect(addr: String) -> Result<Self, anyhow::Error>
        where
            Self: Sized;
        async fn create_stream(
            &mut self,
            rx: UnboundedReceiver<Self::ProtoCommand>,
        ) -> Result<Streaming<Self::ProtoResponse>, anyhow::Error>;
    }
    #[async_trait::async_trait]
    impl BidiProtoClient for ProtoComputeClient<tonic::transport::Channel> {
        type ProtoCommand = super::ProtoComputeCommand;
        type ProtoResponse = super::ProtoComputeResponse;
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
    #[async_trait::async_trait]
    impl BidiProtoClient for ProtoStorageClient<tonic::transport::Channel> {
        type ProtoCommand = super::ProtoStorageCommand;
        type ProtoResponse = super::ProtoStorageResponse;
        async fn connect(addr: String) -> Result<Self, anyhow::Error>
        where
            Self: Sized,
        {
            Ok(ProtoStorageClient::connect(addr).await?)
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

    // Convenient re-exports
    pub use super::proto_compute_server::ProtoComputeServer;
    pub use super::proto_storage_server::ProtoStorageServer;

    // Convenience methods for serving a grpc server that is connected to a local client

    /// Configuration required to configure `serve`.
    pub struct ServeConfig {
        /// What address to listen for grpc rpcs on.
        pub listen_addr: String,
        /// Whether or not to continue running if the client (the coordinator)
        /// disconnects.
        pub linger: bool,
    }

    /// Start a grpc server, and forward commands and responses to the
    /// _local_ `client`.
    ///
    /// The trait bounds here are intimidating, but the `f` parameter is
    /// _a function that turns a `GrpcServer<ProtoCommandType, ProtoResponseType>`
    /// into a `tower::Service` that represents a grpc server. This is _always_
    /// encapsulated by the `ExportedFromTonicServer::new` for a specific protobuf
    /// `service`.
    ///
    /// Some servers (like `ProtoComputeServer`) are re-exported from this module
    /// for convenience.
    pub async fn serve<C, R, PC, PR, S, F, G>(
        config: ServeConfig,
        mut client: G,
        f: F,
    ) -> Result<(), anyhow::Error>
    where
        PC: Send + Sync + 'static + fmt::Debug,
        PR: Send + Sync + 'static + fmt::Debug,
        C: RustType<PC> + Send + Sync + 'static,
        R: RustType<PR> + Send + Sync,
        G: super::GenericClient<C, R>,
        S: tower::Service<
                http::request::Request<tonic::transport::Body>,
                Response = http::response::Response<tonic::body::BoxBody>,
                Error = std::convert::Infallible,
            > + tonic::transport::NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
        F: FnOnce(GrpcServer<PC, PR>) -> S + Send + 'static,
    {
        let mut grpc_serve = grpc_server(config.listen_addr, f);

        loop {
            // This select implies that the .recv functions of the clients must be cancellation safe.
            loop {
                tokio::select! {
                    res = grpc_serve.recv() => {
                        match res {
                            Ok(cmd) => client.send(cmd).await.unwrap(),
                            Err(err) => {
                                tracing::warn!("Lost connection: {}", err);
                                break;
                            }
                        }
                    },
                    res = client.recv() => {
                        match res.unwrap() {
                            None => { },
                            Some(response) => {
                                match grpc_serve.send(response).await {
                                    Ok(_) =>  { } ,
                                    Err(err) => {
                                        tracing::warn!("Lost connection: {}", err);
                                        break;
                                    }
                                }
                            }
                        }
                    },
                }
            }
            if !config.linger {
                tracing::info!("coordinator connection gone; terminating");
                break;
            }
            tracing::info!("coordinator connection gone; waiting for reconnect");
        }

        Ok(())
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

        #[test]
        fn storage_command_protobuf_roundtrip(expect in any::<StorageCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoStorageCommand>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn storage_response_protobuf_roundtrip(expect in any::<StorageResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoStorageResponse>(&expect);
            assert!(actual.is_ok());
            let actual = actual.unwrap();
            if let StorageResponse::FrontierUppers(expected_traces) = expect {
                if let StorageResponse::FrontierUppers(actual_traces) = actual {
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
                    assert_eq!(actual, StorageResponse::FrontierUppers(expected_traces));
                }
            } else {
                assert_eq!(actual, expect);
            }
        }
    }
}
