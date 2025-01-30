// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]
// Tonic generates code that violates clippy lints.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::as_conversions, clippy::clone_on_ref_ptr)]

//! The public API of the storage layer.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::iter;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig, TryIntoTimelyConfig};
use mz_ore::assert_none;
use mz_persist_client::batch::ProtoBatch;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{GrpcClient, GrpcServer, ProtoServiceTypes, ResponseStream};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::oneshot_sources::OneshotIngestionRequest;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sinks::StorageSinkDesc;
use mz_storage_types::sources::IngestionDescription;
use mz_timely_util::progress::any_antichain;
use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::PartialOrder;
use tonic::{Request, Status as TonicStatus, Streaming};

use crate::client::proto_storage_server::ProtoStorage;
use crate::metrics::ReplicaMetrics;
use crate::statistics::{SinkStatisticsUpdate, SourceStatisticsUpdate};

include!(concat!(env!("OUT_DIR"), "/mz_storage_client.client.rs"));

/// A client to a storage server.
pub trait StorageClient<T = mz_repr::Timestamp>:
    GenericClient<StorageCommand<T>, StorageResponse<T>>
{
}

impl<C, T> StorageClient<T> for C where C: GenericClient<StorageCommand<T>, StorageResponse<T>> {}

#[async_trait]
impl<T: Send> GenericClient<StorageCommand<T>, StorageResponse<T>> for Box<dyn StorageClient<T>> {
    async fn send(&mut self, cmd: StorageCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        // `GenericClient::recv` is required to be cancel safe.
        (**self).recv().await
    }
}

#[derive(Debug, Clone)]
pub enum StorageProtoServiceTypes {}

impl ProtoServiceTypes for StorageProtoServiceTypes {
    type PC = ProtoStorageCommand;
    type PR = ProtoStorageResponse;
    type STATS = ReplicaMetrics;
    const URL: &'static str = "/mz_storage_client.client.ProtoStorage/CommandResponseStream";
}

pub type StorageGrpcClient = GrpcClient<StorageProtoServiceTypes>;

#[async_trait]
impl<F, G> ProtoStorage for GrpcServer<F>
where
    F: Fn() -> G + Send + Sync + 'static,
    G: StorageClient + 'static,
{
    type CommandResponseStreamStream = ResponseStream<ProtoStorageResponse>;

    async fn command_response_stream(
        &self,
        request: Request<Streaming<ProtoStorageCommand>>,
    ) -> Result<tonic::Response<Self::CommandResponseStreamStream>, TonicStatus> {
        self.forward_bidi_stream(request).await
    }
}

/// Commands related to the ingress and egress of collections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageCommand<T = mz_repr::Timestamp> {
    /// Specifies to the storage server(s) the shape of the timely cluster
    /// we want created, before other commands are sent.
    CreateTimely {
        config: TimelyConfig,
        epoch: ClusterStartupEpoch,
    },
    /// Indicates that the controller has sent all commands reflecting its
    /// initial state.
    InitializationComplete,
    /// `AllowWrites` informs the replica that it can transition out of the
    /// read-only stage and into the read-write computation stage.
    /// It is now allowed to affect changes to external systems (writes).
    ///
    /// See `ComputeCommand::AllowWrites` for details. This command works
    /// analogously to the compute version.
    AllowWrites,
    /// Update storage instance configuration.
    UpdateConfiguration(StorageParameters),
    /// Run the enumerated sources, each associated with its identifier.
    RunIngestions(Vec<RunIngestionCommand>),
    /// Run a dataflow which will ingest data from an external source and only __stage__ it in
    /// Persist.
    ///
    /// Unlike regular ingestions/sources, some other component (e.g. `environmentd`) is
    /// responsible for linking the staged data into a shard.
    RunOneshotIngestion(RunOneshotIngestionCommand),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
    RunSinks(Vec<RunSinkCommand<T>>),
}

impl<T> StorageCommand<T> {
    /// Returns whether this command instructs the installation of storage objects.
    pub fn installs_objects(&self) -> bool {
        use StorageCommand::*;
        match self {
            CreateTimely { .. }
            | InitializationComplete
            | AllowWrites
            | UpdateConfiguration(_)
            | AllowCompaction(_) => false,
            // TODO(cf2): multi-replica oneshot ingestions. At the moment returning
            // true here means we can't run `COPY FROM` on multi-replica clusters, this
            // should be easy enough to support though.
            RunIngestions(_) | RunSinks(_) | RunOneshotIngestion(_) => true,
        }
    }
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunIngestionCommand {
    /// The id of the storage collection being ingested.
    pub id: GlobalId,
    /// The description of what source type should be ingested and what post-processing steps must
    /// be applied to the data before writing them down into the storage collection
    pub description: IngestionDescription<CollectionMetadata>,
}

impl Arbitrary for RunIngestionCommand {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<IngestionDescription<CollectionMetadata>>(),
        )
            .prop_map(|(id, description)| Self { id, description })
            .boxed()
    }
}

impl RustType<ProtoRunIngestionCommand> for RunIngestionCommand {
    fn into_proto(&self) -> ProtoRunIngestionCommand {
        ProtoRunIngestionCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRunIngestionCommand) -> Result<Self, TryFromProtoError> {
        Ok(RunIngestionCommand {
            id: proto.id.into_rust_if_some("ProtoRunIngestionCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoRunIngestionCommand::description")?,
        })
    }
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunOneshotIngestionCommand {
    /// The ID of the ingestion dataflow.
    pub ingestion_id: uuid::Uuid,
    /// The ID of collection we'll stage batches for.
    pub collection_id: GlobalId,
    /// Metadata for the collection we'll stage batches for.
    pub collection_meta: CollectionMetadata,
    /// Details for the oneshot ingestion.
    pub request: OneshotIngestionRequest,
}

impl RustType<ProtoRunOneshotIngestionCommand> for RunOneshotIngestionCommand {
    fn into_proto(&self) -> ProtoRunOneshotIngestionCommand {
        ProtoRunOneshotIngestionCommand {
            ingestion_id: Some(self.ingestion_id.into_proto()),
            collection_id: Some(self.collection_id.into_proto()),
            storage_metadata: Some(self.collection_meta.into_proto()),
            request: Some(self.request.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRunOneshotIngestionCommand) -> Result<Self, TryFromProtoError> {
        Ok(RunOneshotIngestionCommand {
            ingestion_id: proto
                .ingestion_id
                .into_rust_if_some("ProtoRunOneshotIngestionCommand::ingestion_id")?,
            collection_id: proto
                .collection_id
                .into_rust_if_some("ProtoRunOneshotIngestionCommand::collection_id")?,
            collection_meta: proto
                .storage_metadata
                .into_rust_if_some("ProtoRunOneshotIngestionCommand::storage_metadata")?,
            request: proto
                .request
                .into_rust_if_some("ProtoRunOneshotIngestionCommand::request")?,
        })
    }
}

impl RustType<ProtoRunSinkCommand> for RunSinkCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoRunSinkCommand {
        ProtoRunSinkCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRunSinkCommand) -> Result<Self, TryFromProtoError> {
        Ok(RunSinkCommand {
            id: proto.id.into_rust_if_some("ProtoRunSinkCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoRunSinkCommand::description")?,
        })
    }
}

/// A command that starts exporting the given sink description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunSinkCommand<T> {
    pub id: GlobalId,
    pub description: StorageSinkDesc<CollectionMetadata, T>,
}

impl Arbitrary for RunSinkCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>>(),
        )
            .prop_map(|(id, description)| Self { id, description })
            .boxed()
    }
}

impl RustType<ProtoStorageCommand> for StorageCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageCommand {
        use proto_storage_command::Kind::*;
        use proto_storage_command::*;
        ProtoStorageCommand {
            kind: Some(match self {
                StorageCommand::CreateTimely { config, epoch } => CreateTimely(ProtoCreateTimely {
                    config: Some(config.into_proto()),
                    epoch: Some(epoch.into_proto()),
                }),
                StorageCommand::InitializationComplete => InitializationComplete(()),
                StorageCommand::AllowWrites => AllowWrites(()),
                StorageCommand::UpdateConfiguration(params) => {
                    UpdateConfiguration(params.into_proto())
                }
                StorageCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                StorageCommand::RunIngestions(sources) => CreateSources(ProtoCreateSources {
                    sources: sources.into_proto(),
                }),
                StorageCommand::RunOneshotIngestion(oneshot) => {
                    OneshotIngestion(oneshot.into_proto())
                }
                StorageCommand::RunSinks(sinks) => RunSinks(ProtoRunSinks {
                    sinks: sinks.into_proto(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageCommand) -> Result<Self, TryFromProtoError> {
        use proto_storage_command::Kind::*;
        use proto_storage_command::*;
        match proto.kind {
            Some(CreateTimely(ProtoCreateTimely { config, epoch })) => {
                Ok(StorageCommand::CreateTimely {
                    config: config.into_rust_if_some("ProtoCreateTimely::config")?,
                    epoch: epoch.into_rust_if_some("ProtoCreateTimely::epoch")?,
                })
            }
            Some(InitializationComplete(())) => Ok(StorageCommand::InitializationComplete),
            Some(AllowWrites(())) => Ok(StorageCommand::AllowWrites),
            Some(UpdateConfiguration(params)) => {
                Ok(StorageCommand::UpdateConfiguration(params.into_rust()?))
            }
            Some(CreateSources(ProtoCreateSources { sources })) => {
                Ok(StorageCommand::RunIngestions(sources.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(StorageCommand::AllowCompaction(collections.into_rust()?))
            }
            Some(RunSinks(ProtoRunSinks { sinks })) => {
                Ok(StorageCommand::RunSinks(sinks.into_rust()?))
            }
            Some(OneshotIngestion(oneshot)) => {
                Ok(StorageCommand::RunOneshotIngestion(oneshot.into_rust()?))
            }
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageCommand::kind",
            )),
        }
    }
}

impl Arbitrary for StorageCommand<mz_repr::Timestamp> {
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
            // TODO(guswynn): cluster-unification: also test `CreateTimely` here.
            proptest::collection::vec(any::<RunIngestionCommand>(), 1..4)
                .prop_map(StorageCommand::RunIngestions)
                .boxed(),
            proptest::collection::vec(any::<RunSinkCommand<mz_repr::Timestamp>>(), 1..4)
                .prop_map(StorageCommand::RunSinks)
                .boxed(),
            proptest::collection::vec(
                (
                    any::<GlobalId>(),
                    proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
                ),
                1..4,
            )
            .prop_map(|collections| {
                StorageCommand::AllowCompaction(
                    collections
                        .into_iter()
                        .map(|(id, frontier_vec)| (id, Antichain::from(frontier_vec)))
                        .collect(),
                )
            })
            .boxed(),
        ])
    }
}

/// A "kind" enum for statuses tracked by the health operator
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    Starting,
    Running,
    Paused,
    Stalled,
    /// This status is currently unused.
    // re-design the ceased status
    Ceased,
    Dropped,
}

impl std::str::FromStr for Status {
    type Err = anyhow::Error;
    /// Keep in sync with [`Status::to_str`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "starting" => Status::Starting,
            "running" => Status::Running,
            "paused" => Status::Paused,
            "stalled" => Status::Stalled,
            "ceased" => Status::Ceased,
            "dropped" => Status::Dropped,
            s => return Err(anyhow::anyhow!("{} is not a valid status", s)),
        })
    }
}

impl Status {
    /// Keep in sync with `Status::from_str`.
    pub fn to_str(&self) -> &'static str {
        match self {
            Status::Starting => "starting",
            Status::Running => "running",
            Status::Paused => "paused",
            Status::Stalled => "stalled",
            Status::Ceased => "ceased",
            Status::Dropped => "dropped",
        }
    }

    /// Determines if a new status should be produced in context of a previous
    /// status.
    pub fn superseded_by(self, new: Status) -> bool {
        match (self, new) {
            (_, Status::Dropped) => true,
            (Status::Dropped, _) => false,
            // Don't re-mark that object as paused.
            (Status::Paused, Status::Paused) => false,
            // De-duplication of other statuses is currently managed by the
            // `health_operator`.
            _ => true,
        }
    }
}

/// A source or sink status update.
///
/// Represents a status update for a given object type. The inner value for each
/// variant should be able to be packed into a status row that conforms to the schema
/// for the object's status history relation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusUpdate {
    pub id: GlobalId,
    pub status: Status,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub error: Option<String>,
    pub hints: BTreeSet<String>,
    pub namespaced_errors: BTreeMap<String, String>,
}

impl StatusUpdate {
    pub fn new(
        id: GlobalId,
        timestamp: chrono::DateTime<chrono::Utc>,
        status: Status,
    ) -> StatusUpdate {
        StatusUpdate {
            id,
            timestamp,
            status,
            error: None,
            hints: Default::default(),
            namespaced_errors: Default::default(),
        }
    }
}

impl From<StatusUpdate> for Row {
    fn from(update: StatusUpdate) -> Self {
        use mz_repr::Datum;

        let timestamp = Datum::TimestampTz(update.timestamp.try_into().expect("must fit"));
        let id = update.id.to_string();
        let id = Datum::String(&id);
        let status = Datum::String(update.status.to_str());
        let error = update.error.as_deref().into();

        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([timestamp, id, status, error]);

        if !update.hints.is_empty() || !update.namespaced_errors.is_empty() {
            packer.push_dict_with(|dict_packer| {
                // `hint` and `namespaced` are ordered,
                // as well as the BTree's they each contain.
                if !update.hints.is_empty() {
                    dict_packer.push(Datum::String("hints"));
                    dict_packer.push_list(update.hints.iter().map(|s| Datum::String(s)));
                }
                if !update.namespaced_errors.is_empty() {
                    dict_packer.push(Datum::String("namespaced"));
                    dict_packer.push_dict(
                        update
                            .namespaced_errors
                            .iter()
                            .map(|(k, v)| (k.as_str(), Datum::String(v))),
                    );
                }
            });
        } else {
            packer.push(Datum::Null);
        }

        row
    }
}

impl RustType<proto_storage_response::ProtoStatus> for Status {
    fn into_proto(&self) -> proto_storage_response::ProtoStatus {
        use proto_storage_response::proto_status::*;

        proto_storage_response::ProtoStatus {
            kind: Some(match self {
                Status::Starting => Kind::Starting(()),
                Status::Running => Kind::Running(()),
                Status::Paused => Kind::Paused(()),
                Status::Stalled => Kind::Stalled(()),
                Status::Ceased => Kind::Ceased(()),
                Status::Dropped => Kind::Dropped(()),
            }),
        }
    }

    fn from_proto(proto: proto_storage_response::ProtoStatus) -> Result<Self, TryFromProtoError> {
        use proto_storage_response::proto_status::*;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoStatus::kind"))?;

        Ok(match kind {
            Kind::Starting(()) => Status::Starting,
            Kind::Running(()) => Status::Running,
            Kind::Paused(()) => Status::Paused,
            Kind::Stalled(()) => Status::Stalled,
            Kind::Ceased(()) => Status::Ceased,
            Kind::Dropped(()) => Status::Dropped,
        })
    }
}

impl RustType<proto_storage_response::ProtoStatusUpdate> for StatusUpdate {
    fn into_proto(&self) -> proto_storage_response::ProtoStatusUpdate {
        proto_storage_response::ProtoStatusUpdate {
            id: Some(self.id.into_proto()),
            status: Some(self.status.into_proto()),
            timestamp: Some(self.timestamp.into_proto()),
            error: self.error.clone(),
            hints: self.hints.iter().cloned().collect(),
            namespaced_errors: self.namespaced_errors.clone(),
        }
    }

    fn from_proto(
        proto: proto_storage_response::ProtoStatusUpdate,
    ) -> Result<Self, TryFromProtoError> {
        Ok(StatusUpdate {
            id: proto.id.into_rust_if_some("ProtoStatusUpdate::id")?,
            timestamp: proto
                .timestamp
                .into_rust_if_some("ProtoStatusUpdate::timestamp")?,
            status: proto
                .status
                .into_rust_if_some("ProtoStatusUpdate::status")?,
            error: proto.error,
            hints: proto.hints.into_iter().collect(),
            namespaced_errors: proto.namespaced_errors,
        })
    }
}

/// An update to an append only collection.
pub enum AppendOnlyUpdate {
    Row((Row, Diff)),
    Status(StatusUpdate),
}

impl AppendOnlyUpdate {
    pub fn into_row(self) -> (Row, Diff) {
        match self {
            AppendOnlyUpdate::Row((row, diff)) => (row, diff),
            AppendOnlyUpdate::Status(status) => (Row::from(status), 1),
        }
    }
}

impl From<(Row, Diff)> for AppendOnlyUpdate {
    fn from((row, diff): (Row, Diff)) -> Self {
        Self::Row((row, diff))
    }
}

impl From<StatusUpdate> for AppendOnlyUpdate {
    fn from(update: StatusUpdate) -> Self {
        Self::Status(update)
    }
}

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new upper frontiers.
    FrontierUppers(Vec<(GlobalId, Antichain<T>)>),
    /// Punctuation indicates that no more responses will be transmitted for the specified id
    DroppedId(GlobalId),
    /// Batches that have been staged in Persist and maybe will be linked into a shard.
    StagedBatches(BTreeMap<uuid::Uuid, Vec<Result<ProtoBatch, String>>>),

    /// A list of statistics updates, currently only for sources.
    StatisticsUpdates(Vec<SourceStatisticsUpdate>, Vec<SinkStatisticsUpdate>),
    /// A list of status updates for sources and sinks. Periodically sent from
    /// storage workers to convey the latest status information about an object.
    StatusUpdates(Vec<StatusUpdate>),
}

impl RustType<ProtoStorageResponse> for StorageResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageResponse {
        use proto_storage_response::Kind::*;
        use proto_storage_response::{
            ProtoDroppedId, ProtoStagedBatches, ProtoStatisticsUpdates, ProtoStatusUpdates,
        };
        ProtoStorageResponse {
            kind: Some(match self {
                StorageResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
                StorageResponse::DroppedId(id) => DroppedId(ProtoDroppedId {
                    id: Some(id.into_proto()),
                }),
                StorageResponse::StatisticsUpdates(source_stats, sink_stats) => {
                    Stats(ProtoStatisticsUpdates {
                        source_updates: source_stats
                            .iter()
                            .map(|update| update.into_proto())
                            .collect(),
                        sink_updates: sink_stats
                            .iter()
                            .map(|update| update.into_proto())
                            .collect(),
                    })
                }
                StorageResponse::StatusUpdates(updates) => StatusUpdates(ProtoStatusUpdates {
                    updates: updates.into_proto(),
                }),
                StorageResponse::StagedBatches(staged) => {
                    let batches = staged
                        .into_iter()
                        .map(|(collection_id, batches)| {
                            let batches = batches
                                .into_iter()
                                .map(|result| {
                                    use proto_storage_response::proto_staged_batches::batch_result::Value;
                                    let value = match result {
                                        Ok(batch) => Value::Batch(batch.clone()),
                                        Err(err) => Value::Error(err.clone()),
                                    };
                                    proto_storage_response::proto_staged_batches::BatchResult { value: Some(value) }
                                })
                                .collect();
                            proto_storage_response::proto_staged_batches::Inner {
                                id: Some(collection_id.into_proto()),
                                batches,
                            }
                        })
                        .collect();
                    StagedBatches(ProtoStagedBatches { batches })
                }
            }),
        }
    }

    fn from_proto(proto: ProtoStorageResponse) -> Result<Self, TryFromProtoError> {
        use proto_storage_response::Kind::*;
        use proto_storage_response::{ProtoDroppedId, ProtoStatusUpdates};
        match proto.kind {
            Some(DroppedId(ProtoDroppedId { id })) => Ok(StorageResponse::DroppedId(
                id.into_rust_if_some("ProtoDroppedId::id")?,
            )),
            Some(FrontierUppers(traces)) => {
                Ok(StorageResponse::FrontierUppers(traces.into_rust()?))
            }
            Some(Stats(stats)) => Ok(StorageResponse::StatisticsUpdates(
                stats
                    .source_updates
                    .into_iter()
                    .map(|update| update.into_rust())
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
                stats
                    .sink_updates
                    .into_iter()
                    .map(|update| update.into_rust())
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
            )),
            Some(StatusUpdates(ProtoStatusUpdates { updates })) => {
                Ok(StorageResponse::StatusUpdates(updates.into_rust()?))
            }
            Some(StagedBatches(staged)) => {
                let batches: BTreeMap<_, _> = staged
                    .batches
                    .into_iter()
                    .map(|inner| {
                        let id = inner
                            .id
                            .into_rust_if_some("ProtoStagedBatches::Inner::id")?;

                        let mut batches = Vec::with_capacity(inner.batches.len());
                        for maybe_batch in inner.batches {
                            use proto_storage_response::proto_staged_batches::batch_result::Value;

                            let value = maybe_batch.value.ok_or_else(|| {
                                TryFromProtoError::missing_field("BatchResult::value")
                            })?;
                            let batch = match value {
                                Value::Batch(batch) => Ok(batch),
                                Value::Error(err) => Err(err),
                            };
                            batches.push(batch);
                        }

                        Ok::<_, TryFromProtoError>((id, batches))
                    })
                    .collect::<Result<_, _>>()?;

                Ok(StorageResponse::StagedBatches(batches))
            }
            None => Err(TryFromProtoError::missing_field(
                "ProtoStorageResponse::kind",
            )),
        }
    }
}

impl Arbitrary for StorageResponse<mz_repr::Timestamp> {
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        // TODO(guswynn): test `SourceStatisticsUpdates`
        Union::new(vec![proptest::collection::vec(
            (any::<GlobalId>(), any_antichain()),
            1..4,
        )
        .prop_map(StorageResponse::FrontierUppers)
        .boxed()])
    }
}

/// Maintained state for partitioned storage clients.
///
/// This helper type unifies the responses of multiple partitioned
/// workers in order to present as a single worker.
#[derive(Debug)]
pub struct PartitionedStorageState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// Upper frontiers for sources and sinks, both unioned across all partitions and from each
    /// individual partition.
    uppers: BTreeMap<GlobalId, (MutableAntichain<T>, Vec<Option<Antichain<T>>>)>,
    /// Staged batches from oneshot sources that will get appended by `environmentd`.
    oneshot_source_responses:
        BTreeMap<uuid::Uuid, BTreeMap<usize, Vec<Result<ProtoBatch, String>>>>,
}

impl<T> Partitionable<StorageCommand<T>, StorageResponse<T>>
    for (StorageCommand<T>, StorageResponse<T>)
where
    T: timely::progress::Timestamp + Lattice,
{
    type PartitionedState = PartitionedStorageState<T>;

    fn new(parts: usize) -> PartitionedStorageState<T> {
        PartitionedStorageState {
            parts,
            uppers: BTreeMap::new(),
            oneshot_source_responses: BTreeMap::new(),
        }
    }
}

impl<T> PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn observe_command(&mut self, command: &StorageCommand<T>) {
        // Note that `observe_command` is quite different in `mz_compute_client`.
        // Compute (currently) only sends the command to 1 process,
        // but storage fans out to all workers, allowing the storage processes
        // to self-coordinate how commands and internal commands are ordered.
        //
        // TODO(guswynn): cluster-unification: consolidate this with compute.
        let _ = match command {
            StorageCommand::CreateTimely { .. } => {
                // Similarly, we don't reset state here like compute, because,
                // until we are required to manage multiple replicas, we can handle
                // keeping track of state across restarts of storage server(s).
            }
            StorageCommand::RunIngestions(ingestions) => ingestions
                .iter()
                .for_each(|i| self.insert_new_uppers(i.description.collection_ids())),
            StorageCommand::RunSinks(exports) => {
                exports.iter().for_each(|e| self.insert_new_uppers([e.id]))
            }
            StorageCommand::InitializationComplete
            | StorageCommand::AllowWrites
            | StorageCommand::UpdateConfiguration(_)
            | StorageCommand::AllowCompaction(_)
            | StorageCommand::RunOneshotIngestion(_) => {}
        };
    }

    /// Shared implementation for commands that install uppers with controllable behavior with
    /// encountering existing uppers.
    ///
    /// If any ID was previously tracked in `self` and `skip_existing` is `false`, we return the ID
    /// as an error.
    fn insert_new_uppers<I: IntoIterator<Item = GlobalId>>(&mut self, ids: I) {
        for id in ids {
            self.uppers.entry(id).or_insert_with(|| {
                let mut frontier = MutableAntichain::new();
                // TODO(guswynn): cluster-unification: fix this dangerous use of `as`, by
                // merging the types that compute and storage use.
                #[allow(clippy::as_conversions)]
                frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                let part_frontiers = vec![Some(Antichain::from_elem(T::minimum())); self.parts];

                (frontier, part_frontiers)
            });
        }
    }
}

impl<T> PartitionedState<StorageCommand<T>, StorageResponse<T>> for PartitionedStorageState<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    fn split_command(&mut self, command: StorageCommand<T>) -> Vec<Option<StorageCommand<T>>> {
        self.observe_command(&command);

        match command {
            StorageCommand::CreateTimely { config, epoch } => {
                let timely_cmds = config.split_command(self.parts);

                let timely_cmds = timely_cmds
                    .into_iter()
                    .map(|config| Some(StorageCommand::CreateTimely { config, epoch }))
                    .collect();
                timely_cmds
            }
            command => {
                // Fan out to all processes (which will fan out to all workers).
                // StorageState manages ordering of commands internally.
                vec![Some(command); self.parts]
            }
        }
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        response: StorageResponse<T>,
    ) -> Option<Result<StorageResponse<T>, anyhow::Error>> {
        match response {
            // Avoid multiple retractions of minimum time, to present as updates from one worker.
            StorageResponse::FrontierUppers(list) => {
                let mut new_uppers = Vec::new();

                for (id, new_shard_upper) in list {
                    let (frontier, shard_frontiers) = match self.uppers.get_mut(&id) {
                        Some(value) => value,
                        None => panic!("Reference to absent collection: {id}"),
                    };
                    let old_upper = frontier.frontier().to_owned();
                    let shard_upper = match &mut shard_frontiers[shard_id] {
                        Some(shard_upper) => shard_upper,
                        None => panic!("Reference to absent shard {shard_id} for collection {id}"),
                    };
                    frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), -1)));
                    frontier.update_iter(new_shard_upper.iter().map(|t| (t.clone(), 1)));
                    shard_upper.join_assign(&new_shard_upper);

                    let new_upper = frontier.frontier();
                    if PartialOrder::less_than(&old_upper.borrow(), &new_upper) {
                        new_uppers.push((id, new_upper.to_owned()));
                    }
                }

                if new_uppers.is_empty() {
                    None
                } else {
                    Some(Ok(StorageResponse::FrontierUppers(new_uppers)))
                }
            }
            StorageResponse::DroppedId(id) => {
                let (_, shard_frontiers) = match self.uppers.get_mut(&id) {
                    Some(value) => value,
                    None => panic!("Reference to absent collection: {id}"),
                };
                let prev = shard_frontiers[shard_id].take();
                assert!(
                    prev.is_some(),
                    "got double drop for {id} from shard {shard_id}"
                );

                if shard_frontiers.iter().all(Option::is_none) {
                    self.uppers.remove(&id);
                    Some(Ok(StorageResponse::DroppedId(id)))
                } else {
                    None
                }
            }
            StorageResponse::StatisticsUpdates(source_stats, sink_stats) => {
                // Just forward it along; the `worker_id` should have been set in `storage_state`.
                // We _could_ consolidate across worker_id's, here, but each worker only produces
                // responses periodically, so we avoid that complexity.
                Some(Ok(StorageResponse::StatisticsUpdates(
                    source_stats,
                    sink_stats,
                )))
            }
            StorageResponse::StatusUpdates(updates) => {
                Some(Ok(StorageResponse::StatusUpdates(updates)))
            }
            StorageResponse::StagedBatches(batches) => {
                let mut finished_batches = BTreeMap::new();

                for (collection_id, batches) in batches {
                    tracing::info!(%shard_id, %collection_id, "got batch");

                    let entry = self
                        .oneshot_source_responses
                        .entry(collection_id)
                        .or_default();
                    let novel = entry.insert(shard_id, batches);
                    assert_none!(novel, "Duplicate oneshot source response");

                    // Check if we've received responses from all shards.
                    if entry.len() == self.parts {
                        let entry = self
                            .oneshot_source_responses
                            .remove(&collection_id)
                            .expect("checked above");
                        let all_batches: Vec<_> = entry.into_values().flatten().collect();

                        finished_batches.insert(collection_id, all_batches);
                    }
                }

                if !finished_batches.is_empty() {
                    Some(Ok(StorageResponse::StagedBatches(finished_batches)))
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update<T = mz_repr::Timestamp> {
    pub row: Row,
    pub timestamp: T,
    pub diff: Diff,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input; however, the input must
/// determine the most appropriate timestamps to use.
///
/// TODO(cf2): Can we remove this and use only on [`TableData`].
pub struct TimestamplessUpdate {
    pub row: Row,
    pub diff: Diff,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableData {
    /// Rows that still need to be persisted and appended.
    ///
    /// The contained [`Row`]s are _not_ consolidated.
    Rows(Vec<(Row, Diff)>),
    /// Batches already staged in Persist ready to be appended.
    Batches(SmallVec<[ProtoBatch; 1]>),
}

impl TableData {
    pub fn is_empty(&self) -> bool {
        match self {
            TableData::Rows(rows) => rows.is_empty(),
            TableData::Batches(batches) => batches.is_empty(),
        }
    }
}

impl RustType<ProtoTrace> for (GlobalId, Antichain<mz_repr::Timestamp>) {
    fn into_proto(&self) -> ProtoTrace {
        ProtoTrace {
            id: Some(self.0.into_proto()),
            upper: Some(self.1.into_proto()),
        }
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.id.into_rust_if_some("ProtoTrace::id")?,
            proto.upper.into_rust_if_some("ProtoTrace::upper")?,
        ))
    }
}

impl RustType<ProtoFrontierUppersKind> for Vec<(GlobalId, Antichain<mz_repr::Timestamp>)> {
    fn into_proto(&self) -> ProtoFrontierUppersKind {
        ProtoFrontierUppersKind {
            traces: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoFrontierUppersKind) -> Result<Self, TryFromProtoError> {
        proto.traces.into_rust()
    }
}

impl RustType<ProtoCompaction> for (GlobalId, Antichain<mz_repr::Timestamp>) {
    fn into_proto(&self) -> ProtoCompaction {
        ProtoCompaction {
            id: Some(self.0.into_proto()),
            frontier: Some(self.1.into_proto()),
        }
    }

    fn from_proto(proto: ProtoCompaction) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.id.into_rust_if_some("ProtoCompaction::id")?,
            proto
                .frontier
                .into_rust_if_some("ProtoCompaction::frontier")?,
        ))
    }
}

impl TryIntoTimelyConfig for StorageCommand {
    fn try_into_timely_config(self) -> Result<(TimelyConfig, ClusterStartupEpoch), Self> {
        match self {
            StorageCommand::CreateTimely { config, epoch } => Ok((config, epoch)),
            cmd => Err(cmd),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn storage_command_protobuf_roundtrip(expect in any::<StorageCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoStorageCommand>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn storage_response_protobuf_roundtrip(expect in any::<StorageResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoStorageResponse>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
