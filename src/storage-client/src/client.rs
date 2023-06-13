// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]
// Tonic generates code that calls clone on an Arc. Allow this here.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::clone_on_ref_ptr)]

//! The public API of the storage layer.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::iter;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_ore::cast::CastFrom;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{GrpcClient, GrpcServer, ProtoServiceTypes, ResponseStream};
use mz_timely_util::progress::any_antichain;
use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::PartialOrder;
use tonic::{Request, Status, Streaming};

use crate::client::proto_storage_server::ProtoStorage;
use crate::controller::CollectionMetadata;
use crate::metrics::RehydratingStorageClientMetrics;
use crate::types::parameters::StorageParameters;
use crate::types::sinks::{MetadataFilled, StorageSinkDesc};
use crate::types::sources::IngestionDescription;

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

    async fn recv(&mut self) -> Result<Option<StorageResponse<T>>, anyhow::Error> {
        (**self).recv().await
    }
}

#[derive(Debug, Clone)]
pub enum StorageProtoServiceTypes {}

impl ProtoServiceTypes for StorageProtoServiceTypes {
    type PC = ProtoStorageCommand;
    type PR = ProtoStorageResponse;
    type STATS = RehydratingStorageClientMetrics;
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
    ) -> Result<tonic::Response<Self::CommandResponseStreamStream>, Status> {
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
    /// Update storage instance configuration.
    UpdateConfiguration(StorageParameters),
    /// Create the enumerated sources, each associated with its identifier.
    CreateSources(Vec<CreateSourceCommand>),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
    CreateSinks(Vec<CreateSinkCommand<T>>),
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateSourceCommand {
    /// The id of the storage collection being ingested.
    pub id: GlobalId,
    /// The description of what source type should be ingested and what post-processing steps must
    /// be applied to the data before writing them down into the storage collection
    pub description: IngestionDescription<CollectionMetadata>,
}

impl Arbitrary for CreateSourceCommand {
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

impl RustType<ProtoCreateSourceCommand> for CreateSourceCommand {
    fn into_proto(&self) -> ProtoCreateSourceCommand {
        ProtoCreateSourceCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoCreateSourceCommand) -> Result<Self, TryFromProtoError> {
        Ok(CreateSourceCommand {
            id: proto.id.into_rust_if_some("ProtoCreateSourceCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoCreateSourceCommand::description")?,
        })
    }
}

impl RustType<ProtoCreateSinkCommand> for CreateSinkCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoCreateSinkCommand {
        ProtoCreateSinkCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoCreateSinkCommand) -> Result<Self, TryFromProtoError> {
        Ok(CreateSinkCommand {
            id: proto.id.into_rust_if_some("ProtoCreateSinkCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoCreateSinkCommand::description")?,
        })
    }
}

/// A command that starts exporting the given sink description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CreateSinkCommand<T> {
    pub id: GlobalId,
    pub description: StorageSinkDesc<MetadataFilled, T>,
}

impl Arbitrary for CreateSinkCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>>(),
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
                StorageCommand::UpdateConfiguration(params) => {
                    UpdateConfiguration(params.into_proto())
                }
                StorageCommand::CreateSources(sources) => CreateSources(ProtoCreateSources {
                    sources: sources.into_proto(),
                }),
                StorageCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                StorageCommand::CreateSinks(sinks) => CreateSinks(ProtoCreateSinks {
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
            Some(UpdateConfiguration(params)) => {
                Ok(StorageCommand::UpdateConfiguration(params.into_rust()?))
            }
            Some(CreateSources(ProtoCreateSources { sources })) => {
                Ok(StorageCommand::CreateSources(sources.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(StorageCommand::AllowCompaction(collections.into_rust()?))
            }
            Some(CreateSinks(ProtoCreateSinks { sinks })) => {
                Ok(StorageCommand::CreateSinks(sinks.into_rust()?))
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
            proptest::collection::vec(any::<CreateSourceCommand>(), 1..4)
                .prop_map(StorageCommand::CreateSources)
                .boxed(),
            proptest::collection::vec(any::<CreateSinkCommand<mz_repr::Timestamp>>(), 1..4)
                .prop_map(StorageCommand::CreateSinks)
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

// These structure represents a full set up updates for the `mz_source_statistics`
// and `mz_sink_statistics` tables for a specific source-worker/sink-worker pair.
// They are structured like this for simplicity
// and efficiency: Each storage worker can individually collect and consolidate metrics,
// then control how much `StorageResponse` traffic is produced when sending updates
// back to the controller to be written.

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SourceStatisticsUpdate {
    pub id: GlobalId,
    pub worker_id: usize,
    pub snapshot_committed: bool,
    pub messages_received: u64,
    pub updates_staged: u64,
    pub updates_committed: u64,
    pub bytes_received: u64,
    pub envelope_state_bytes: u64,
    pub envelope_state_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinkStatisticsUpdate {
    pub id: GlobalId,
    pub worker_id: usize,
    pub messages_staged: u64,
    pub messages_committed: u64,
    pub bytes_staged: u64,
    pub bytes_committed: u64,
}

/// A trait that abstracts over user-facing statistics objects, used
/// by `spawn_statistics_scraper`.
pub trait PackableStats {
    /// Pack `self` into the `Row`.
    fn pack(&self, packer: mz_repr::RowPacker<'_>);
}
impl PackableStats for SourceStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(u64::cast_from(self.worker_id)));
        packer.push(Datum::from(self.snapshot_committed));
        packer.push(Datum::from(self.messages_received));
        packer.push(Datum::from(self.updates_staged));
        packer.push(Datum::from(self.updates_committed));
        packer.push(Datum::from(self.bytes_received));
        packer.push(Datum::from(self.envelope_state_bytes));
        packer.push(Datum::from(self.envelope_state_count));
    }
}
impl PackableStats for SinkStatisticsUpdate {
    fn pack(&self, mut packer: mz_repr::RowPacker<'_>) {
        use mz_repr::Datum;
        packer.push(Datum::from(self.id.to_string().as_str()));
        packer.push(Datum::from(u64::cast_from(self.worker_id)));
        packer.push(Datum::from(self.messages_staged));
        packer.push(Datum::from(self.messages_committed));
        packer.push(Datum::from(self.bytes_staged));
        packer.push(Datum::from(self.bytes_committed));
    }
}

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new upper frontiers.
    ///
    /// TODO(teskje): Consider also reporting the previous upper frontier and using that
    /// information to assert the correct implementation of our protocols at various places.
    FrontierUppers(Vec<(GlobalId, Antichain<T>)>),
    /// Punctuation indicates that no more responses will be transmitted for the specified ids
    DroppedIds(BTreeSet<GlobalId>),

    /// A list of statistics updates, currently only for sources.
    StatisticsUpdates(Vec<SourceStatisticsUpdate>, Vec<SinkStatisticsUpdate>),
}

impl RustType<ProtoStorageResponse> for StorageResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageResponse {
        use proto_storage_response::Kind::*;
        use proto_storage_response::{
            ProtoDroppedIds, ProtoSinkStatisticsUpdate, ProtoSourceStatisticsUpdate,
            ProtoStatisticsUpdates,
        };
        ProtoStorageResponse {
            kind: Some(match self {
                StorageResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
                StorageResponse::DroppedIds(ids) => DroppedIds(ProtoDroppedIds {
                    ids: ids.into_proto(),
                }),
                StorageResponse::StatisticsUpdates(source_stats, sink_stats) => {
                    Stats(ProtoStatisticsUpdates {
                        source_updates: source_stats
                            .iter()
                            .map(|update| ProtoSourceStatisticsUpdate {
                                id: Some(update.id.into_proto()),
                                worker_id: u64::cast_from(update.worker_id),
                                snapshot_committed: update.snapshot_committed,
                                messages_received: update.messages_received,
                                updates_staged: update.updates_staged,
                                updates_committed: update.updates_committed,
                                bytes_received: update.bytes_received,
                                envelope_state_bytes: update.envelope_state_bytes,
                                envelope_state_count: update.envelope_state_count,
                            })
                            .collect(),
                        sink_updates: sink_stats
                            .iter()
                            .map(|update| ProtoSinkStatisticsUpdate {
                                id: Some(update.id.into_proto()),
                                worker_id: u64::cast_from(update.worker_id),
                                messages_staged: update.messages_staged,
                                messages_committed: update.messages_committed,
                                bytes_staged: update.bytes_staged,
                                bytes_committed: update.bytes_committed,
                            })
                            .collect(),
                    })
                }
            }),
        }
    }

    fn from_proto(proto: ProtoStorageResponse) -> Result<Self, TryFromProtoError> {
        use proto_storage_response::Kind::*;
        use proto_storage_response::ProtoDroppedIds;
        match proto.kind {
            Some(DroppedIds(ProtoDroppedIds { ids })) => {
                Ok(StorageResponse::DroppedIds(ids.into_rust()?))
            }
            Some(FrontierUppers(traces)) => {
                Ok(StorageResponse::FrontierUppers(traces.into_rust()?))
            }
            Some(Stats(stats)) => Ok(StorageResponse::StatisticsUpdates(
                stats
                    .source_updates
                    .into_iter()
                    .map(|update| {
                        Ok(SourceStatisticsUpdate {
                            id: update.id.into_rust_if_some(
                                "ProtoStorageResponse::stats::source_updates::id",
                            )?,
                            worker_id: usize::cast_from(update.worker_id),
                            snapshot_committed: update.snapshot_committed,
                            messages_received: update.messages_received,
                            updates_staged: update.updates_staged,
                            updates_committed: update.updates_committed,
                            bytes_received: update.bytes_received,
                            envelope_state_bytes: update.envelope_state_bytes,
                            envelope_state_count: update.envelope_state_count,
                        })
                    })
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
                stats
                    .sink_updates
                    .into_iter()
                    .map(|update| {
                        Ok(SinkStatisticsUpdate {
                            id: update.id.into_rust_if_some(
                                "ProtoStorageResponse::stats::sink_updates::id",
                            )?,
                            worker_id: usize::cast_from(update.worker_id),
                            messages_staged: update.messages_staged,
                            messages_committed: update.messages_committed,
                            bytes_staged: update.bytes_staged,
                            bytes_committed: update.bytes_committed,
                        })
                    })
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
            )),
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
        // but storage fan's out to all workers, allowing the storage processes
        // to self-coordinate how commands and internal commands are ordered.
        //
        // TODO(guswynn): cluster-unification: consolidate this with compute.
        match command {
            StorageCommand::CreateTimely { .. } => {
                // Similarly, we don't reset state here like compute, because,
                // until we are required to manage multiple replicas, we can handle
                // keeping track of state across restarts of storage server(s).
            }
            StorageCommand::CreateSources(ingestions) => {
                for ingestion in ingestions {
                    for export_id in ingestion.description.subsource_ids() {
                        let mut frontier = MutableAntichain::new();
                        // TODO(guswynn): cluster-unification: fix this dangerous use of `as`, by
                        // merging the types that compute and storage use.
                        #[allow(clippy::as_conversions)]
                        frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                        let part_frontiers =
                            vec![Some(Antichain::from_elem(T::minimum())); self.parts];
                        let previous = self.uppers.insert(export_id, (frontier, part_frontiers));
                        assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", export_id, command);
                    }
                }
            }
            StorageCommand::CreateSinks(exports) => {
                for export in exports {
                    let mut frontier = MutableAntichain::new();
                    // TODO(guswynn): cluster-unification: fix this dangerous use of `as`, by
                    // merging the types that compute and storage use.
                    #[allow(clippy::as_conversions)]
                    frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                    let part_frontiers = vec![Some(Antichain::from_elem(T::minimum())); self.parts];
                    let previous = self.uppers.insert(export.id, (frontier, part_frontiers));
                    assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", export.id, command);
                }
            }
            StorageCommand::InitializationComplete
            | StorageCommand::UpdateConfiguration(_)
            | StorageCommand::AllowCompaction(_) => {
                // Other commands have no known impact on frontier tracking.
            }
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
            StorageResponse::DroppedIds(dropped_ids) => {
                let mut new_drops = BTreeSet::new();

                for id in dropped_ids {
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
                        new_drops.insert(id);
                    }
                }

                if new_drops.is_empty() {
                    None
                } else {
                    Some(Ok(StorageResponse::DroppedIds(new_drops)))
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
pub struct TimestamplessUpdate {
    pub row: Row,
    pub diff: Diff,
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

#[cfg(test)]
mod tests {
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
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[mz_ore::test]
        fn storage_response_protobuf_roundtrip(expect in any::<StorageResponse<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoStorageResponse>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
