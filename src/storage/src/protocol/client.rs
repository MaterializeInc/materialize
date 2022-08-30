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

use std::collections::HashMap;
use std::fmt::Debug;
use std::iter;

use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use proptest::prelude::{any, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::PartialOrder;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{BidiProtoClient, ClientTransport, GrpcClient, GrpcServer, ResponseStream};
use mz_timely_util::progress::any_antichain;

use crate::controller::CollectionMetadata;
use crate::protocol::client::proto_storage_client::ProtoStorageClient;
use crate::protocol::client::proto_storage_server::ProtoStorage;
use crate::types::sinks::StorageSinkDesc;
use crate::types::sources::IngestionDescription;

include!(concat!(env!("OUT_DIR"), "/mz_storage.protocol.client.rs"));

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

pub type StorageGrpcClient = GrpcClient<ProtoStorageClient<ClientTransport>>;

#[async_trait]
impl BidiProtoClient for ProtoStorageClient<ClientTransport> {
    type PC = ProtoStorageCommand;
    type PR = ProtoStorageResponse;

    fn new(inner: ClientTransport) -> Self {
        ProtoStorageClient::new(inner)
    }

    async fn establish_bidi_stream(
        &mut self,
        rx: UnboundedReceiverStream<Self::PC>,
    ) -> Result<Response<Streaming<Self::PR>>, Status> {
        self.command_response_stream(rx).await
    }
}

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
    /// Indicates that the controller has sent all commands reflecting its
    /// initial state.
    InitializationComplete,
    /// Create the enumerated sources, each associated with its identifier.
    IngestSources(Vec<IngestSourceCommand<T>>),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
    ExportSinks(Vec<ExportSinkCommand<T>>),
}

/// A command that starts ingesting the given ingestion description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct IngestSourceCommand<T> {
    /// The id of the storage collection being ingested.
    pub id: GlobalId,
    /// The description of what source type should be ingested and what post-processing steps must
    /// be applied to the data before writing them down into the storage collection
    pub description: IngestionDescription<CollectionMetadata>,
    /// The upper frontier that this ingestion should resume at
    pub resume_upper: Antichain<T>,
}

impl Arbitrary for IngestSourceCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<IngestionDescription<CollectionMetadata>>(),
            proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4).prop_map(Antichain::from),
        )
            .prop_map(|(id, description, resume_upper)| Self {
                id,
                description,
                resume_upper,
            })
            .boxed()
    }
}

impl RustType<ProtoIngestSourceCommand> for IngestSourceCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoIngestSourceCommand {
        ProtoIngestSourceCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
            resume_upper: Some((&self.resume_upper).into()),
        }
    }

    fn from_proto(proto: ProtoIngestSourceCommand) -> Result<Self, TryFromProtoError> {
        Ok(IngestSourceCommand {
            id: proto.id.into_rust_if_some("ProtoIngestSourceCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoIngestSourceCommand::description")?,
            resume_upper: proto.resume_upper.map(Into::into).ok_or_else(|| {
                TryFromProtoError::missing_field("ProtoIngestSourceCommand::resume_upper")
            })?,
        })
    }
}

impl RustType<ProtoExportSinkCommand> for ExportSinkCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoExportSinkCommand {
        ProtoExportSinkCommand {
            id: Some(self.id.into_proto()),
            description: Some(self.description.into_proto()),
        }
    }

    fn from_proto(proto: ProtoExportSinkCommand) -> Result<Self, TryFromProtoError> {
        Ok(ExportSinkCommand {
            id: proto.id.into_rust_if_some("ProtoExportSinkCommand::id")?,
            description: proto
                .description
                .into_rust_if_some("ProtoExportSinkCommand::description")?,
        })
    }
}

/// A command that starts exporting the given sink description
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExportSinkCommand<T> {
    pub id: GlobalId,
    pub description: StorageSinkDesc<CollectionMetadata, T>,
}

impl Arbitrary for ExportSinkCommand<mz_repr::Timestamp> {
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
        ProtoStorageCommand {
            kind: Some(match self {
                StorageCommand::InitializationComplete => InitializationComplete(()),
                StorageCommand::IngestSources(ingestions) => IngestSources(ProtoIngestSources {
                    ingestions: ingestions.into_proto(),
                }),
                StorageCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                StorageCommand::ExportSinks(exports) => ExportSinks(ProtoExportSinks {
                    exports: exports.into_proto(),
                }),
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
            Some(InitializationComplete(())) => Ok(StorageCommand::InitializationComplete),
            Some(ExportSinks(ProtoExportSinks { exports })) => {
                Ok(StorageCommand::ExportSinks(exports.into_rust()?))
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
            proptest::collection::vec(any::<IngestSourceCommand<mz_repr::Timestamp>>(), 1..4)
                .prop_map(StorageCommand::IngestSources),
            proptest::collection::vec(any::<ExportSinkCommand<mz_repr::Timestamp>>(), 1..4)
                .prop_map(StorageCommand::ExportSinks),
            proptest::collection::vec(
                (
                    any::<GlobalId>(),
                    proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4)
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

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new upper frontiers.
    ///
    /// TODO(teskje): Consider also reporting the previous upper frontier and using that
    /// information to assert the correct implementation of our protocols at various places.
    FrontierUppers(Vec<(GlobalId, Antichain<T>)>),
}

impl RustType<ProtoStorageResponse> for StorageResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageResponse {
        use proto_storage_response::Kind::*;
        ProtoStorageResponse {
            kind: Some(match self {
                StorageResponse::FrontierUppers(traces) => FrontierUppers(traces.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageResponse) -> Result<Self, TryFromProtoError> {
        use proto_storage_response::Kind::*;
        match proto.kind {
            Some(FrontierUppers(traces)) => {
                Ok(StorageResponse::FrontierUppers(traces.into_rust()?))
            }
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
            proptest::collection::vec((any::<GlobalId>(), any_antichain()), 1..4)
                .prop_map(StorageResponse::FrontierUppers),
        ]
        .boxed()
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
    uppers: HashMap<GlobalId, (MutableAntichain<T>, Vec<Antichain<T>>)>,
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
            uppers: HashMap::new(),
        }
    }
}

impl<T> PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn observe_command(&mut self, command: &StorageCommand<T>) {
        match command {
            StorageCommand::IngestSources(ingestions) => {
                for ingestion in ingestions {
                    let mut frontier = MutableAntichain::new();
                    frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                    let part_frontiers = vec![Antichain::from_elem(T::minimum()); self.parts];
                    let previous = self.uppers.insert(ingestion.id, (frontier, part_frontiers));
                    assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", ingestion.id, command);
                }
            }
            StorageCommand::ExportSinks(exports) => {
                for export in exports {
                    let mut frontier = MutableAntichain::new();
                    frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                    let part_frontiers = vec![Antichain::from_elem(T::minimum()); self.parts];
                    let previous = self.uppers.insert(export.id, (frontier, part_frontiers));
                    assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", export.id, command);
                }
            }
            StorageCommand::AllowCompaction(_) | StorageCommand::InitializationComplete => {
                // Other commands have no known impact on frontier tracking.
            }
        }
    }
}

impl<T> PartitionedState<StorageCommand<T>, StorageResponse<T>> for PartitionedStorageState<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    fn split_command(&mut self, command: StorageCommand<T>) -> Vec<StorageCommand<T>> {
        self.observe_command(&command);

        vec![command; self.parts]
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
                    if let Some((frontier, shard_frontiers)) = self.uppers.get_mut(&id) {
                        let old_upper = frontier.frontier().to_owned();
                        let shard_upper = &mut shard_frontiers[shard_id];
                        frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), -1)));
                        frontier.update_iter(new_shard_upper.iter().map(|t| (t.clone(), 1)));
                        shard_upper.join_assign(&new_shard_upper);

                        let new_upper = frontier.frontier();
                        if PartialOrder::less_than(&old_upper.borrow(), &new_upper) {
                            new_uppers.push((id, new_upper.to_owned()));
                        }
                    }
                }

                if new_uppers.is_empty() {
                    None
                } else {
                    Some(Ok(StorageResponse::FrontierUppers(new_uppers)))
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

impl RustType<ProtoTrace> for (GlobalId, Antichain<mz_repr::Timestamp>) {
    fn into_proto(&self) -> ProtoTrace {
        ProtoTrace {
            id: Some(self.0.into_proto()),
            upper: Some((&self.1).into()),
        }
    }

    fn from_proto(proto: ProtoTrace) -> Result<Self, TryFromProtoError> {
        Ok((
            proto.id.into_rust_if_some("ProtoTrace::id")?,
            proto
                .upper
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoTrace::upper"))?,
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

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

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
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
