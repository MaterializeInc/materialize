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
use std::iter;

use async_trait::async_trait;
use proptest::prelude::{any, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::progress::ChangeBatch;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use mz_proto::any_uuid;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{
    BidiProtoClient, GrpcClient, GrpcServer, GrpcServerCommand, ResponseStream,
};
use mz_timely_util::progress::any_change_batch;

use crate::client::controller::CollectionMetadata;
use crate::client::proto_storage_client::ProtoStorageClient;
use crate::client::proto_storage_server::ProtoStorage;
use crate::client::sources::IngestionDescription;

pub mod connections;
pub mod controller;
pub mod errors;
pub mod sinks;
pub mod sources;
pub mod transforms;

include!(concat!(env!("OUT_DIR"), "/mz_storage.client.rs"));

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

pub type StorageGrpcClient = GrpcClient<
    proto_storage_client::ProtoStorageClient<tonic::transport::Channel>,
    ProtoStorageCommand,
    ProtoStorageResponse,
>;

#[async_trait]
impl BidiProtoClient<ProtoStorageCommand, ProtoStorageResponse> for ProtoStorageClient<Channel> {
    async fn connect(addr: String) -> Result<Self, tonic::transport::Error>
    where
        Self: Sized,
    {
        ProtoStorageClient::connect(addr).await
    }

    async fn establish_bidi_stream(
        &mut self,
        rx: UnboundedReceiverStream<ProtoStorageCommand>,
    ) -> Result<Response<Streaming<ProtoStorageResponse>>, Status> {
        self.command_response_stream(rx).await
    }
}

#[async_trait]
impl<G> ProtoStorage for GrpcServer<G>
where
    G: GenericClient<GrpcServerCommand<StorageCommand>, StorageResponse> + 'static,
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
    /// Create the enumerated sources, each associated with its identifier.
    IngestSources(Vec<IngestSourceCommand<T>>),
    /// Enable compaction in storage-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),
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
            resume_upper: proto
                .resume_upper
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoCompaction::resume_upper"))?,
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
            proptest::collection::vec(any::<IngestSourceCommand<mz_repr::Timestamp>>(), 1..4)
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

/// Responses that the storage nature of a worker/dataflow can provide back to the coordinator.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum StorageResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with prior and new upper frontiers.
    FrontierUppers(Vec<(GlobalId, ChangeBatch<T>)>),

    // TODO(benesch,gus): remove this variant, because it is not produced by
    // storaged processes.
    /// Data about timestamp bindings, sent to the coordinator, in service
    /// of a specific "linearized" read request
    LinearizedTimestamps(LinearizedTimestampBindingFeedback<T>),
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

/// Maintained state for partitioned storage clients.
///
/// This helper type unifies the responses of multiple partitioned
/// workers in order to present as a single worker.
#[derive(Debug)]
pub struct PartitionedStorageState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// Upper frontiers for sources.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

impl<T> Partitionable<StorageCommand<T>, StorageResponse<T>>
    for (StorageCommand<T>, StorageResponse<T>)
where
    T: timely::progress::Timestamp,
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
                    let previous = self.uppers.insert(ingestion.id, frontier);
                    assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", ingestion.id, command);
                }
            }
            _ => {
                // Other commands have no known impact on frontier tracking.
            }
        }
    }
}

impl<T> PartitionedState<StorageCommand<T>, StorageResponse<T>> for PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn split_command(&mut self, command: StorageCommand<T>) -> Vec<StorageCommand<T>> {
        self.observe_command(&command);

        vec![command; self.parts]
    }

    fn absorb_response(
        &mut self,
        _shard_id: usize,
        response: StorageResponse<T>,
    ) -> Option<Result<StorageResponse<T>, anyhow::Error>> {
        match response {
            // Avoid multiple retractions of minimum time, to present as updates from one worker.
            StorageResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
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
                    Some(Ok(StorageResponse::FrontierUppers(list)))
                }
            }
            // TODO(guswynn): is this the correct implementation?
            StorageResponse::LinearizedTimestamps(feedback) => {
                Some(Ok(StorageResponse::LinearizedTimestamps(feedback)))
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
