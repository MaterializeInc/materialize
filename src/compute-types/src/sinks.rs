// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflow sinks.

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sinks::S3UploadInfo;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

include!(concat!(env!("OUT_DIR"), "/mz_compute_types.sinks.rs"));

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeSinkDesc<S: 'static = (), T = Timestamp> {
    /// TODO(database-issues#7533): Add documentation.
    pub from: GlobalId,
    /// TODO(database-issues#7533): Add documentation.
    pub from_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub connection: ComputeSinkConnection<S>,
    /// TODO(database-issues#7533): Add documentation.
    pub with_snapshot: bool,
    /// TODO(database-issues#7533): Add documentation.
    pub up_to: Antichain<T>,
    /// TODO(database-issues#7533): Add documentation.
    pub non_null_assertions: Vec<usize>,
    /// TODO(database-issues#7533): Add documentation.
    pub refresh_schedule: Option<RefreshSchedule>,
}

impl Arbitrary for ComputeSinkDesc<CollectionMetadata, Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<ComputeSinkConnection<CollectionMetadata>>(),
            any::<bool>(),
            proptest::collection::vec(any::<Timestamp>(), 1..4),
            proptest::collection::vec(any::<usize>(), 0..4),
            proptest::option::of(any::<RefreshSchedule>()),
        )
            .prop_map(
                |(
                    from,
                    from_desc,
                    connection,
                    with_snapshot,
                    up_to_frontier,
                    non_null_assertions,
                    refresh_schedule,
                )| {
                    ComputeSinkDesc {
                        from,
                        from_desc,
                        connection,
                        with_snapshot,
                        up_to: Antichain::from(up_to_frontier),
                        non_null_assertions,
                        refresh_schedule,
                    }
                },
            )
            .boxed()
    }
}

impl Arbitrary for ComputeSinkDesc<(), Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<ComputeSinkConnection<()>>(),
            any::<bool>(),
            proptest::collection::vec(any::<Timestamp>(), 1..4),
            proptest::collection::vec(any::<usize>(), 0..4),
            proptest::option::of(any::<RefreshSchedule>()),
        )
            .prop_map(
                |(
                    from,
                    from_desc,
                    connection,
                    with_snapshot,
                    up_to_frontier,
                    non_null_assertions,
                    refresh_schedule,
                )| {
                    ComputeSinkDesc {
                        from,
                        from_desc,
                        connection,
                        with_snapshot,
                        up_to: Antichain::from(up_to_frontier),
                        non_null_assertions,
                        refresh_schedule,
                    }
                },
            )
            .boxed()
    }
}

impl RustType<ProtoComputeSinkDesc> for ComputeSinkDesc<CollectionMetadata, Timestamp> {
    fn into_proto(&self) -> ProtoComputeSinkDesc {
        ProtoComputeSinkDesc {
            connection: Some(self.connection.into_proto()),
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            with_snapshot: self.with_snapshot,
            up_to: Some(self.up_to.into_proto()),
            non_null_assertions: self.non_null_assertions.into_proto(),
            refresh_schedule: self.refresh_schedule.into_proto(),
        }
    }

    fn from_proto(proto: ProtoComputeSinkDesc) -> Result<Self, TryFromProtoError> {
        Ok(ComputeSinkDesc {
            from: proto.from.into_rust_if_some("ProtoComputeSinkDesc::from")?,
            from_desc: proto
                .from_desc
                .into_rust_if_some("ProtoComputeSinkDesc::from_desc")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoComputeSinkDesc::connection")?,
            with_snapshot: proto.with_snapshot,
            up_to: proto
                .up_to
                .into_rust_if_some("ProtoComputeSinkDesc::up_to")?,
            non_null_assertions: proto.non_null_assertions.into_rust()?,
            refresh_schedule: proto.refresh_schedule.into_rust()?,
        })
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ComputeSinkConnection<S: 'static = ()> {
    /// TODO(database-issues#7533): Add documentation.
    Subscribe(SubscribeSinkConnection),
    /// TODO(database-issues#7533): Add documentation.
    MaterializedView(MaterializedViewSinkConnection<S>),
    /// ContinualTask-specific information necessary for rendering a
    /// ContinualTask sink.
    ContinualTask(ContinualTaskConnection<S>),
    /// A compute sink to do a oneshot copy to s3.
    CopyToS3Oneshot(CopyToS3OneshotSinkConnection),
}

impl<S> ComputeSinkConnection<S> {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            ComputeSinkConnection::Subscribe(_) => "subscribe",
            ComputeSinkConnection::MaterializedView(_) => "materialized_view",
            ComputeSinkConnection::ContinualTask(_) => "continual_task",
            ComputeSinkConnection::CopyToS3Oneshot(_) => "copy_to_s3_oneshot",
        }
    }

    /// True if the sink is a subscribe, which is differently recoverable than other sinks.
    pub fn is_subscribe(&self) -> bool {
        if let ComputeSinkConnection::Subscribe(_) = self {
            true
        } else {
            false
        }
    }
}

impl RustType<ProtoComputeSinkConnection> for ComputeSinkConnection<CollectionMetadata> {
    fn into_proto(&self) -> ProtoComputeSinkConnection {
        use proto_compute_sink_connection::Kind;
        ProtoComputeSinkConnection {
            kind: Some(match self {
                ComputeSinkConnection::Subscribe(_) => Kind::Subscribe(()),
                ComputeSinkConnection::MaterializedView(materialized_view) => {
                    Kind::MaterializedView(materialized_view.into_proto())
                }
                ComputeSinkConnection::ContinualTask(continual_task) => {
                    Kind::ContinualTask(continual_task.into_proto())
                }
                ComputeSinkConnection::CopyToS3Oneshot(s3) => {
                    Kind::CopyToS3Oneshot(s3.into_proto())
                }
            }),
        }
    }

    fn from_proto(proto: ProtoComputeSinkConnection) -> Result<Self, TryFromProtoError> {
        use proto_compute_sink_connection::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoComputeSinkConnection::kind"))?;
        Ok(match kind {
            Kind::Subscribe(_) => ComputeSinkConnection::Subscribe(SubscribeSinkConnection {}),
            Kind::MaterializedView(materialized_view) => {
                ComputeSinkConnection::MaterializedView(materialized_view.into_rust()?)
            }
            Kind::ContinualTask(continual_task) => {
                ComputeSinkConnection::ContinualTask(continual_task.into_rust()?)
            }
            Kind::CopyToS3Oneshot(s3) => ComputeSinkConnection::CopyToS3Oneshot(s3.into_rust()?),
        })
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Arbitrary, Default, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SubscribeSinkConnection {}

/// Connection attributes required to do a oneshot copy to s3.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CopyToS3OneshotSinkConnection {
    /// Information specific to the upload.
    pub upload_info: S3UploadInfo,
    /// The AWS connection information to do the writes.
    pub aws_connection: AwsConnection,
    /// The ID of the Connection object, used to generate the External ID when
    /// using AssumeRole with AWS connection.
    pub connection_id: CatalogItemId,
    /// The number of batches the COPY TO output will be divided into
    /// where each worker will process 0 or more batches of data.
    pub output_batch_count: u64,
}

impl RustType<ProtoCopyToS3OneshotSinkConnection> for CopyToS3OneshotSinkConnection {
    fn into_proto(&self) -> ProtoCopyToS3OneshotSinkConnection {
        ProtoCopyToS3OneshotSinkConnection {
            upload_info: Some(self.upload_info.into_proto()),
            aws_connection: Some(self.aws_connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            output_batch_count: self.output_batch_count,
        }
    }

    fn from_proto(proto: ProtoCopyToS3OneshotSinkConnection) -> Result<Self, TryFromProtoError> {
        Ok(CopyToS3OneshotSinkConnection {
            upload_info: proto
                .upload_info
                .into_rust_if_some("ProtoCopyToS3OneshotSinkConnection::upload_info")?,
            aws_connection: proto
                .aws_connection
                .into_rust_if_some("ProtoCopyToS3OneshotSinkConnection::aws_connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoCopyToS3OneshotSinkConnection::connection_id")?,
            output_batch_count: proto.output_batch_count,
        })
    }
}

/// TODO(database-issues#7533): Add documentation.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializedViewSinkConnection<S> {
    /// TODO(database-issues#7533): Add documentation.
    pub value_desc: RelationDesc,
    /// TODO(database-issues#7533): Add documentation.
    pub storage_metadata: S,
}

impl RustType<ProtoMaterializedViewSinkConnection>
    for MaterializedViewSinkConnection<CollectionMetadata>
{
    fn into_proto(&self) -> ProtoMaterializedViewSinkConnection {
        ProtoMaterializedViewSinkConnection {
            value_desc: Some(self.value_desc.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMaterializedViewSinkConnection) -> Result<Self, TryFromProtoError> {
        Ok(MaterializedViewSinkConnection {
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoMaterializedViewSinkConnection::value_desc")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoMaterializedViewSinkConnection::storage_metadata")?,
        })
    }
}

/// ContinualTask-specific information necessary for rendering a ContinualTask
/// sink. (Shared-sink information is instead stored on ComputeSinkConnection.)
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContinualTaskConnection<S> {
    /// The id of the (for now) single input to this CT.
    //
    // TODO(ct3): This can be removed once we render the "input" sources without
    // the hack.
    pub input_id: GlobalId,
    /// The necessary storage information for writing to the output collection.
    pub storage_metadata: S,
}

impl RustType<ProtoContinualTaskConnection> for ContinualTaskConnection<CollectionMetadata> {
    fn into_proto(&self) -> ProtoContinualTaskConnection {
        ProtoContinualTaskConnection {
            input_id: Some(self.input_id.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
        }
    }

    fn from_proto(proto: ProtoContinualTaskConnection) -> Result<Self, TryFromProtoError> {
        Ok(ContinualTaskConnection {
            input_id: proto
                .input_id
                .into_rust_if_some("ProtoContinualTaskConnection::input_id")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoContinualTaskConnection::output_metadata")?,
        })
    }
}
