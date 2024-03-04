// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to reporting changing collections out of `dataflow`.

use std::borrow::Cow;
use std::fmt::Debug;

use mz_persist_client::ShardId;
use mz_pgcopy::CopyFormatParams;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::PartialOrder;

use crate::connections::ConnectionContext;
use crate::controller::{CollectionMetadata, StorageError};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.sinks.rs"));

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageSinkDesc<S: StorageSinkDescFillState, T = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: StorageSinkConnection,
    pub with_snapshot: bool,
    pub envelope: SinkEnvelope,
    pub as_of: Antichain<T>,
    pub status_id: Option<<S as StorageSinkDescFillState>::StatusId>,
    pub from_storage_metadata: <S as StorageSinkDescFillState>::StorageMetadata,
}

impl<S: Debug + StorageSinkDescFillState + PartialEq, T: Debug + PartialEq + PartialOrder>
    crate::AlterCompatible for StorageSinkDesc<S, T>
{
    /// Determines if `self` is compatible with another `StorageSinkDesc`, in
    /// such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations.
    ///
    /// Currently, the only "valid transformation" is the passage of time such
    /// that the sink's as ofs may differ. However, this will change once we
    /// support `ALTER CONNECTION` or `ALTER SINK`.
    fn alter_compatible(
        &self,
        id: GlobalId,
        other: &StorageSinkDesc<S, T>,
    ) -> Result<(), StorageError> {
        if self == other {
            return Ok(());
        }
        let StorageSinkDesc {
            from,
            from_desc,
            connection,
            envelope,
            // The as of of the descriptions may differ.
            as_of: _,
            status_id,
            from_storage_metadata,
            with_snapshot,
        } = self;

        let compatibility_checks = [
            (from == &other.from, "from"),
            (from_desc == &other.from_desc, "from_desc"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (envelope == &other.envelope, "envelope"),
            (status_id == &other.status_id, "status_id"),
            (with_snapshot == &other.with_snapshot, "with_snapshot"),
            (
                from_storage_metadata == &other.from_storage_metadata,
                "from_storage_metadata",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "StorageSinkDesc incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(StorageError::InvalidAlter { id });
            }
        }

        Ok(())
    }
}

pub trait StorageSinkDescFillState {
    type StatusId: Debug + Clone + Serialize + for<'a> Deserialize<'a> + Eq + PartialEq;
    type StorageMetadata: Debug + Clone + Serialize + for<'a> Deserialize<'a> + Eq + PartialEq;
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MetadataUnfilled;
impl StorageSinkDescFillState for MetadataUnfilled {
    type StatusId = GlobalId;
    type StorageMetadata = ();
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MetadataFilled;
impl StorageSinkDescFillState for MetadataFilled {
    type StatusId = ShardId;
    type StorageMetadata = CollectionMetadata;
}

impl Arbitrary for StorageSinkDesc<MetadataFilled, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<StorageSinkConnection>(),
            any::<SinkEnvelope>(),
            any::<Option<mz_repr::Timestamp>>(),
            any::<Option<ShardId>>(),
            any::<CollectionMetadata>(),
            any::<bool>(),
        )
            .prop_map(
                |(
                    from,
                    from_desc,
                    connection,
                    envelope,
                    as_of,
                    status_id,
                    from_storage_metadata,
                    with_snapshot,
                )| {
                    StorageSinkDesc {
                        from,
                        from_desc,
                        connection,
                        envelope,
                        as_of: Antichain::from_iter(as_of),
                        status_id,
                        from_storage_metadata,
                        with_snapshot,
                    }
                },
            )
            .boxed()
    }
}

impl RustType<ProtoStorageSinkDesc> for StorageSinkDesc<MetadataFilled, mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageSinkDesc {
        ProtoStorageSinkDesc {
            connection: Some(self.connection.into_proto()),
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            envelope: Some(self.envelope.into_proto()),
            as_of: Some(self.as_of.into_proto()),
            status_id: self.status_id.into_proto(),
            from_storage_metadata: Some(self.from_storage_metadata.into_proto()),
            with_snapshot: self.with_snapshot,
        }
    }

    fn from_proto(proto: ProtoStorageSinkDesc) -> Result<Self, TryFromProtoError> {
        Ok(StorageSinkDesc {
            from: proto.from.into_rust_if_some("ProtoStorageSinkDesc::from")?,
            from_desc: proto
                .from_desc
                .into_rust_if_some("ProtoStorageSinkDesc::from_desc")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoStorageSinkDesc::connection")?,
            envelope: proto
                .envelope
                .into_rust_if_some("ProtoStorageSinkDesc::envelope")?,
            as_of: proto
                .as_of
                .into_rust_if_some("ProtoStorageSinkDesc::as_of")?,
            status_id: proto.status_id.into_rust()?,
            from_storage_metadata: proto
                .from_storage_metadata
                .into_rust_if_some("ProtoStorageSinkDesc::from_storage_metadata")?,
            with_snapshot: proto.with_snapshot,
        })
    }
}

#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkEnvelope {
    Debezium,
    Upsert,
}

impl RustType<ProtoSinkEnvelope> for SinkEnvelope {
    fn into_proto(&self) -> ProtoSinkEnvelope {
        use proto_sink_envelope::Kind;
        ProtoSinkEnvelope {
            kind: Some(match self {
                SinkEnvelope::Debezium => Kind::Debezium(()),
                SinkEnvelope::Upsert => Kind::Upsert(()),
            }),
        }
    }

    fn from_proto(proto: ProtoSinkEnvelope) -> Result<Self, TryFromProtoError> {
        use proto_sink_envelope::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSinkEnvelope::kind"))?;
        Ok(match kind {
            Kind::Debezium(()) => SinkEnvelope::Debezium,
            Kind::Upsert(()) => SinkEnvelope::Upsert,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageSinkConnection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaSinkConnection<C>),
}

impl<C: ConnectionAccess> StorageSinkConnection<C> {
    /// Determines if `self` is compatible with another `StorageSinkConnection`,
    /// in such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations (e.g. no transformation or `ALTER
    /// CONNECTION`).
    pub fn alter_compatible(
        &self,
        id: GlobalId,
        other: &StorageSinkConnection<C>,
    ) -> Result<(), StorageError> {
        if self == other {
            return Ok(());
        }
        match (self, other) {
            (StorageSinkConnection::Kafka(s), StorageSinkConnection::Kafka(o)) => {
                s.alter_compatible(id, o)?
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<StorageSinkConnection, R>
    for StorageSinkConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> StorageSinkConnection {
        match self {
            Self::Kafka(conn) => StorageSinkConnection::Kafka(conn.into_inline_connection(r)),
        }
    }
}

impl RustType<ProtoStorageSinkConnection> for StorageSinkConnection {
    fn into_proto(&self) -> ProtoStorageSinkConnection {
        use proto_storage_sink_connection::Kind::*;

        ProtoStorageSinkConnection {
            kind: Some(match self {
                Self::Kafka(conn) => KafkaV2(conn.into_proto()),
            }),
        }
    }
    fn from_proto(proto: ProtoStorageSinkConnection) -> Result<Self, TryFromProtoError> {
        use proto_storage_sink_connection::Kind::*;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoStorageSinkConnection::kind"))?;

        Ok(match kind {
            KafkaV2(proto) => Self::Kafka(proto.into_rust()?),
        })
    }
}

impl<C: ConnectionAccess> StorageSinkConnection<C> {
    /// returns an option to not constrain ourselves in the future
    pub fn connection_id(&self) -> Option<GlobalId> {
        use StorageSinkConnection::*;
        match self {
            Kafka(KafkaSinkConnection { connection_id, .. }) => Some(*connection_id),
        }
    }

    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        use StorageSinkConnection::*;
        match self {
            Kafka(_) => "kafka",
        }
    }
}

impl RustType<proto_kafka_sink_connection_v2::ProtoKeyDescAndIndices>
    for (RelationDesc, Vec<usize>)
{
    fn into_proto(&self) -> proto_kafka_sink_connection_v2::ProtoKeyDescAndIndices {
        proto_kafka_sink_connection_v2::ProtoKeyDescAndIndices {
            desc: Some(self.0.into_proto()),
            indices: self.1.into_proto(),
        }
    }

    fn from_proto(
        proto: proto_kafka_sink_connection_v2::ProtoKeyDescAndIndices,
    ) -> Result<Self, TryFromProtoError> {
        Ok((
            proto
                .desc
                .into_rust_if_some("ProtoKeyDescAndIndices::desc")?,
            proto.indices.into_rust()?,
        ))
    }
}

impl RustType<proto_kafka_sink_connection_v2::ProtoRelationKeyIndicesVec> for Vec<usize> {
    fn into_proto(&self) -> proto_kafka_sink_connection_v2::ProtoRelationKeyIndicesVec {
        proto_kafka_sink_connection_v2::ProtoRelationKeyIndicesVec {
            relation_key_indices: self.into_proto(),
        }
    }

    fn from_proto(
        proto: proto_kafka_sink_connection_v2::ProtoRelationKeyIndicesVec,
    ) -> Result<Self, TryFromProtoError> {
        proto.relation_key_indices.into_rust()
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkCompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl KafkaSinkCompressionType {
    /// Format the compression type as expected by `compression.type` librdkafka
    /// setting.
    pub fn to_librdkafka_option(&self) -> &'static str {
        match self {
            KafkaSinkCompressionType::None => "none",
            KafkaSinkCompressionType::Gzip => "gzip",
            KafkaSinkCompressionType::Snappy => "snappy",
            KafkaSinkCompressionType::Lz4 => "lz4",
            KafkaSinkCompressionType::Zstd => "zstd",
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
    pub connection: C::Kafka,
    pub format: KafkaSinkFormat<C>,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub value_desc: RelationDesc,
    pub topic: String,
    pub compression_type: KafkaSinkCompressionType,
    pub progress_group_id: KafkaIdStyle,
    pub transactional_id: KafkaIdStyle,
}

impl KafkaSinkConnection {
    /// Returns the client ID to register with librdkafka with.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn client_id(&self, connection_context: &ConnectionContext, sink_id: GlobalId) -> String {
        format!(
            "materialize-{}-{}-{}",
            connection_context.environment_id, self.connection_id, sink_id,
        )
    }

    /// Returns the name of the progress topic to use for the sink.
    pub fn progress_topic(&self, connection_context: &ConnectionContext) -> Cow<str> {
        self.connection
            .progress_topic(connection_context, self.connection_id)
    }

    /// Returns the ID for the consumer group the sink will use to read the
    /// progress topic on resumption.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn progress_group_id(
        &self,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        match self.progress_group_id {
            KafkaIdStyle::Prefix(ref prefix) => format!(
                "{}{}",
                prefix.as_deref().unwrap_or(""),
                self.client_id(connection_context, sink_id)
            ),
            KafkaIdStyle::Legacy => format!("materialize-bootstrap-sink-{sink_id}"),
        }
    }

    /// Returns the transactional ID to use for the sink.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn transactional_id(
        &self,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        match self.transactional_id {
            KafkaIdStyle::Prefix(ref prefix) => format!(
                "{}{}",
                prefix.as_deref().unwrap_or(""),
                self.client_id(connection_context, sink_id)
            ),
            KafkaIdStyle::Legacy => format!("mz-producer-{sink_id}-0"),
        }
    }
}

impl<C: ConnectionAccess> KafkaSinkConnection<C> {
    /// Determines if `self` is compatible with another `StorageSinkConnection`,
    /// in such a way that it is possible to turn `self` into `other` through a
    /// valid series of transformations (e.g. no transformation or `ALTER
    /// CONNECTION`).
    pub fn alter_compatible(
        &self,
        id: GlobalId,
        other: &KafkaSinkConnection<C>,
    ) -> Result<(), StorageError> {
        if self == other {
            return Ok(());
        }
        let KafkaSinkConnection {
            connection_id,
            // The details of the Kafka connection itself may change
            connection: _,
            format,
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (format.alter_compatible(id, &other.format).is_ok(), "format"),
            (
                relation_key_indices == &other.relation_key_indices,
                "relation_key_indices",
            ),
            (
                key_desc_and_indices == &other.key_desc_and_indices,
                "key_desc_and_indices",
            ),
            (value_desc == &other.value_desc, "value_desc"),
            (topic == &other.topic, "topic"),
            (
                compression_type == &other.compression_type,
                "compression_type",
            ),
            (
                progress_group_id == &other.progress_group_id,
                "progress_group_id",
            ),
            (
                transactional_id == &other.transactional_id,
                "transactional_id",
            ),
        ];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "KafkaSinkConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(StorageError::InvalidAlter { id });
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkConnection, R>
    for KafkaSinkConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkConnection {
        let KafkaSinkConnection {
            connection_id,
            connection,
            format,
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
        } = self;
        KafkaSinkConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_kafka(),
            format: format.into_inline_connection(r),
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaIdStyle {
    /// A new-style id that is optionally prefixed.
    Prefix(Option<String>),
    /// A legacy style id.
    Legacy,
}

impl RustType<ProtoKafkaIdStyle> for KafkaIdStyle {
    fn into_proto(&self) -> ProtoKafkaIdStyle {
        use crate::sinks::proto_kafka_id_style::Kind::*;
        use crate::sinks::proto_kafka_id_style::ProtoKafkaIdStylePrefix;

        ProtoKafkaIdStyle {
            kind: Some(match self {
                Self::Prefix(prefix) => Prefix(ProtoKafkaIdStylePrefix {
                    prefix: prefix.into_proto(),
                }),
                Self::Legacy => Legacy(()),
            }),
        }
    }
    fn from_proto(proto: ProtoKafkaIdStyle) -> Result<Self, TryFromProtoError> {
        use crate::sinks::proto_kafka_id_style::Kind::*;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaIdStyle::kind"))?;

        Ok(match kind {
            Prefix(prefix) => Self::Prefix(prefix.prefix.into_rust()?),
            Legacy(()) => Self::Legacy,
        })
    }
}

impl RustType<ProtoKafkaSinkConnectionV2> for KafkaSinkConnection {
    fn into_proto(&self) -> ProtoKafkaSinkConnectionV2 {
        use crate::sinks::proto_kafka_sink_connection_v2::CompressionType;
        ProtoKafkaSinkConnectionV2 {
            connection_id: Some(self.connection_id.into_proto()),
            connection: Some(self.connection.into_proto()),
            format: Some(self.format.into_proto()),
            key_desc_and_indices: self.key_desc_and_indices.into_proto(),
            relation_key_indices: self.relation_key_indices.into_proto(),
            value_desc: Some(self.value_desc.into_proto()),
            topic: self.topic.clone(),
            compression_type: Some(match self.compression_type {
                KafkaSinkCompressionType::None => CompressionType::None(()),
                KafkaSinkCompressionType::Gzip => CompressionType::Gzip(()),
                KafkaSinkCompressionType::Snappy => CompressionType::Snappy(()),
                KafkaSinkCompressionType::Lz4 => CompressionType::Lz4(()),
                KafkaSinkCompressionType::Zstd => CompressionType::Zstd(()),
            }),
            progress_group_id: Some(self.progress_group_id.into_proto()),
            transactional_id: Some(self.transactional_id.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConnectionV2) -> Result<Self, TryFromProtoError> {
        use crate::sinks::proto_kafka_sink_connection_v2::CompressionType;
        Ok(KafkaSinkConnection {
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::connection_id")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::connection")?,
            format: proto
                .format
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::format")?,
            key_desc_and_indices: proto.key_desc_and_indices.into_rust()?,
            relation_key_indices: proto.relation_key_indices.into_rust()?,
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::value_desc")?,
            topic: proto.topic,
            compression_type: match proto.compression_type {
                Some(CompressionType::None(())) => KafkaSinkCompressionType::None,
                Some(CompressionType::Gzip(())) => KafkaSinkCompressionType::Gzip,
                Some(CompressionType::Snappy(())) => KafkaSinkCompressionType::Snappy,
                Some(CompressionType::Lz4(())) => KafkaSinkCompressionType::Lz4,
                Some(CompressionType::Zstd(())) => KafkaSinkCompressionType::Zstd,
                None => {
                    return Err(TryFromProtoError::missing_field(
                        "ProtoKafkaSinkConnectionV2::compression_type",
                    ))
                }
            },
            progress_group_id: proto
                .progress_group_id
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::progress_group_id")?,
            transactional_id: proto
                .transactional_id
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::transactional_id")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormat<C: ConnectionAccess = InlinedConnection> {
    Avro {
        key_schema: Option<String>,
        value_schema: String,
        csr_connection: C::Csr,
    },
    Json,
}

impl<C: ConnectionAccess> KafkaSinkFormat<C> {
    pub fn get_format_name(&self) -> &str {
        match self {
            Self::Avro { .. } => "avro",
            Self::Json => "json",
        }
    }

    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), StorageError> {
        if self == other {
            return Ok(());
        }

        match (self, other) {
            (
                Self::Avro {
                    key_schema,
                    value_schema,
                    // Connections may change
                    csr_connection: _,
                },
                Self::Avro {
                    key_schema: other_key_schema,
                    value_schema: other_value_schema,
                    csr_connection: _,
                },
            ) => {
                let compatibility_checks = [
                    (key_schema == other_key_schema, "key_schema"),
                    (value_schema == other_value_schema, "value_schema"),
                ];
                for (compatible, field) in compatibility_checks {
                    if !compatible {
                        tracing::warn!(
                            "KafkaSinkAvroFormatState::Avro incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                            self,
                            other
                        );

                        return Err(StorageError::InvalidAlter { id });
                    }
                }
            }
            (s, o) => {
                if s != o {
                    tracing::warn!(
                        "KafkaSinkFormat incompatible\nself:\n{:#?}\n\nother:{:#?}",
                        s,
                        o
                    );
                    return Err(StorageError::InvalidAlter { id });
                }
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkFormat, R>
    for KafkaSinkFormat<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkFormat {
        match self {
            Self::Avro {
                key_schema,
                value_schema,
                csr_connection,
            } => KafkaSinkFormat::Avro {
                key_schema,
                value_schema,
                csr_connection: r.resolve_connection(csr_connection).unwrap_csr(),
            },
            Self::Json => KafkaSinkFormat::Json,
        }
    }
}

impl RustType<ProtoKafkaSinkFormat> for KafkaSinkFormat {
    fn into_proto(&self) -> ProtoKafkaSinkFormat {
        use proto_kafka_sink_format::Kind;
        ProtoKafkaSinkFormat {
            kind: Some(match self {
                Self::Avro {
                    key_schema,
                    value_schema,
                    csr_connection,
                } => Kind::Avro(proto_kafka_sink_format::ProtoKafkaSinkAvroFormat {
                    key_schema: key_schema.clone(),
                    value_schema: value_schema.clone(),
                    csr_connection: Some(csr_connection.into_proto()),
                }),
                Self::Json => Kind::Json(()),
            }),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkFormat) -> Result<Self, TryFromProtoError> {
        use proto_kafka_sink_format::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaSinkFormat::kind"))?;

        Ok(match kind {
            Kind::Avro(proto) => Self::Avro {
                key_schema: proto.key_schema,
                value_schema: proto.value_schema,
                csr_connection: proto
                    .csr_connection
                    .into_rust_if_some("ProtoKafkaSinkAvroFormat::csr_connection")?,
            },
            Kind::Json(()) => Self::Json,
        })
    }
}

/// Info required to copy the data to s3.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct S3UploadInfo {
    /// The s3 prefix path to write the data to.
    pub prefix: String,
    /// The max file size of each file uploaded to S3.
    pub max_file_size: u64,
    /// The relation desc of the data to be uploaded to S3.
    pub desc: RelationDesc,
    /// The selected sink format.
    pub format: CopyFormatParams<'static>,
}

impl RustType<ProtoS3UploadInfo> for S3UploadInfo {
    fn into_proto(&self) -> ProtoS3UploadInfo {
        ProtoS3UploadInfo {
            prefix: self.prefix.clone(),
            max_file_size: self.max_file_size,
            desc: Some(self.desc.into_proto()),
            format: Some(self.format.into_proto()),
        }
    }

    fn from_proto(proto: ProtoS3UploadInfo) -> Result<Self, TryFromProtoError> {
        Ok(S3UploadInfo {
            prefix: proto.prefix,
            max_file_size: proto.max_file_size,
            desc: proto.desc.into_rust_if_some("ProtoS3UploadInfo::desc")?,
            format: proto
                .format
                .into_rust_if_some("ProtoS3UploadInfo::format")?,
        })
    }
}
