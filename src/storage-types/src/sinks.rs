// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to reporting changing collections out of `dataflow`.

use std::fmt::Debug;

use mz_ore::cast::CastFrom;
use mz_persist_client::ShardId;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};
use mz_stash_types::objects::proto;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::PartialOrder;

use crate::connections::{CsrConnection, KafkaConnection};
use crate::controller::CollectionMetadata;

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
    pub envelope: SinkEnvelope,
    pub as_of: SinkAsOf<T>,
    pub status_id: Option<<S as StorageSinkDescFillState>::StatusId>,
    pub from_storage_metadata: <S as StorageSinkDescFillState>::StorageMetadata,
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
            any::<SinkAsOf<mz_repr::Timestamp>>(),
            any::<Option<ShardId>>(),
            any::<CollectionMetadata>(),
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
                )| {
                    StorageSinkDesc {
                        from,
                        from_desc,
                        connection,
                        envelope,
                        as_of,
                        status_id,
                        from_storage_metadata,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkAsOf<T = mz_repr::Timestamp> {
    pub frontier: Antichain<T>,
    pub strict: bool,
}

impl<T: PartialOrder + Clone> SinkAsOf<T> {
    /// Forwards the since frontier of this `SinkAsOf`. If it is already
    /// sufficiently far advanced the downgrade is a no-op.
    pub fn downgrade(&mut self, other_since: &Antichain<T>) {
        if PartialOrder::less_equal(&self.frontier, other_since) {
            // TODO(aljoscha): Should this be meet_assign?
            self.frontier.clone_from(other_since);
            // If we're using the since, never read the snapshot
            self.strict = true;
        }
    }
}

impl Arbitrary for SinkAsOf<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
            any::<bool>(),
        )
            .prop_map(|(frontier, strict)| SinkAsOf {
                frontier: Antichain::from(frontier),
                strict,
            })
            .boxed()
    }
}

impl RustType<ProtoSinkAsOf> for SinkAsOf<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoSinkAsOf {
        ProtoSinkAsOf {
            frontier: Some(self.frontier.into_proto()),
            strict: self.strict,
        }
    }

    fn from_proto(proto: ProtoSinkAsOf) -> Result<Self, TryFromProtoError> {
        Ok(SinkAsOf {
            frontier: proto
                .frontier
                .into_rust_if_some("ProtoSinkAsOf::frontier")?,
            strict: proto.strict,
        })
    }
}

impl RustType<proto::SinkAsOf> for SinkAsOf<mz_repr::Timestamp> {
    fn into_proto(&self) -> proto::SinkAsOf {
        proto::SinkAsOf {
            frontier: Some(self.frontier.into_proto()),
            strict: self.strict,
        }
    }

    fn from_proto(proto: proto::SinkAsOf) -> Result<Self, TryFromProtoError> {
        let frontier = proto.frontier.into_rust_if_some("SinkAsOf::frontier")?;
        Ok(SinkAsOf {
            frontier,
            strict: proto.strict,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageSinkConnection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaSinkConnection<C>),
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
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaConsistencyConfig::kind"))?;

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
pub enum KafkaConsistencyConfig {
    Progress { topic: String },
}

impl RustType<ProtoKafkaConsistencyConfig> for KafkaConsistencyConfig {
    fn into_proto(&self) -> ProtoKafkaConsistencyConfig {
        use proto_kafka_consistency_config::Kind::*;
        use proto_kafka_consistency_config::ProtoKafkaConsistencyConfigProgress;

        ProtoKafkaConsistencyConfig {
            kind: Some(match self {
                Self::Progress { topic } => Progress(ProtoKafkaConsistencyConfigProgress {
                    topic: topic.clone(),
                }),
            }),
        }
    }
    fn from_proto(proto: ProtoKafkaConsistencyConfig) -> Result<Self, TryFromProtoError> {
        use proto_kafka_consistency_config::Kind::*;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaConsistencyConfig::kind"))?;

        Ok(match kind {
            Progress(proto) => Self::Progress { topic: proto.topic },
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
    pub connection: KafkaConnection<C>,
    pub format: KafkaSinkFormat<C>,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub value_desc: RelationDesc,
    pub topic: String,
    pub consistency_config: KafkaConsistencyConfig,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub fuel: usize,
    pub retention: KafkaSinkConnectionRetention,
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
            consistency_config,
            partition_count,
            replication_factor,
            fuel,
            retention,
        } = self;
        KafkaSinkConnection {
            connection_id,
            connection: connection.into_inline_connection(&r),
            format: format.into_inline_connection(r),
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            topic,
            consistency_config,
            partition_count,
            replication_factor,
            fuel,
            retention,
        }
    }
}

impl RustType<ProtoKafkaSinkConnectionV2> for KafkaSinkConnection {
    fn into_proto(&self) -> ProtoKafkaSinkConnectionV2 {
        ProtoKafkaSinkConnectionV2 {
            connection_id: Some(self.connection_id.into_proto()),
            connection: Some(self.connection.into_proto()),
            format: Some(self.format.into_proto()),
            key_desc_and_indices: self.key_desc_and_indices.into_proto(),
            relation_key_indices: self.relation_key_indices.into_proto(),
            value_desc: Some(self.value_desc.into_proto()),
            topic: self.topic.clone(),
            consistency_config: Some(self.consistency_config.into_proto()),
            partition_count: self.partition_count,
            replication_factor: self.replication_factor,
            fuel: u64::cast_from(self.fuel),
            retention: Some(self.retention.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConnectionV2) -> Result<Self, TryFromProtoError> {
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
            consistency_config: proto
                .consistency_config
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::consistency_config")?,
            partition_count: proto.partition_count,
            replication_factor: proto.replication_factor,
            fuel: proto.fuel.into_rust()?,
            retention: proto
                .retention
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::retention")?,
        })
    }
}

#[derive(Arbitrary, Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectionRetention {
    pub duration: Option<i64>,
    pub bytes: Option<i64>,
}

impl RustType<ProtoKafkaSinkConnectionRetention> for KafkaSinkConnectionRetention {
    fn into_proto(&self) -> ProtoKafkaSinkConnectionRetention {
        ProtoKafkaSinkConnectionRetention {
            duration: self.duration,
            bytes: self.bytes,
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConnectionRetention) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSinkConnectionRetention {
            duration: proto.duration,
            bytes: proto.bytes,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkAvroFormatState<C: ConnectionAccess = InlinedConnection> {
    /// If we haven't yet communicated with the CSR, we don't yet know if we
    /// have a schema and value ID. It's possible that this schema was already
    /// published.
    UnpublishedMaybe {
        key_schema: Option<String>,
        value_schema: String,
        csr_connection: CsrConnection<C>,
    },
    /// After communicating with the CSR, the IDs we've been given for the
    /// schemas.
    Published {
        key_schema_id: Option<i32>,
        value_schema_id: i32,
    },
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkAvroFormatState, R>
    for KafkaSinkAvroFormatState<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkAvroFormatState {
        match self {
            Self::UnpublishedMaybe {
                key_schema,
                value_schema,
                csr_connection,
            } => KafkaSinkAvroFormatState::UnpublishedMaybe {
                key_schema,
                value_schema,
                csr_connection: csr_connection.into_inline_connection(r),
            },
            Self::Published {
                key_schema_id,
                value_schema_id,
            } => KafkaSinkAvroFormatState::Published {
                key_schema_id,
                value_schema_id,
            },
        }
    }
}

impl RustType<proto_kafka_sink_format::ProtoKafkaSinkAvroFormatState> for KafkaSinkAvroFormatState {
    fn into_proto(&self) -> proto_kafka_sink_format::ProtoKafkaSinkAvroFormatState {
        use proto_kafka_sink_format::proto_kafka_sink_avro_format_state::{
            Kind, ProtoPublished, ProtoUnpublishedMaybe,
        };
        use proto_kafka_sink_format::ProtoKafkaSinkAvroFormatState;

        ProtoKafkaSinkAvroFormatState {
            kind: Some(match self {
                KafkaSinkAvroFormatState::UnpublishedMaybe {
                    key_schema,
                    value_schema,
                    csr_connection,
                } => Kind::UnpublishedMaybe(ProtoUnpublishedMaybe {
                    key_schema: key_schema.clone(),
                    value_schema: value_schema.clone(),
                    csr_connection: Some(csr_connection.into_proto()),
                }),
                KafkaSinkAvroFormatState::Published {
                    key_schema_id,
                    value_schema_id,
                } => Kind::Published(ProtoPublished {
                    key_schema_id: *key_schema_id,
                    value_schema_id: *value_schema_id,
                }),
            }),
        }
    }

    fn from_proto(
        proto: proto_kafka_sink_format::ProtoKafkaSinkAvroFormatState,
    ) -> Result<Self, TryFromProtoError> {
        use proto_kafka_sink_format::proto_kafka_sink_avro_format_state::Kind;

        let kind = proto.kind.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoKafkaSinkAvroFormatState::kind")
        })?;

        Ok(match kind {
            Kind::UnpublishedMaybe(proto) => Self::UnpublishedMaybe {
                key_schema: proto.key_schema,
                value_schema: proto.value_schema,
                csr_connection: proto
                    .csr_connection
                    .into_rust_if_some("ProtoKafkaSinkAvroFormatState::csr_connection")?,
            },
            Kind::Published(proto) => Self::Published {
                key_schema_id: proto.key_schema_id,
                value_schema_id: proto.value_schema_id,
            },
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormat<C: ConnectionAccess = InlinedConnection> {
    Avro(KafkaSinkAvroFormatState<C>),
    Json,
}

impl KafkaSinkFormat {
    pub fn get_format_name(&self) -> &str {
        match self {
            Self::Avro(_) => "avro",
            Self::Json => "json",
        }
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkFormat, R>
    for KafkaSinkFormat<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkFormat {
        match self {
            Self::Avro(avro) => KafkaSinkFormat::Avro(avro.into_inline_connection(r)),
            Self::Json => KafkaSinkFormat::Json,
        }
    }
}

impl RustType<ProtoKafkaSinkFormat> for KafkaSinkFormat {
    fn into_proto(&self) -> ProtoKafkaSinkFormat {
        use proto_kafka_sink_format::Kind;
        ProtoKafkaSinkFormat {
            kind: Some(match self {
                Self::Avro(avro) => Kind::Avro(avro.into_proto()),
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
            Kind::Avro(avro) => Self::Avro(avro.into_rust()?),
            Kind::Json(()) => Self::Json,
        })
    }
}
