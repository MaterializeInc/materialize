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
use std::time::Duration;

use mz_dyncfg::ConfigSet;
use mz_expr::MirScalarExpr;
use mz_pgcopy::CopyFormatParams;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::bytes::ByteSize;
use mz_repr::{CatalogItemId, GlobalId, RelationDesc};
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy, any};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::progress::frontier::Antichain;

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::connections::{ConnectionContext, KafkaConnection, KafkaTopicOptions};
use crate::controller::{AlterError, CollectionMetadata};

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.sinks.rs"));

pub mod s3_oneshot_sink;

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageSinkDesc<S, T = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: StorageSinkConnection,
    pub with_snapshot: bool,
    pub version: u64,
    pub envelope: SinkEnvelope,
    pub as_of: Antichain<T>,
    pub from_storage_metadata: S,
    pub to_storage_metadata: S,
}

impl<S: Debug + PartialEq, T: Debug + PartialEq + PartialOrder> AlterCompatible
    for StorageSinkDesc<S, T>
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
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let StorageSinkDesc {
            from,
            from_desc,
            connection,
            envelope,
            version: _,
            // The as-of of the descriptions may differ.
            as_of: _,
            from_storage_metadata,
            with_snapshot,
            to_storage_metadata,
        } = self;

        let compatibility_checks = [
            (from == &other.from, "from"),
            (from_desc == &other.from_desc, "from_desc"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (envelope == &other.envelope, "envelope"),
            // This can legally change from true to false once the snapshot has been
            // written out.
            (*with_snapshot || !other.with_snapshot, "with_snapshot"),
            (
                from_storage_metadata == &other.from_storage_metadata,
                "from_storage_metadata",
            ),
            (
                to_storage_metadata == &other.to_storage_metadata,
                "to_storage_metadata",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "StorageSinkDesc incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl Arbitrary for StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<StorageSinkConnection>(),
            any::<SinkEnvelope>(),
            any::<Option<mz_repr::Timestamp>>(),
            any::<CollectionMetadata>(),
            any::<bool>(),
            any::<u64>(),
            any::<CollectionMetadata>(),
        )
            .prop_map(
                |(
                    from,
                    from_desc,
                    connection,
                    envelope,
                    as_of,
                    from_storage_metadata,
                    with_snapshot,
                    version,
                    to_storage_metadata,
                )| {
                    StorageSinkDesc {
                        from,
                        from_desc,
                        connection,
                        envelope,
                        version,
                        as_of: Antichain::from_iter(as_of),
                        from_storage_metadata,
                        with_snapshot,
                        to_storage_metadata,
                    }
                },
            )
            .prop_filter("identical source and sink", |desc| {
                desc.from_storage_metadata != desc.to_storage_metadata
            })
            .boxed()
    }
}

impl RustType<ProtoStorageSinkDesc> for StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageSinkDesc {
        ProtoStorageSinkDesc {
            connection: Some(self.connection.into_proto()),
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            envelope: Some(self.envelope.into_proto()),
            as_of: Some(self.as_of.into_proto()),
            from_storage_metadata: Some(self.from_storage_metadata.into_proto()),
            to_storage_metadata: Some(self.to_storage_metadata.into_proto()),
            with_snapshot: self.with_snapshot,
            version: self.version,
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
            from_storage_metadata: proto
                .from_storage_metadata
                .into_rust_if_some("ProtoStorageSinkDesc::from_storage_metadata")?,
            with_snapshot: proto.with_snapshot,
            version: proto.version,
            to_storage_metadata: proto
                .to_storage_metadata
                .into_rust_if_some("ProtoStorageSinkDesc::to_storage_metadata")?,
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
    ) -> Result<(), AlterError> {
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
    pub fn connection_id(&self) -> Option<CatalogItemId> {
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
    pub connection_id: CatalogItemId,
    pub connection: C::Kafka,
    pub format: KafkaSinkFormat<C>,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    /// The index of the column containing message headers value, if any.
    pub headers_index: Option<usize>,
    pub value_desc: RelationDesc,
    /// An expression that, if present, computes a hash value that should be
    /// used to determine the partition for each message.
    pub partition_by: Option<MirScalarExpr>,
    pub topic: String,
    /// Options to use when creating the topic if it doesn't already exist.
    pub topic_options: KafkaTopicOptions,
    pub compression_type: KafkaSinkCompressionType,
    pub progress_group_id: KafkaIdStyle,
    pub transactional_id: KafkaIdStyle,
    pub topic_metadata_refresh_interval: Duration,
}

impl KafkaSinkConnection {
    /// Returns the client ID to register with librdkafka with.
    ///
    /// The caller is responsible for providing the sink ID as it is not known
    /// to `KafkaSinkConnection`.
    pub fn client_id(
        &self,
        configs: &ConfigSet,
        connection_context: &ConnectionContext,
        sink_id: GlobalId,
    ) -> String {
        let mut client_id =
            KafkaConnection::id_base(connection_context, self.connection_id, sink_id);
        self.connection.enrich_client_id(configs, &mut client_id);
        client_id
    }

    /// Returns the name of the progress topic to use for the sink.
    pub fn progress_topic(&self, connection_context: &ConnectionContext) -> Cow<'_, str> {
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
                KafkaConnection::id_base(connection_context, self.connection_id, sink_id),
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
                KafkaConnection::id_base(connection_context, self.connection_id, sink_id)
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
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let KafkaSinkConnection {
            connection_id,
            connection,
            format,
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (format.alter_compatible(id, &other.format).is_ok(), "format"),
            (
                relation_key_indices == &other.relation_key_indices,
                "relation_key_indices",
            ),
            (
                key_desc_and_indices == &other.key_desc_and_indices,
                "key_desc_and_indices",
            ),
            (headers_index == &other.headers_index, "headers_index"),
            (value_desc == &other.value_desc, "value_desc"),
            (partition_by == &other.partition_by, "partition_by"),
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
            (topic_options == &other.topic_options, "topic_config"),
            (
                topic_metadata_refresh_interval == &other.topic_metadata_refresh_interval,
                "topic_metadata_refresh_interval",
            ),
        ];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "KafkaSinkConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
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
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
        } = self;
        KafkaSinkConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_kafka(),
            format: format.into_inline_connection(r),
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            value_desc,
            partition_by,
            topic,
            compression_type,
            progress_group_id,
            transactional_id,
            topic_options,
            topic_metadata_refresh_interval,
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
            headers_index: self.headers_index.into_proto(),
            value_desc: Some(self.value_desc.into_proto()),
            partition_by: self.partition_by.into_proto(),
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
            topic_options: Some(self.topic_options.into_proto()),
            topic_metadata_refresh_interval: Some(
                self.topic_metadata_refresh_interval.into_proto(),
            ),
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
            headers_index: proto.headers_index.into_rust()?,
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::value_desc")?,
            partition_by: proto.partition_by.into_rust()?,
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
                    ));
                }
            },
            progress_group_id: proto
                .progress_group_id
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::progress_group_id")?,
            transactional_id: proto
                .transactional_id
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::transactional_id")?,
            topic_options: match proto.topic_options {
                Some(topic_options) => topic_options.into_rust()?,
                None => Default::default(),
            },
            topic_metadata_refresh_interval: proto
                .topic_metadata_refresh_interval
                .into_rust_if_some("ProtoKafkaSinkConnectionV2::topic_metadata_refresh_interval")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkFormat<C: ConnectionAccess = InlinedConnection> {
    pub key_format: Option<KafkaSinkFormatType<C>>,
    pub value_format: KafkaSinkFormatType<C>,
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormatType<C: ConnectionAccess = InlinedConnection> {
    Avro {
        schema: String,
        compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
        csr_connection: C::Csr,
    },
    Json,
    Text,
    Bytes,
}

impl<C: ConnectionAccess> KafkaSinkFormatType<C> {
    pub fn get_format_name(&self) -> &str {
        match self {
            Self::Avro { .. } => "avro",
            Self::Json => "json",
            Self::Text => "text",
            Self::Bytes => "bytes",
        }
    }
}

impl<C: ConnectionAccess> KafkaSinkFormat<C> {
    pub fn get_format_name<'a>(&'a self) -> Cow<'a, str> {
        // For legacy reasons, if the key-format is none or the key & value formats are
        // both the same (either avro or json), we return the value format name,
        // otherwise we return a composite name.
        match &self.key_format {
            None => self.value_format.get_format_name().into(),
            Some(key_format) => match (key_format, &self.value_format) {
                (KafkaSinkFormatType::Avro { .. }, KafkaSinkFormatType::Avro { .. }) => {
                    "avro".into()
                }
                (KafkaSinkFormatType::Json, KafkaSinkFormatType::Json) => "json".into(),
                (keyf, valuef) => format!(
                    "key-{}-value-{}",
                    keyf.get_format_name(),
                    valuef.get_format_name()
                )
                .into(),
            },
        }
    }

    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        match (&self.value_format, &other.value_format) {
            (
                KafkaSinkFormatType::Avro {
                    schema,
                    compatibility_level: _,
                    csr_connection,
                },
                KafkaSinkFormatType::Avro {
                    schema: other_schema,
                    compatibility_level: _,
                    csr_connection: other_csr_connection,
                },
            ) => {
                if schema != other_schema
                    || csr_connection
                        .alter_compatible(id, other_csr_connection)
                        .is_err()
                {
                    tracing::warn!(
                        "KafkaSinkFormat::Avro incompatible at value_format:\nself:\n{:#?}\n\nother\n{:#?}",
                        self,
                        other
                    );

                    return Err(AlterError { id });
                }
            }
            (s, o) => {
                if s != o {
                    tracing::warn!(
                        "KafkaSinkFormat incompatible at value_format:\nself:\n{:#?}\n\nother:{:#?}",
                        s,
                        o
                    );
                    return Err(AlterError { id });
                }
            }
        }

        match (&self.key_format, &other.key_format) {
            (
                Some(KafkaSinkFormatType::Avro {
                    schema,
                    compatibility_level: _,
                    csr_connection,
                }),
                Some(KafkaSinkFormatType::Avro {
                    schema: other_schema,
                    compatibility_level: _,
                    csr_connection: other_csr_connection,
                }),
            ) => {
                if schema != other_schema
                    || csr_connection
                        .alter_compatible(id, other_csr_connection)
                        .is_err()
                {
                    tracing::warn!(
                        "KafkaSinkFormat::Avro incompatible at key_format:\nself:\n{:#?}\n\nother\n{:#?}",
                        self,
                        other
                    );

                    return Err(AlterError { id });
                }
            }
            (s, o) => {
                if s != o {
                    tracing::warn!(
                        "KafkaSinkFormat incompatible at key_format\nself:\n{:#?}\n\nother:{:#?}",
                        s,
                        o
                    );
                    return Err(AlterError { id });
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
        KafkaSinkFormat {
            key_format: self.key_format.map(|f| f.into_inline_connection(&r)),
            value_format: self.value_format.into_inline_connection(&r),
        }
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSinkFormatType, R>
    for KafkaSinkFormatType<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSinkFormatType {
        match self {
            KafkaSinkFormatType::Avro {
                schema,
                compatibility_level,
                csr_connection,
            } => KafkaSinkFormatType::Avro {
                schema,
                compatibility_level,
                csr_connection: r.resolve_connection(csr_connection).unwrap_csr(),
            },
            KafkaSinkFormatType::Json => KafkaSinkFormatType::Json,
            KafkaSinkFormatType::Text => KafkaSinkFormatType::Text,
            KafkaSinkFormatType::Bytes => KafkaSinkFormatType::Bytes,
        }
    }
}

impl RustType<ProtoKafkaSinkFormatType> for KafkaSinkFormatType {
    fn into_proto(&self) -> ProtoKafkaSinkFormatType {
        use proto_kafka_sink_format_type::Type;
        ProtoKafkaSinkFormatType {
            r#type: Some(match self {
                Self::Avro {
                    schema,
                    compatibility_level,
                    csr_connection,
                } => Type::Avro(proto_kafka_sink_format_type::ProtoKafkaSinkAvroFormat {
                    schema: schema.clone(),
                    compatibility_level: csr_compat_level_to_proto(compatibility_level),
                    csr_connection: Some(csr_connection.into_proto()),
                }),
                Self::Json => Type::Json(()),
                Self::Text => Type::Text(()),
                Self::Bytes => Type::Bytes(()),
            }),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkFormatType) -> Result<Self, TryFromProtoError> {
        use proto_kafka_sink_format_type::Type;
        let r#type = proto
            .r#type
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaSinkFormatType::type"))?;

        Ok(match r#type {
            Type::Avro(proto) => Self::Avro {
                schema: proto.schema,
                compatibility_level: csr_compat_level_from_proto(proto.compatibility_level),
                csr_connection: proto
                    .csr_connection
                    .into_rust_if_some("ProtoKafkaSinkFormatType::csr_connection")?,
            },
            Type::Json(()) => Self::Json,
            Type::Text(()) => Self::Text,
            Type::Bytes(()) => Self::Bytes,
        })
    }
}

impl RustType<ProtoKafkaSinkFormat> for KafkaSinkFormat {
    fn into_proto(&self) -> ProtoKafkaSinkFormat {
        ProtoKafkaSinkFormat {
            key_format: self.key_format.as_ref().map(|f| f.into_proto()),
            value_format: Some(self.value_format.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkFormat) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSinkFormat {
            key_format: proto.key_format.into_rust()?,
            value_format: proto
                .value_format
                .into_rust_if_some("ProtoKafkaSinkFormat::value_format")?,
        })
    }
}

fn csr_compat_level_to_proto(compatibility_level: &Option<mz_ccsr::CompatibilityLevel>) -> i32 {
    use proto_kafka_sink_format_type::proto_kafka_sink_avro_format::CompatibilityLevel as ProtoCompatLevel;
    match compatibility_level {
        Some(level) => match level {
            mz_ccsr::CompatibilityLevel::Backward => ProtoCompatLevel::Backward,
            mz_ccsr::CompatibilityLevel::BackwardTransitive => ProtoCompatLevel::BackwardTransitive,
            mz_ccsr::CompatibilityLevel::Forward => ProtoCompatLevel::Forward,
            mz_ccsr::CompatibilityLevel::ForwardTransitive => ProtoCompatLevel::ForwardTransitive,
            mz_ccsr::CompatibilityLevel::Full => ProtoCompatLevel::Full,
            mz_ccsr::CompatibilityLevel::FullTransitive => ProtoCompatLevel::FullTransitive,
            mz_ccsr::CompatibilityLevel::None => ProtoCompatLevel::None,
        },
        None => ProtoCompatLevel::Unset,
    }
    .into()
}

fn csr_compat_level_from_proto(val: i32) -> Option<mz_ccsr::CompatibilityLevel> {
    use proto_kafka_sink_format_type::proto_kafka_sink_avro_format::CompatibilityLevel as ProtoCompatLevel;
    match ProtoCompatLevel::try_from(val) {
        Ok(ProtoCompatLevel::Backward) => Some(mz_ccsr::CompatibilityLevel::Backward),
        Ok(ProtoCompatLevel::BackwardTransitive) => {
            Some(mz_ccsr::CompatibilityLevel::BackwardTransitive)
        }
        Ok(ProtoCompatLevel::Forward) => Some(mz_ccsr::CompatibilityLevel::Forward),
        Ok(ProtoCompatLevel::ForwardTransitive) => {
            Some(mz_ccsr::CompatibilityLevel::ForwardTransitive)
        }
        Ok(ProtoCompatLevel::Full) => Some(mz_ccsr::CompatibilityLevel::Full),
        Ok(ProtoCompatLevel::FullTransitive) => Some(mz_ccsr::CompatibilityLevel::FullTransitive),
        Ok(ProtoCompatLevel::None) => Some(mz_ccsr::CompatibilityLevel::None),
        Ok(ProtoCompatLevel::Unset) => None,
        Err(_) => None,
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum S3SinkFormat {
    /// Encoded using the PG `COPY` protocol, with one of its supported formats.
    PgCopy(CopyFormatParams<'static>),
    /// Encoded as Parquet.
    Parquet,
}

impl RustType<ProtoS3SinkFormat> for S3SinkFormat {
    fn into_proto(&self) -> ProtoS3SinkFormat {
        use proto_s3_sink_format::Kind;
        ProtoS3SinkFormat {
            kind: Some(match self {
                Self::PgCopy(params) => Kind::PgCopy(params.into_proto()),
                Self::Parquet => Kind::Parquet(()),
            }),
        }
    }

    fn from_proto(proto: ProtoS3SinkFormat) -> Result<Self, TryFromProtoError> {
        use proto_s3_sink_format::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoS3SinkFormat::kind"))?;

        Ok(match kind {
            Kind::PgCopy(proto) => Self::PgCopy(proto.into_rust()?),
            Kind::Parquet(_) => Self::Parquet,
        })
    }
}

/// Info required to copy the data to s3.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct S3UploadInfo {
    /// The s3 uri path to write the data to.
    pub uri: String,
    /// The max file size of each file uploaded to S3.
    pub max_file_size: u64,
    /// The relation desc of the data to be uploaded to S3.
    pub desc: RelationDesc,
    /// The selected sink format.
    pub format: S3SinkFormat,
}

impl RustType<ProtoS3UploadInfo> for S3UploadInfo {
    fn into_proto(&self) -> ProtoS3UploadInfo {
        ProtoS3UploadInfo {
            uri: self.uri.clone(),
            max_file_size: self.max_file_size,
            desc: Some(self.desc.into_proto()),
            format: Some(self.format.into_proto()),
        }
    }

    fn from_proto(proto: ProtoS3UploadInfo) -> Result<Self, TryFromProtoError> {
        Ok(S3UploadInfo {
            uri: proto.uri,
            max_file_size: proto.max_file_size,
            desc: proto.desc.into_rust_if_some("ProtoS3UploadInfo::desc")?,
            format: proto
                .format
                .into_rust_if_some("ProtoS3UploadInfo::format")?,
        })
    }
}

pub const MIN_S3_SINK_FILE_SIZE: ByteSize = ByteSize::mb(16);
pub const MAX_S3_SINK_FILE_SIZE: ByteSize = ByteSize::gb(4);
