// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to reporting changing collections out of `dataflow`.

use std::time::Duration;

use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;

use mz_persist_client::ShardId;
use mz_repr::proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};

use crate::client::controller::storage::CollectionMetadata;
use crate::connections::{CsrConnection, KafkaConnection};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.types.sinks.rs"
));

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SinkDesc<S = (), T = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: SinkConnection<S>,
    pub envelope: Option<SinkEnvelope>,
    pub as_of: SinkAsOf<T>,
}

impl Arbitrary for SinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<SinkConnection<CollectionMetadata>>(),
            any::<Option<SinkEnvelope>>(),
            any::<SinkAsOf<mz_repr::Timestamp>>(),
        )
            .prop_map(|(from, from_desc, connection, envelope, as_of)| SinkDesc {
                from,
                from_desc,
                connection,
                envelope,
                as_of,
            })
            .boxed()
    }
}

impl RustType<ProtoSinkDesc> for SinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoSinkDesc {
        ProtoSinkDesc {
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            connection: Some(self.connection.into_proto()),
            envelope: self.envelope.into_proto(),
            as_of: Some(self.as_of.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSinkDesc) -> Result<Self, TryFromProtoError> {
        Ok(SinkDesc {
            from: proto.from.into_rust_if_some("ProtoSinkDesc::from")?,
            from_desc: proto
                .from_desc
                .into_rust_if_some("ProtoSinkDesc::from_desc")?,
            connection: proto
                .connection
                .into_rust_if_some("ProtoSinkDesc::connection")?,
            envelope: proto.envelope.into_rust()?,
            as_of: proto.as_of.into_rust_if_some("ProtoSinkDesc::as_of")?,
        })
    }
}

#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkEnvelope {
    Debezium,
    Upsert,
    /// An envelope for sinks that directly write differential Rows. This is internal and
    /// cannot be requested via SQL.
    DifferentialRow,
}

impl RustType<ProtoSinkEnvelope> for SinkEnvelope {
    fn into_proto(&self) -> ProtoSinkEnvelope {
        use proto_sink_envelope::Kind;
        ProtoSinkEnvelope {
            kind: Some(match self {
                SinkEnvelope::Debezium => Kind::Debezium(()),
                SinkEnvelope::Upsert => Kind::Upsert(()),
                SinkEnvelope::DifferentialRow => Kind::DifferentialRow(()),
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
            Kind::DifferentialRow(()) => SinkEnvelope::DifferentialRow,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkAsOf<T = mz_repr::Timestamp> {
    pub frontier: Antichain<T>,
    pub strict: bool,
}

impl Arbitrary for SinkAsOf<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (proptest::collection::vec(any::<u64>(), 1..4), any::<bool>())
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
            frontier: Some((&self.frontier).into()),
            strict: self.strict,
        }
    }

    fn from_proto(proto: ProtoSinkAsOf) -> Result<Self, TryFromProtoError> {
        Ok(SinkAsOf {
            frontier: proto
                .frontier
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoSinkAsOf::frontier"))?,
            strict: proto.strict,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SinkConnection<S = ()> {
    Kafka(KafkaSinkConnection),
    Tail(TailSinkConnection),
    Persist(PersistSinkConnection<S>),
}

impl RustType<ProtoSinkConnection> for SinkConnection<CollectionMetadata> {
    fn into_proto(&self) -> ProtoSinkConnection {
        use proto_sink_connection::Kind;
        ProtoSinkConnection {
            kind: Some(match self {
                SinkConnection::Kafka(kafka) => Kind::Kafka(kafka.into_proto()),
                SinkConnection::Tail(_) => Kind::Tail(()),
                SinkConnection::Persist(persist) => Kind::Persist(persist.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSinkConnection) -> Result<Self, TryFromProtoError> {
        use proto_sink_connection::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSinkConnection::kind"))?;
        Ok(match kind {
            Kind::Kafka(kafka) => SinkConnection::Kafka(kafka.into_rust()?),
            Kind::Tail(()) => SinkConnection::Tail(TailSinkConnection {}),
            Kind::Persist(persist) => SinkConnection::Persist(persist.into_rust()?),
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConsistencyConnection {
    pub topic: String,
    pub schema_id: i32,
}

impl RustType<ProtoKafkaSinkConsistencyConnection> for KafkaSinkConsistencyConnection {
    fn into_proto(self: &Self) -> ProtoKafkaSinkConsistencyConnection {
        ProtoKafkaSinkConsistencyConnection {
            topic: self.topic.clone(),
            schema_id: self.schema_id,
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConsistencyConnection) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSinkConsistencyConnection {
            topic: proto.topic,
            schema_id: proto.schema_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnection {
    pub connection: KafkaConnection,
    pub topic: String,
    pub topic_prefix: String,
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub relation_key_indices: Option<Vec<usize>>,
    pub value_desc: RelationDesc,
    pub published_schema_info: Option<PublishedSchemaInfo>,
    pub consistency: Option<KafkaSinkConsistencyConnection>,
    pub exactly_once: bool,
    // Source dependencies for exactly-once sinks.
    pub transitive_source_dependencies: Vec<GlobalId>,
    // Maximum number of records the sink will attempt to send each time it is
    // invoked
    pub fuel: usize,
}

proptest::prop_compose! {
    fn any_kafka_sink_connection()(
        connection in any::<KafkaConnection>(),
        topic in any::<String>(),
        topic_prefix in any::<String>(),
        key_desc_and_indices in any::<Option<(RelationDesc, Vec<usize>)>>(),
        relation_key_indices in any::<Option<Vec<usize>>>(),
        value_desc in any::<RelationDesc>(),
        published_schema_info in any::<Option<PublishedSchemaInfo>>(),
        consistency in any::<Option<KafkaSinkConsistencyConnection>>(),
        exactly_once in any::<bool>(),
        transitive_source_dependencies in any::<Vec<GlobalId>>(),
        fuel in any::<usize>(),
    ) -> KafkaSinkConnection {
        KafkaSinkConnection {
            connection,
            topic,
            topic_prefix,
            key_desc_and_indices,
            relation_key_indices,
            value_desc,
            published_schema_info,
            consistency,
            exactly_once,
            transitive_source_dependencies,
            fuel,
        }
    }
}

impl Arbitrary for KafkaSinkConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_kafka_sink_connection().boxed()
    }
}

impl RustType<proto_kafka_sink_connection::ProtoKeyDescAndIndices> for (RelationDesc, Vec<usize>) {
    fn into_proto(&self) -> proto_kafka_sink_connection::ProtoKeyDescAndIndices {
        proto_kafka_sink_connection::ProtoKeyDescAndIndices {
            desc: Some(self.0.into_proto()),
            indices: self.1.into_proto(),
        }
    }

    fn from_proto(
        proto: proto_kafka_sink_connection::ProtoKeyDescAndIndices,
    ) -> Result<Self, TryFromProtoError> {
        Ok((
            proto
                .desc
                .into_rust_if_some("ProtoKeyDescAndIndices::desc")?,
            proto.indices.into_rust()?,
        ))
    }
}

impl RustType<proto_kafka_sink_connection::ProtoRelationKeyIndicesVec> for Vec<usize> {
    fn into_proto(&self) -> proto_kafka_sink_connection::ProtoRelationKeyIndicesVec {
        proto_kafka_sink_connection::ProtoRelationKeyIndicesVec {
            relation_key_indices: self.into_proto(),
        }
    }

    fn from_proto(
        proto: proto_kafka_sink_connection::ProtoRelationKeyIndicesVec,
    ) -> Result<Self, TryFromProtoError> {
        proto.relation_key_indices.into_rust()
    }
}

impl RustType<ProtoKafkaSinkConnection> for KafkaSinkConnection {
    fn into_proto(&self) -> ProtoKafkaSinkConnection {
        ProtoKafkaSinkConnection {
            connection: Some(self.connection.into_proto()),
            topic: self.topic.clone(),
            topic_prefix: self.topic_prefix.clone(),
            key_desc_and_indices: self.key_desc_and_indices.into_proto(),
            relation_key_indices: self.relation_key_indices.into_proto(),
            value_desc: Some(self.value_desc.into_proto()),
            published_schema_info: self.published_schema_info.into_proto(),
            consistency: self.consistency.into_proto(),
            exactly_once: self.exactly_once,
            transitive_source_dependencies: self.transitive_source_dependencies.into_proto(),
            fuel: self.fuel.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConnection) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSinkConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoKafkaSinkConnection::connection")?,
            topic: proto.topic,
            topic_prefix: proto.topic_prefix,
            key_desc_and_indices: proto.key_desc_and_indices.into_rust()?,
            relation_key_indices: proto.relation_key_indices.into_rust()?,
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoKafkaSinkConnection::addrs")?,
            published_schema_info: proto.published_schema_info.into_rust()?,
            consistency: proto.consistency.into_rust()?,
            exactly_once: proto.exactly_once,
            transitive_source_dependencies: proto.transitive_source_dependencies.into_rust()?,
            fuel: proto.fuel.into_rust()?,
        })
    }
}

/// TODO(JLDLaughlin): Documentation.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishedSchemaInfo {
    pub key_schema_id: Option<i32>,
    pub value_schema_id: i32,
}

impl RustType<ProtoPublishedSchemaInfo> for PublishedSchemaInfo {
    fn into_proto(self: &Self) -> ProtoPublishedSchemaInfo {
        ProtoPublishedSchemaInfo {
            key_schema_id: self.key_schema_id.clone(),
            value_schema_id: self.value_schema_id,
        }
    }

    fn from_proto(proto: ProtoPublishedSchemaInfo) -> Result<Self, TryFromProtoError> {
        Ok(PublishedSchemaInfo {
            key_schema_id: proto.key_schema_id,
            value_schema_id: proto.value_schema_id,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistSinkConnection<S> {
    pub value_desc: RelationDesc,
    pub storage_metadata: S,
}

impl RustType<ProtoPersistSinkConnection> for PersistSinkConnection<CollectionMetadata> {
    fn into_proto(self: &Self) -> ProtoPersistSinkConnection {
        ProtoPersistSinkConnection {
            value_desc: Some(self.value_desc.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPersistSinkConnection) -> Result<Self, TryFromProtoError> {
        Ok(PersistSinkConnection {
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoPersistSinkConnection::value_desc")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoPersistSinkConnection::storage_metadata")?,
        })
    }
}

impl<S> SinkConnection<S> {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            SinkConnection::Kafka(_) => "kafka",
            SinkConnection::Tail(_) => "tail",
            SinkConnection::Persist(_) => "persist",
        }
    }

    /// Returns `true` if this sink requires sources to block timestamp binding
    /// compaction until all sinks that depend on a given source have finished
    /// writing out that timestamp.
    ///
    /// To achieve that, each sink will hold a `AntichainToken` for all of
    /// the sources it depends on, and will advance all of its source
    /// dependencies' compaction frontiers as it completes writes.
    ///
    /// Sinks that do need to hold back compaction need to insert an
    /// [`Antichain`] into `StorageState::sink_write_frontiers` that they update
    /// in order to advance the frontier that holds back upstream compaction
    /// of timestamp bindings.
    ///
    /// See also [`transitive_source_dependencies`](SinkConnection::transitive_source_dependencies).
    pub fn requires_source_compaction_holdback(&self) -> bool {
        match self {
            SinkConnection::Kafka(k) => k.exactly_once,
            SinkConnection::Tail(_) => false,
            SinkConnection::Persist(_) => false,
        }
    }

    /// Returns the [`GlobalIds`](GlobalId) of the transitive sources of this
    /// sink.
    pub fn transitive_source_dependencies(&self) -> &[GlobalId] {
        match self {
            SinkConnection::Kafka(k) => &k.transitive_source_dependencies,
            SinkConnection::Tail(_) => &[],
            SinkConnection::Persist(_) => &[],
        }
    }
}

#[derive(Arbitrary, Default, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TailSinkConnection {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkConnectionBuilder {
    Kafka(KafkaSinkConnectionBuilder),
    Persist(PersistSinkConnectionBuilder),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PersistSinkConnectionBuilder {
    pub consensus_uri: String,
    pub blob_uri: String,
    pub shard_id: ShardId,
    pub value_desc: RelationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectionBuilder {
    pub connection: KafkaConnection,
    pub format: KafkaSinkFormat,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub value_desc: RelationDesc,
    pub topic_prefix: String,
    pub consistency_topic_prefix: Option<String>,
    pub consistency_format: Option<KafkaSinkFormat>,
    pub topic_suffix_nonce: String,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub fuel: usize,
    // Forces the sink to always write to the same topic across restarts instead
    // of picking a new topic each time.
    pub reuse_topic: bool,
    // Source dependencies for exactly-once sinks.
    pub transitive_source_dependencies: Vec<GlobalId>,
    pub retention: KafkaSinkConnectionRetention,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectionRetention {
    pub duration: Option<Option<Duration>>,
    pub bytes: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaSinkFormat {
    Avro {
        key_schema: Option<String>,
        value_schema: String,
        csr_connection: CsrConnection,
    },
    Json,
}
