// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to reporting changing collections out of `dataflow`.

use std::collections::{BTreeMap, HashSet};

use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use timely::PartialOrder;

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationDesc};

use crate::controller::CollectionMetadata;
use crate::types::connections::{
    CsrConnection, KafkaConnection, PopulateClientConfig, StringOrSecret,
};

include!(concat!(env!("OUT_DIR"), "/mz_storage.types.sinks.rs"));

/// A sink for updates to a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct StorageSinkDesc<S = (), T = mz_repr::Timestamp> {
    pub from: GlobalId,
    pub from_desc: RelationDesc,
    pub connection: StorageSinkConnection,
    pub envelope: Option<SinkEnvelope>,
    pub as_of: SinkAsOf<T>,
    pub from_storage_metadata: S,
}

impl Arbitrary for StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<RelationDesc>(),
            any::<StorageSinkConnection>(),
            any::<Option<SinkEnvelope>>(),
            any::<SinkAsOf<mz_repr::Timestamp>>(),
            any::<CollectionMetadata>(),
        )
            .prop_map(
                |(from, from_desc, connection, envelope, as_of, from_storage_metadata)| {
                    StorageSinkDesc {
                        from,
                        from_desc,
                        connection,
                        envelope,
                        as_of,
                        from_storage_metadata,
                    }
                },
            )
            .boxed()
    }
}

impl RustType<ProtoStorageSinkDesc> for StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoStorageSinkDesc {
        ProtoStorageSinkDesc {
            connection: Some(self.connection.into_proto()),
            from: Some(self.from.into_proto()),
            from_desc: Some(self.from_desc.into_proto()),
            envelope: self.envelope.into_proto(),
            as_of: Some(self.as_of.into_proto()),
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
            envelope: proto.envelope.into_rust()?,
            as_of: proto
                .as_of
                .into_rust_if_some("ProtoStorageSinkDesc::as_of")?,
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
    pub fn maybe_fast_forward(&self, other_since: &Antichain<T>) -> Self {
        if PartialOrder::less_equal(&self.frontier, other_since) {
            SinkAsOf {
                frontier: other_since.to_owned(),
                // If we're using the since, never read the snapshot
                strict: true,
            }
        } else {
            self.to_owned()
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

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum StorageSinkConnection {
    Kafka(KafkaSinkConnection),
}

impl RustType<ProtoStorageSinkConnection> for StorageSinkConnection {
    fn into_proto(&self) -> ProtoStorageSinkConnection {
        use proto_storage_sink_connection::Kind;
        ProtoStorageSinkConnection {
            kind: Some(match self {
                StorageSinkConnection::Kafka(kafka) => Kind::Kafka(kafka.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoStorageSinkConnection) -> Result<Self, TryFromProtoError> {
        use proto_storage_sink_connection::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoStorageSinkConnection::kind"))?;
        Ok(match kind {
            Kind::Kafka(kafka) => StorageSinkConnection::Kafka(kafka.into_rust()?),
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkProgressConnection {
    pub topic: String,
}

impl RustType<ProtoKafkaSinkProgressConnection> for KafkaSinkProgressConnection {
    fn into_proto(&self) -> ProtoKafkaSinkProgressConnection {
        ProtoKafkaSinkProgressConnection {
            topic: self.topic.clone(),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkProgressConnection) -> Result<Self, TryFromProtoError> {
        Ok(KafkaSinkProgressConnection { topic: proto.topic })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnection {
    pub connection: KafkaConnection,
    pub options: BTreeMap<String, StringOrSecret>,
    pub topic: String,
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub relation_key_indices: Option<Vec<usize>>,
    pub value_desc: RelationDesc,
    pub published_schema_info: Option<PublishedSchemaInfo>,
    pub progress: KafkaSinkProgressConnection,
    pub exactly_once: bool,
    // Maximum number of records the sink will attempt to send each time it is
    // invoked
    pub fuel: usize,
}

impl PopulateClientConfig for KafkaSinkConnection {
    fn kafka_connection(&self) -> &KafkaConnection {
        &self.connection
    }
    fn options(&self) -> &BTreeMap<String, StringOrSecret> {
        &self.options
    }
    fn drop_option_keys() -> HashSet<&'static str> {
        ["statistics.interval.ms", "isolation.level"].into()
    }
}

proptest::prop_compose! {
    fn any_kafka_sink_connection()(
        connection in any::<KafkaConnection>(),
        options in any::<BTreeMap<String, StringOrSecret>>(),
        topic in any::<String>(),
        key_desc_and_indices in any::<Option<(RelationDesc, Vec<usize>)>>(),
        relation_key_indices in any::<Option<Vec<usize>>>(),
        value_desc in any::<RelationDesc>(),
        published_schema_info in any::<Option<PublishedSchemaInfo>>(),
        progress in any::<KafkaSinkProgressConnection>(),
        exactly_once in any::<bool>(),
        fuel in any::<usize>(),
    ) -> KafkaSinkConnection {
        KafkaSinkConnection {
            connection,
            options,
            topic,
            key_desc_and_indices,
            relation_key_indices,
            value_desc,
            published_schema_info,
            progress,
            exactly_once,
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
            options: self
                .options
                .iter()
                .map(|(k, v)| (k.clone(), v.into_proto()))
                .collect(),
            topic: self.topic.clone(),
            key_desc_and_indices: self.key_desc_and_indices.into_proto(),
            relation_key_indices: self.relation_key_indices.into_proto(),
            value_desc: Some(self.value_desc.into_proto()),
            published_schema_info: self.published_schema_info.into_proto(),
            progress: Some(self.progress.into_proto()),
            exactly_once: self.exactly_once,
            fuel: self.fuel.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaSinkConnection) -> Result<Self, TryFromProtoError> {
        let options: Result<_, TryFromProtoError> = proto
            .options
            .into_iter()
            .map(|(k, v)| StringOrSecret::from_proto(v).map(|v| (k, v)))
            .collect();

        Ok(KafkaSinkConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoKafkaSinkConnection::connection")?,
            options: options?,
            topic: proto.topic,
            key_desc_and_indices: proto.key_desc_and_indices.into_rust()?,
            relation_key_indices: proto.relation_key_indices.into_rust()?,
            value_desc: proto
                .value_desc
                .into_rust_if_some("ProtoKafkaSinkConnection::addrs")?,
            published_schema_info: proto.published_schema_info.into_rust()?,
            progress: proto
                .progress
                .into_rust_if_some("ProtoKafkaSinkConnection::progress")?,
            exactly_once: proto.exactly_once,
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
    fn into_proto(&self) -> ProtoPublishedSchemaInfo {
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

impl StorageSinkConnection {
    /// Returns the name of the sink connection.
    pub fn name(&self) -> &'static str {
        match self {
            StorageSinkConnection::Kafka(_) => "kafka",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageSinkConnectionBuilder {
    Kafka(KafkaSinkConnectionBuilder),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaConsistencyConfig {
    Progress { topic: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectionBuilder {
    pub connection_id: GlobalId,
    pub connection: KafkaConnection,
    pub options: BTreeMap<String, StringOrSecret>,
    pub format: KafkaSinkFormat,
    /// A natural key of the sinked relation (view or source).
    pub relation_key_indices: Option<Vec<usize>>,
    /// The user-specified key for the sink.
    pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    pub value_desc: RelationDesc,
    pub topic_name: String,
    pub consistency_config: KafkaConsistencyConfig,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub fuel: usize,
    pub retention: KafkaSinkConnectionRetention,
}

impl PopulateClientConfig for KafkaSinkConnectionBuilder {
    fn kafka_connection(&self) -> &KafkaConnection {
        &self.connection
    }
    fn options(&self) -> &BTreeMap<String, StringOrSecret> {
        &self.options
    }
    fn drop_option_keys() -> HashSet<&'static str> {
        ["statistics.interval.ms", "isolation.level"].into()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSinkConnectionRetention {
    pub duration: Option<i64>,
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
