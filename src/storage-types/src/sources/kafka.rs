// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related kafka sources

use dec::OrderedDecimal;
use mz_expr::PartitionId;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{ColumnType, Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_timely_util::order::{Interval, Partitioned, RangeBound};
use once_cell::sync::Lazy;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::connections::ConnectionContext;
use crate::controller::StorageError;
use crate::sources::{MzOffset, SourceConnection, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.kafka.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection: C::Kafka,
    pub connection_id: GlobalId,
    pub topic: String,
    // Map from partition -> starting offset
    pub start_offsets: BTreeMap<i32, i64>,
    pub group_id_prefix: Option<String>,
    pub metadata_columns: Vec<(String, KafkaMetadataKind)>,
    pub topic_metadata_refresh_interval: Duration,
}

impl<R: ConnectionResolver> IntoInlineConnection<KafkaSourceConnection, R>
    for KafkaSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> KafkaSourceConnection {
        let KafkaSourceConnection {
            connection,
            connection_id,
            topic,
            start_offsets,
            group_id_prefix,
            metadata_columns,
            topic_metadata_refresh_interval,
        } = self;
        KafkaSourceConnection {
            connection: r.resolve_connection(connection).unwrap_kafka(),
            connection_id,
            topic,
            start_offsets,
            group_id_prefix,
            metadata_columns,
            topic_metadata_refresh_interval,
        }
    }
}

pub static KAFKA_PROGRESS_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column(
            "partition",
            ScalarType::Range {
                element_type: Box::new(ScalarType::Numeric { max_scale: None }),
            }
            .nullable(false),
        )
        .with_column("offset", ScalarType::UInt64.nullable(true))
});

impl<C: ConnectionAccess> KafkaSourceConnection<C> {
    /// Returns the client ID to register with librdkafka with.
    ///
    /// The caller is responsible for providing the source ID as it is not known
    /// to `KafkaSourceConnection`.
    pub fn client_id(&self, connection_context: &ConnectionContext, source_id: GlobalId) -> String {
        format!(
            "materialize-{}-{}-{}",
            connection_context.environment_id, self.connection_id, source_id,
        )
    }

    /// Returns the ID for the consumer group the configured source will use.
    ///
    /// The caller is responsible for providing the source ID as it is not known
    /// to `KafkaSourceConnection`.
    pub fn group_id(&self, connection_context: &ConnectionContext, source_id: GlobalId) -> String {
        format!(
            "{}{}",
            self.group_id_prefix.as_deref().unwrap_or(""),
            self.client_id(connection_context, source_id)
        )
    }
}

impl<C: ConnectionAccess> SourceConnection for KafkaSourceConnection<C> {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn upstream_name(&self) -> Option<&str> {
        Some(self.topic.as_str())
    }

    fn timestamp_desc(&self) -> RelationDesc {
        KAFKA_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        Some(self.connection_id)
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        self.metadata_columns
            .iter()
            .map(|(name, kind)| {
                let typ = match kind {
                    KafkaMetadataKind::Partition => ScalarType::Int32.nullable(false),
                    KafkaMetadataKind::Offset => ScalarType::UInt64.nullable(false),
                    KafkaMetadataKind::Timestamp => {
                        ScalarType::Timestamp { precision: None }.nullable(false)
                    }
                    KafkaMetadataKind::Header {
                        use_bytes: true, ..
                    } => ScalarType::Bytes.nullable(true),
                    KafkaMetadataKind::Header {
                        use_bytes: false, ..
                    } => ScalarType::String.nullable(true),
                    KafkaMetadataKind::Headers => ScalarType::List {
                        element_type: Box::new(ScalarType::Record {
                            fields: vec![
                                (
                                    "key".into(),
                                    ColumnType {
                                        nullable: false,
                                        scalar_type: ScalarType::String,
                                    },
                                ),
                                (
                                    "value".into(),
                                    ColumnType {
                                        nullable: false,
                                        scalar_type: ScalarType::Bytes,
                                    },
                                ),
                            ],
                            custom_id: None,
                        }),
                        custom_id: None,
                    }
                    .nullable(false),
                };
                (&**name, typ)
            })
            .collect()
    }
}

impl<C: ConnectionAccess> crate::AlterCompatible for KafkaSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), StorageError> {
        if self == other {
            return Ok(());
        }

        let KafkaSourceConnection {
            // Connection details may change
            connection: _,
            connection_id,
            topic,
            start_offsets,
            group_id_prefix,
            metadata_columns,
            topic_metadata_refresh_interval,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (topic == &other.topic, "topic"),
            (start_offsets == &other.start_offsets, "start_offsets"),
            (group_id_prefix == &other.group_id_prefix, "group_id_prefix"),
            (
                metadata_columns == &other.metadata_columns,
                "metadata_columns",
            ),
            (
                topic_metadata_refresh_interval == &other.topic_metadata_refresh_interval,
                "topic_metadata_refresh_interval",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "KafkaSourceConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(StorageError::InvalidAlter { id });
            }
        }

        Ok(())
    }
}

impl<C: ConnectionAccess> Arbitrary for KafkaSourceConnection<C>
where
    <<C as ConnectionAccess>::Kafka as Arbitrary>::Strategy: 'static,
{
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<C::Kafka>(),
            any::<GlobalId>(),
            any::<String>(),
            proptest::collection::btree_map(any::<i32>(), any::<i64>(), 1..4),
            any::<Option<String>>(),
            proptest::collection::vec(any::<(String, KafkaMetadataKind)>(), 0..4),
            any::<Duration>(),
        )
            .prop_map(
                |(
                    connection,
                    connection_id,
                    topic,
                    start_offsets,
                    group_id_prefix,
                    metadata_columns,
                    topic_metadata_refresh_interval,
                )| KafkaSourceConnection {
                    connection,
                    connection_id,
                    topic,
                    start_offsets,
                    group_id_prefix,
                    metadata_columns,
                    topic_metadata_refresh_interval,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoKafkaSourceConnection> for KafkaSourceConnection<InlinedConnection> {
    fn into_proto(&self) -> ProtoKafkaSourceConnection {
        ProtoKafkaSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            topic: self.topic.clone(),
            start_offsets: self.start_offsets.clone(),
            group_id_prefix: self.group_id_prefix.clone(),
            metadata_columns: self
                .metadata_columns
                .iter()
                .map(|(name, kind)| ProtoKafkaMetadataColumn {
                    name: name.into_proto(),
                    kind: Some(kind.into_proto()),
                })
                .collect(),
            topic_metadata_refresh_interval: Some(
                self.topic_metadata_refresh_interval.into_proto(),
            ),
        }
    }

    fn from_proto(proto: ProtoKafkaSourceConnection) -> Result<Self, TryFromProtoError> {
        let mut metadata_columns = Vec::with_capacity(proto.metadata_columns.len());
        for c in proto.metadata_columns {
            let kind = c.kind.into_rust_if_some("ProtoKafkaMetadataColumn::kind")?;
            metadata_columns.push((c.name, kind));
        }

        Ok(KafkaSourceConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoKafkaSourceConnection::connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoKafkaSourceConnection::connection_id")?,
            topic: proto.topic,
            start_offsets: proto.start_offsets,
            group_id_prefix: proto.group_id_prefix,
            metadata_columns,
            topic_metadata_refresh_interval: proto
                .topic_metadata_refresh_interval
                .into_rust_if_some("ProtoKafkaSourceConnection::topic_metadata_refresh_interval")?,
        })
    }
}

impl SourceTimestamp for Partitioned<i32, MzOffset> {
    fn from_compat_ts(pid: PartitionId, offset: MzOffset) -> Self {
        match pid {
            PartitionId::Kafka(pid) => Partitioned::with_partition(pid, offset),
            PartitionId::None => panic!("invalid partitioned partition {pid}"),
        }
    }

    fn try_into_compat_ts(&self) -> Option<(PartitionId, MzOffset)> {
        let pid = self.partition()?;
        Some((PartitionId::Kafka(*pid), *self.timestamp()))
    }

    fn encode_row(&self) -> Row {
        use mz_repr::adt::range;
        let mut row = Row::with_capacity(2);
        let mut packer = row.packer();

        let to_numeric = |p: i32| Datum::from(OrderedDecimal(Numeric::from(p)));

        let (lower, upper) = match self.interval() {
            Interval::Range(l, u) => match (l, u) {
                (RangeBound::Bottom, RangeBound::Top) => {
                    ((Datum::Null, false), (Datum::Null, false))
                }
                (RangeBound::Bottom, RangeBound::Elem(pid)) => {
                    ((Datum::Null, false), (to_numeric(*pid), false))
                }
                (RangeBound::Elem(pid), RangeBound::Top) => {
                    ((to_numeric(*pid), false), (Datum::Null, false))
                }
                (RangeBound::Elem(l_pid), RangeBound::Elem(u_pid)) => {
                    ((to_numeric(*l_pid), false), (to_numeric(*u_pid), false))
                }
                o => unreachable!("don't know how to handle this partition {o:?}"),
            },
            Interval::Point(pid) => ((to_numeric(*pid), true), (to_numeric(*pid), true)),
        };

        let offset = self.timestamp().offset;

        packer
            .push_range(range::Range::new(Some((
                range::RangeBound::new(lower.0, lower.1),
                range::RangeBound::new(upper.0, upper.1),
            ))))
            .expect("pushing range must not generate errors");

        packer.push(Datum::UInt64(offset));
        row
    }
    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();

        match (datums.next(), datums.next(), datums.next()) {
            (Some(Datum::Range(range)), Some(Datum::UInt64(offset)), None) => {
                let mut range = range.into_bounds(|b| b.datum());
                //XXX: why do we have to canonicalize on read?
                range.canonicalize().expect("ranges must be valid");
                let range = range.inner.expect("empty range");

                let lower = range.lower.bound.map(|row| {
                    i32::try_from(row.unwrap_numeric().0)
                        .expect("only i32 values converted to ranges")
                });
                let upper = range.upper.bound.map(|row| {
                    i32::try_from(row.unwrap_numeric().0)
                        .expect("only i32 values converted to ranges")
                });

                match (range.lower.inclusive, range.upper.inclusive) {
                    (true, true) => {
                        assert_eq!(lower, upper);
                        Partitioned::with_partition(lower.unwrap(), MzOffset::from(offset))
                    }
                    (false, false) => Partitioned::with_range(lower, upper, MzOffset::from(offset)),
                    _ => panic!("invalid timestamp"),
                }
            }
            invalid_binding => unreachable!("invalid binding {:?}", invalid_binding),
        }
    }
}

/// Which piece of metadata a column corresponds to
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KafkaMetadataKind {
    Partition,
    Offset,
    Timestamp,
    Headers,
    Header { key: String, use_bytes: bool },
}

impl RustType<ProtoKafkaMetadataKind> for KafkaMetadataKind {
    fn into_proto(&self) -> ProtoKafkaMetadataKind {
        use proto_kafka_metadata_kind::Kind;
        ProtoKafkaMetadataKind {
            kind: Some(match self {
                KafkaMetadataKind::Partition => Kind::Partition(()),
                KafkaMetadataKind::Offset => Kind::Offset(()),
                KafkaMetadataKind::Timestamp => Kind::Timestamp(()),
                KafkaMetadataKind::Headers => Kind::Headers(()),
                KafkaMetadataKind::Header { key, use_bytes } => Kind::Header(ProtoKafkaHeader {
                    key: key.clone(),
                    use_bytes: *use_bytes,
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoKafkaMetadataKind) -> Result<Self, TryFromProtoError> {
        use proto_kafka_metadata_kind::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKafkaMetadataKind::kind"))?;
        Ok(match kind {
            Kind::Partition(()) => KafkaMetadataKind::Partition,
            Kind::Offset(()) => KafkaMetadataKind::Offset,
            Kind::Timestamp(()) => KafkaMetadataKind::Timestamp,
            Kind::Headers(()) => KafkaMetadataKind::Headers,
            Kind::Header(ProtoKafkaHeader { key, use_bytes }) => {
                KafkaMetadataKind::Header { key, use_bytes }
            }
        })
    }
}
