// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related kafka sources

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use dec::OrderedDecimal;
use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::adt::numeric::Numeric;
use mz_repr::{ColumnType, Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_timely_util::order::{Extrema, Partitioned};
use once_cell::sync::Lazy;
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::connections::ConnectionContext;
use crate::controller::AlterError;
use crate::sources::{MzOffset, SourceConnection, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.kafka.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct KafkaSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection: C::Kafka,
    pub connection_id: GlobalId,
    pub topic: String,
    // Map from partition -> starting offset
    #[proptest(strategy = "proptest::collection::btree_map(any::<i32>(), any::<i64>(), 0..4)")]
    pub start_offsets: BTreeMap<i32, i64>,
    pub group_id_prefix: Option<String>,
    #[proptest(strategy = "proptest::collection::vec(any::<(String, KafkaMetadataKind)>(), 0..4)")]
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

    fn key_desc(&self) -> RelationDesc {
        RelationDesc::empty().with_column("key", ScalarType::Bytes.nullable(true))
    }

    fn value_desc(&self) -> RelationDesc {
        RelationDesc::empty().with_column("value", ScalarType::Bytes.nullable(true))
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
                                        nullable: true,
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

    fn output_idx_for_name(&self, _name: &mz_sql_parser::ast::UnresolvedItemName) -> Option<usize> {
        None
    }
}

impl<C: ConnectionAccess> crate::AlterCompatible for KafkaSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let KafkaSourceConnection {
            connection,
            connection_id,
            topic,
            start_offsets,
            group_id_prefix,
            metadata_columns,
            topic_metadata_refresh_interval,
        } = self;

        let compatibility_checks = [
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
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

                return Err(AlterError { id });
            }
        }

        Ok(())
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

/// Given an ordered type `P` it augments each of its values with a point right *before* that
/// value, exactly *at* that value, and right *after* that value. Additionally, it provides two
/// special values for positive and negative infinity that are greater than and less than all the
/// other elements respectively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum RangeBound<P> {
    /// Negative infinity.
    NegInfinity,
    /// A specific element value with its associated kind.
    Elem(P, BoundKind),
    /// Positive infinity.
    PosInfinity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BoundKind {
    /// A bound right before a value. When used as an upper it represents an exclusive range.
    Before,
    /// A bound exactly at a value. When used as a lower or upper it represents an inclusive range.
    At,
    /// A bound right after a value. When used as a lower it represents an exclusive range.
    After,
}

impl<P: std::fmt::Debug> RangeBound<P> {
    /// Constructs a range bound right before `elem`.
    pub fn before(elem: P) -> Self {
        Self::Elem(elem, BoundKind::Before)
    }

    /// Constructs a range bound exactly at `elem`.
    pub fn exact(elem: P) -> Self {
        Self::Elem(elem, BoundKind::At)
    }

    /// Constructs a range bound right after `elem`.
    pub fn after(elem: P) -> Self {
        Self::Elem(elem, BoundKind::After)
    }

    /// Unwraps the element of this bound.
    ///
    /// # Panics
    ///
    /// This method panics if this is not an exact element range bound.
    pub fn unwrap_exact(&self) -> &P {
        match self {
            RangeBound::Elem(p, BoundKind::At) => p,
            _ => panic!("attempt to unwrap_exact {self:?}"),
        }
    }
}

impl<P: fmt::Display> fmt::Display for RangeBound<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegInfinity => f.write_str("-inf"),
            Self::Elem(elem, BoundKind::Before) => write!(f, "<{elem}"),
            Self::Elem(elem, BoundKind::At) => write!(f, "{elem}"),
            Self::Elem(elem, BoundKind::After) => write!(f, "{elem}>"),
            Self::PosInfinity => f.write_str("+inf"),
        }
    }
}

impl<P> Extrema for RangeBound<P> {
    fn minimum() -> Self {
        Self::NegInfinity
    }
    fn maximum() -> Self {
        Self::PosInfinity
    }
}

impl SourceTimestamp for Partitioned<RangeBound<i32>, MzOffset> {
    fn encode_row(&self) -> Row {
        use mz_repr::adt::range;
        let mut row = Row::with_capacity(2);
        let mut packer = row.packer();

        let to_numeric = |p: i32| Datum::from(OrderedDecimal(Numeric::from(p)));

        let (lower, lower_inclusive) = match self.interval().lower {
            RangeBound::NegInfinity => (Datum::Null, false),
            RangeBound::Elem(pid, BoundKind::After) => (to_numeric(pid), false),
            RangeBound::Elem(pid, BoundKind::At) => (to_numeric(pid), true),
            lower => unreachable!("invalid lower bound {lower:?}"),
        };
        let (upper, upper_inclusive) = match self.interval().upper {
            RangeBound::PosInfinity => (Datum::Null, false),
            RangeBound::Elem(pid, BoundKind::Before) => (to_numeric(pid), false),
            RangeBound::Elem(pid, BoundKind::At) => (to_numeric(pid), true),
            upper => unreachable!("invalid upper bound {upper:?}"),
        };
        assert_eq!(lower_inclusive, upper_inclusive, "invalid range {self}");

        packer
            .push_range(range::Range::new(Some((
                range::RangeBound::new(lower, lower_inclusive),
                range::RangeBound::new(upper, upper_inclusive),
            ))))
            .expect("pushing range must not generate errors");

        packer.push(Datum::UInt64(self.timestamp().offset));
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
                        Partitioned::new_singleton(
                            RangeBound::exact(lower.unwrap()),
                            MzOffset::from(offset),
                        )
                    }
                    (false, false) => {
                        let lower = match lower {
                            Some(pid) => RangeBound::after(pid),
                            None => RangeBound::NegInfinity,
                        };
                        let upper = match upper {
                            Some(pid) => RangeBound::before(pid),
                            None => RangeBound::PosInfinity,
                        };
                        Partitioned::new_range(lower, upper, MzOffset::from(offset))
                    }
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
