// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to the introduction of changing collections into `dataflow`.

use std::collections::{BTreeMap, HashMap};
use std::num::TryFromIntError;
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::time::Duration;

use anyhow::{anyhow, bail};
use bytes::BufMut;
use chrono::NaiveDateTime;

use globset::{Glob, GlobBuilder};
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Just, Strategy};
use proptest::prop_oneof;
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use uuid::Uuid;

use mz_kafka_util::KafkaAddrs;
use mz_persist_types::Codec;
use mz_repr::chrono::any_naive_datetime;
use mz_repr::proto::{any_duration, any_uuid, TryFromProtoError};
use mz_repr::proto::{IntoRustIfSome, ProtoType, RustType};
use mz_repr::{ColumnType, GlobalId, RelationDesc, RelationType, Row, ScalarType};

pub mod encoding;

use crate::aws::AwsConfig;
use crate::DataflowError;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.types.sources.rs"
));

/// A description of a source ingestion
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IngestionDescription<S = (), T = mz_repr::Timestamp> {
    /// Source collections made available to this ingestion.
    pub source_imports: BTreeMap<GlobalId, S>,
    /// The source identifier
    pub id: GlobalId,
    /// The source description
    pub desc: SourceDesc,
    /// The initial `since` frontier
    pub since: Antichain<T>,
    /// Additional storage controller metadata needed to ingest this source
    pub storage_metadata: S,
}

/// Universal language for describing message positions in Materialize, in a source independent
/// way. Individual sources like Kafka or File sources should explicitly implement their own offset
/// type that converts to/From MzOffsets. A 0-MzOffset denotes an empty stream.
#[derive(
    Copy,
    Clone,
    Default,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Arbitrary,
)]
pub struct MzOffset {
    pub offset: u64,
}

impl RustType<ProtoMzOffset> for MzOffset {
    fn into_proto(&self) -> ProtoMzOffset {
        ProtoMzOffset {
            offset: self.offset,
        }
    }

    fn from_proto(proto: ProtoMzOffset) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            offset: proto.offset,
        })
    }
}

impl MzOffset {
    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.offset
            .checked_sub(other.offset)
            .map(|offset| Self { offset })
    }
}

/// Convert from MzOffset to Kafka::Offset as long as
/// the offset is not negative
impl From<u64> for MzOffset {
    fn from(offset: u64) -> Self {
        Self { offset }
    }
}

impl std::fmt::Display for MzOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.offset)
    }
}

// Assume overflow does not occur for addition
impl Add<u64> for MzOffset {
    type Output = MzOffset;

    fn add(self, x: u64) -> MzOffset {
        MzOffset {
            offset: self.offset + x,
        }
    }
}
impl Add<Self> for MzOffset {
    type Output = Self;

    fn add(self, x: Self) -> Self {
        MzOffset {
            offset: self.offset + x.offset,
        }
    }
}
impl AddAssign<u64> for MzOffset {
    fn add_assign(&mut self, x: u64) {
        self.offset += x;
    }
}
impl AddAssign<Self> for MzOffset {
    fn add_assign(&mut self, x: Self) {
        self.offset += x.offset;
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct KafkaOffset {
    pub offset: i64,
}

/// Convert from KafkaOffset to MzOffset (1-indexed), failing if the offset
/// is negative.
impl TryFrom<KafkaOffset> for MzOffset {
    type Error = TryFromIntError;
    fn try_from(kafka_offset: KafkaOffset) -> Result<Self, Self::Error> {
        Ok(MzOffset {
            // If the offset is negative, or +1 overflows, then this
            // fails
            offset: (kafka_offset.offset + 1).try_into()?,
        })
    }
}

/// Convert from `PgLsn` to MzOffset
impl From<tokio_postgres::types::PgLsn> for MzOffset {
    fn from(lsn: tokio_postgres::types::PgLsn) -> Self {
        MzOffset { offset: lsn.into() }
    }
}

/// Which piece of metadata a column corresponds to
#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IncludedColumnSource {
    /// The materialize-specific notion of "position"
    ///
    /// This is legacy, and should be removed when default metadata is no longer included
    DefaultPosition,
    Partition,
    Offset,
    Timestamp,
    Topic,
    Headers,
}

impl RustType<ProtoIncludedColumnSource> for IncludedColumnSource {
    fn into_proto(self: &Self) -> ProtoIncludedColumnSource {
        use proto_included_column_source::Kind;
        ProtoIncludedColumnSource {
            kind: Some(match self {
                IncludedColumnSource::DefaultPosition => Kind::DefaultPosition(()),
                IncludedColumnSource::Partition => Kind::Partition(()),
                IncludedColumnSource::Offset => Kind::Offset(()),
                IncludedColumnSource::Timestamp => Kind::Timestamp(()),
                IncludedColumnSource::Topic => Kind::Topic(()),
                IncludedColumnSource::Headers => Kind::Headers(()),
            }),
        }
    }

    fn from_proto(proto: ProtoIncludedColumnSource) -> Result<Self, TryFromProtoError> {
        use proto_included_column_source::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoIncludedColumnSource::kind"))?;
        Ok(match kind {
            Kind::DefaultPosition(()) => IncludedColumnSource::DefaultPosition,
            Kind::Partition(()) => IncludedColumnSource::Partition,
            Kind::Offset(()) => IncludedColumnSource::Offset,
            Kind::Timestamp(()) => IncludedColumnSource::Timestamp,
            Kind::Topic(()) => IncludedColumnSource::Topic,
            Kind::Headers(()) => IncludedColumnSource::Headers,
        })
    }
}

/// Whether and how to include the decoded key of a stream in dataflows
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyEnvelope {
    /// Never include the key in the output row
    None,
    /// For composite key encodings, pull the fields from the encoding into columns.
    Flattened,
    /// Upsert is identical to Flattened but differs for non-avro sources, for which key names are overwritten.
    LegacyUpsert,
    /// Always use the given name for the key.
    ///
    /// * For a single-field key, this means that the column will get the given name.
    /// * For a multi-column key, the columns will get packed into a [`ScalarType::Record`], and
    ///   that Record will get the given name.
    Named(String),
}

impl RustType<ProtoKeyEnvelope> for KeyEnvelope {
    fn into_proto(self: &Self) -> ProtoKeyEnvelope {
        use proto_key_envelope::Kind;
        ProtoKeyEnvelope {
            kind: Some(match self {
                KeyEnvelope::None => Kind::None(()),
                KeyEnvelope::Flattened => Kind::Flattened(()),
                KeyEnvelope::LegacyUpsert => Kind::LegacyUpsert(()),
                KeyEnvelope::Named(name) => Kind::Named(name.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoKeyEnvelope) -> Result<Self, TryFromProtoError> {
        use proto_key_envelope::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKeyEnvelope::kind"))?;
        Ok(match kind {
            Kind::None(()) => KeyEnvelope::None,
            Kind::Flattened(()) => KeyEnvelope::Flattened,
            Kind::LegacyUpsert(()) => KeyEnvelope::LegacyUpsert,
            Kind::Named(name) => KeyEnvelope::Named(name),
        })
    }
}

/// A column that was created via an `INCLUDE` expression
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IncludedColumnPos {
    pub name: String,
    pub pos: usize,
}

impl RustType<ProtoIncludedColumnPos> for IncludedColumnPos {
    fn into_proto(&self) -> ProtoIncludedColumnPos {
        ProtoIncludedColumnPos {
            name: self.name.clone(),
            pos: self.pos.into_proto(),
        }
    }

    fn from_proto(proto: ProtoIncludedColumnPos) -> Result<Self, TryFromProtoError> {
        Ok(IncludedColumnPos {
            name: proto.name,
            pos: usize::from_proto(proto.pos)?,
        })
    }
}

/// The meaning of the timestamp number produced by data sources. This type
/// is not concerned with the source of the timestamp (like if the data came
/// from a Debezium consistency topic or a CDCv2 stream), instead only what the
/// timestamp number means.
///
/// Some variants here have attached data used to differentiate incomparable
/// instantiations. These attached data types should be expanded in the future
/// if we need to tell apart more kinds of sources.
#[derive(Arbitrary, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum Timeline {
    /// EpochMilliseconds means the timestamp is the number of milliseconds since
    /// the Unix epoch.
    EpochMilliseconds,
    /// External means the timestamp comes from an external data source and we
    /// don't know what the number means. The attached String is the source's name,
    /// which will result in different sources being incomparable.
    External(String),
    /// User means the user has manually specified a timeline. The attached
    /// String is specified by the user, allowing them to decide sources that are
    /// joinable.
    User(String),
}

impl RustType<ProtoTimeline> for Timeline {
    fn into_proto(self: &Self) -> ProtoTimeline {
        use proto_timeline::Kind;
        ProtoTimeline {
            kind: Some(match self {
                Timeline::EpochMilliseconds => Kind::EpochMilliseconds(()),
                Timeline::External(s) => Kind::External(s.clone()),
                Timeline::User(s) => Kind::User(s.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoTimeline) -> Result<Self, TryFromProtoError> {
        use proto_timeline::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoTimeline::kind"))?;
        Ok(match kind {
            Kind::EpochMilliseconds(()) => Timeline::EpochMilliseconds,
            Kind::External(s) => Timeline::External(s),
            Kind::User(s) => Timeline::User(s),
        })
    }
}

/// `SourceEnvelope`s, describe how to turn a stream of messages from `SourceConnector`s,
/// and turn them into a _differential stream_, that is, a stream of (data, time, diff)
/// triples.
///
/// Some sources (namely postgres and pubnub) skip any explicit envelope handling, effectively
/// asserting that `SourceEnvelope` is `None` with `KeyEnvelope::None`.
// TODO(guswynn): update this ^ when SimpleSource is gone.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SourceEnvelope {
    /// The most trivial version is `None`, which typically produces triples where the diff
    /// is `1`. However, some sources are able to produce values with more exotic diff's,
    /// such as the posgres source. Currently, this is the only variant usable with
    /// those sources.
    ///
    /// If the `KeyEnvelope` is present,
    /// include the key columns as an output column of the source with the given properties.
    None(NoneEnvelope),
    /// `Debezium` avoids holding onto previously seen values by trusting the required
    /// `before` and `after` value fields coming from the upstream source.
    Debezium(DebeziumEnvelope),
    /// `Upsert` holds onto previously seen values and produces `1` or `-1` diffs depending on
    /// whether or not the required _key_ outputed by the source has been seen before. This also
    /// supports a `Debezium` mode.
    Upsert(UpsertEnvelope),
    /// `CdcV2` requires sources output messages in a strict form that requires a upstream-provided
    /// timeline.
    CdcV2,
    /// An envelope for sources that directly read differential Rows. This is internal and
    /// cannot be requested via SQL.
    DifferentialRow,
}

impl RustType<ProtoSourceEnvelope> for SourceEnvelope {
    fn into_proto(self: &Self) -> ProtoSourceEnvelope {
        use proto_source_envelope::Kind;
        ProtoSourceEnvelope {
            kind: Some(match self {
                SourceEnvelope::None(e) => Kind::None(e.into_proto()),
                SourceEnvelope::Debezium(e) => Kind::Debezium(e.into_proto()),
                SourceEnvelope::Upsert(e) => Kind::Upsert(e.into_proto()),
                SourceEnvelope::CdcV2 => Kind::CdcV2(()),
                SourceEnvelope::DifferentialRow => Kind::DifferentialRow(()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceEnvelope) -> Result<Self, TryFromProtoError> {
        use proto_source_envelope::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSourceEnvelope::kind"))?;
        Ok(match kind {
            Kind::None(e) => SourceEnvelope::None(e.into_rust()?),
            Kind::Debezium(e) => SourceEnvelope::Debezium(e.into_rust()?),
            Kind::Upsert(e) => SourceEnvelope::Upsert(e.into_rust()?),
            Kind::CdcV2(()) => SourceEnvelope::CdcV2,
            Kind::DifferentialRow(()) => SourceEnvelope::DifferentialRow,
        })
    }
}

/// `UnplannedSourceEnvelope` is a `SourceEnvelope` missing some information. This information
/// is obtained in `UnplannedSourceEnvelope::desc`, where
/// `UnplannedSourceEnvelope::into_source_envelope`
/// creates a full `SourceEnvelope`
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UnplannedSourceEnvelope {
    None(KeyEnvelope),
    Debezium(DebeziumEnvelope),
    Upsert(UpsertStyle),
    CdcV2,
    /// An envelope for sources that directly read differential Rows. This is internal and
    /// cannot be requested via SQL.
    DifferentialRow,
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NoneEnvelope {
    pub key_envelope: KeyEnvelope,
    pub key_arity: usize,
}

impl RustType<ProtoNoneEnvelope> for NoneEnvelope {
    fn into_proto(&self) -> ProtoNoneEnvelope {
        ProtoNoneEnvelope {
            key_envelope: Some(self.key_envelope.into_proto()),
            key_arity: self.key_arity.into_proto(),
        }
    }

    fn from_proto(proto: ProtoNoneEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(NoneEnvelope {
            key_envelope: proto
                .key_envelope
                .into_rust_if_some("ProtoNoneEnvelope::key_envelope")?,
            key_arity: proto.key_arity.into_rust()?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UpsertEnvelope {
    /// What style of Upsert we are using
    pub style: UpsertStyle,
    /// The indices of the keys in the full value row, used
    /// to deduplicate data in `upsert_core`
    pub key_indices: Vec<usize>,
}

impl RustType<ProtoUpsertEnvelope> for UpsertEnvelope {
    fn into_proto(self: &Self) -> ProtoUpsertEnvelope {
        ProtoUpsertEnvelope {
            style: Some(self.style.into_proto()),
            key_indices: self.key_indices.into_proto(),
        }
    }

    fn from_proto(proto: ProtoUpsertEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(UpsertEnvelope {
            style: proto
                .style
                .into_rust_if_some("ProtoUpsertEnvelope::style")?,
            key_indices: proto.key_indices.into_rust()?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UpsertStyle {
    /// `ENVELOPE UPSERT`, where the key shape depends on the independent
    /// `KeyEnvelope`
    Default(KeyEnvelope),
    /// `ENVELOPE DEBEZIUM UPSERT`
    Debezium { after_idx: usize },
}

impl RustType<ProtoUpsertStyle> for UpsertStyle {
    fn into_proto(self: &Self) -> ProtoUpsertStyle {
        use proto_upsert_style::{Kind, ProtoDebezium};
        ProtoUpsertStyle {
            kind: Some(match self {
                UpsertStyle::Default(e) => Kind::Default(e.into_proto()),
                UpsertStyle::Debezium { after_idx } => Kind::Debezium(ProtoDebezium {
                    after_idx: after_idx.into_proto(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoUpsertStyle) -> Result<Self, TryFromProtoError> {
        use proto_upsert_style::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoUpsertStyle::kind"))?;
        Ok(match kind {
            Kind::Default(e) => UpsertStyle::Default(e.into_rust()?),
            Kind::Debezium(d) => UpsertStyle::Debezium {
                after_idx: d.after_idx.into_rust()?,
            },
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DebeziumEnvelope {
    /// The column index containing the `before` row
    pub before_idx: usize,
    /// The column index containing the `after` row
    pub after_idx: usize,
    pub mode: DebeziumMode,
}

impl RustType<ProtoDebeziumEnvelope> for DebeziumEnvelope {
    fn into_proto(self: &Self) -> ProtoDebeziumEnvelope {
        ProtoDebeziumEnvelope {
            before_idx: self.before_idx.into_proto(),
            after_idx: self.after_idx.into_proto(),
            mode: Some(self.mode.into_proto()),
        }
    }

    fn from_proto(proto: ProtoDebeziumEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(DebeziumEnvelope {
            before_idx: proto.before_idx.into_rust()?,
            after_idx: proto.after_idx.into_rust()?,
            mode: proto
                .mode
                .into_rust_if_some("ProtoDebeziumEnvelope::mode")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DebeziumTransactionMetadata {
    pub tx_metadata_global_id: GlobalId,
    pub tx_status_idx: usize,
    pub tx_transaction_id_idx: usize,
    pub tx_data_collections_idx: usize,
    pub tx_data_collections_data_collection_idx: usize,
    pub tx_data_collections_event_count_idx: usize,
    pub tx_data_collection_name: String,
    pub data_transaction_id_idx: usize,
}

impl RustType<ProtoDebeziumTransactionMetadata> for DebeziumTransactionMetadata {
    fn into_proto(self: &Self) -> ProtoDebeziumTransactionMetadata {
        ProtoDebeziumTransactionMetadata {
            tx_metadata_global_id: Some(self.tx_metadata_global_id.into_proto()),
            tx_status_idx: self.tx_status_idx.into_proto(),
            tx_transaction_id_idx: self.tx_transaction_id_idx.into_proto(),
            tx_data_collections_idx: self.tx_data_collections_idx.into_proto(),
            tx_data_collections_data_collection_idx: self
                .tx_data_collections_data_collection_idx
                .into_proto(),
            tx_data_collections_event_count_idx: self
                .tx_data_collections_event_count_idx
                .into_proto(),
            tx_data_collection_name: self.tx_data_collection_name.clone(),
            data_transaction_id_idx: self.data_transaction_id_idx.into_proto(),
        }
    }

    fn from_proto(proto: ProtoDebeziumTransactionMetadata) -> Result<Self, TryFromProtoError> {
        Ok(DebeziumTransactionMetadata {
            tx_metadata_global_id: proto
                .tx_metadata_global_id
                .into_rust_if_some("ProtoDebeziumTransactionMetadata::tx_metadata_global_id")?,
            tx_status_idx: proto.tx_status_idx.into_rust()?,
            tx_transaction_id_idx: proto.tx_transaction_id_idx.into_rust()?,
            tx_data_collections_idx: proto.tx_data_collections_idx.into_rust()?,
            tx_data_collections_data_collection_idx: proto
                .tx_data_collections_data_collection_idx
                .into_rust()?,
            tx_data_collections_event_count_idx: proto
                .tx_data_collections_event_count_idx
                .into_rust()?,
            tx_data_collection_name: proto.tx_data_collection_name,
            data_transaction_id_idx: proto.data_transaction_id_idx.into_rust()?,
        })
    }
}

/// Ordered means we can trust Debezium high water marks
///
/// In standard operation, Debezium should always emit messages in position order, but
/// messages may be duplicated.
///
/// For example, this is a legal stream of Debezium event positions:
///
/// ```text
/// 1 2 3 2
/// ```
///
/// Note that `2` appears twice, but the *first* time it appeared it appeared in order.
/// Any position below the highest-ever seen position is guaranteed to be a duplicate,
/// and can be ignored.
///
/// Now consider this stream:
///
/// ```text
/// 1 3 2
/// ```
///
/// In this case, `2` is sent *out* of order, and if it is ignored we will miss important
/// state.
///
/// It is possible for users to do things with multiple databases and multiple Debezium
/// instances pointing at the same Kafka topic that mean that the Debezium guarantees do
/// not hold, in which case we are required to track individual messages, instead of just
/// the highest-ever-seen message.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DebeziumMode {
    /// Do not perform any deduplication
    None,
    /// We can trust high water mark
    Ordered(DebeziumDedupProjection),
    /// We need to store some piece of state for every message
    Full(DebeziumDedupProjection),
    FullInRange {
        projection: DebeziumDedupProjection,
        pad_start: Option<NaiveDateTime>,
        start: NaiveDateTime,
        end: NaiveDateTime,
    },
}

impl Arbitrary for DebeziumMode {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(DebeziumMode::None),
            any::<DebeziumDedupProjection>().prop_map(DebeziumMode::Ordered),
            any::<DebeziumDedupProjection>().prop_map(DebeziumMode::Full),
            (
                any::<DebeziumDedupProjection>(),
                any::<bool>(),
                any_naive_datetime(),
                any_naive_datetime(),
                any_naive_datetime(),
            )
                .prop_map(|(projection, pad_start_option, pad_start, start, end)| {
                    DebeziumMode::FullInRange {
                        projection,
                        pad_start: if pad_start_option {
                            Some(pad_start)
                        } else {
                            None
                        },
                        start,
                        end,
                    }
                }),
        ]
        .boxed()
    }
}

impl RustType<ProtoDebeziumMode> for DebeziumMode {
    fn into_proto(self: &Self) -> ProtoDebeziumMode {
        use proto_debezium_mode::{Kind, ProtoFullInRange};
        ProtoDebeziumMode {
            kind: Some(match self {
                DebeziumMode::None => Kind::None(()),
                DebeziumMode::Ordered(o) => Kind::Ordered(o.into_proto()),
                DebeziumMode::Full(f) => Kind::Full(f.into_proto()),
                DebeziumMode::FullInRange {
                    projection,
                    pad_start,
                    start,
                    end,
                } => Kind::FullInRange(ProtoFullInRange {
                    projection: Some(projection.into_proto()),
                    pad_start: pad_start.into_proto(),
                    start: Some(start.into_proto()),
                    end: Some(end.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoDebeziumMode) -> Result<Self, TryFromProtoError> {
        use proto_debezium_mode::{Kind, ProtoFullInRange};
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDebeziumMode::kind"))?;
        Ok(match kind {
            Kind::None(()) => DebeziumMode::None,
            Kind::Ordered(o) => DebeziumMode::Ordered(o.into_rust()?),
            Kind::Full(o) => DebeziumMode::Full(o.into_rust()?),
            Kind::FullInRange(ProtoFullInRange {
                projection,
                pad_start,
                start,
                end,
            }) => DebeziumMode::FullInRange {
                projection: projection.into_rust_if_some("ProtoFullInRange::projection")?,
                pad_start: pad_start.into_rust()?,
                start: start.into_rust_if_some("ProtoFullInRange::start")?,
                end: end.into_rust_if_some("ProtoFullInRange::end")?,
            },
        })
    }
}

impl DebeziumMode {
    pub fn tx_metadata(&self) -> Option<&DebeziumTransactionMetadata> {
        match self {
            DebeziumMode::Ordered(DebeziumDedupProjection { tx_metadata, .. })
            | DebeziumMode::Full(DebeziumDedupProjection { tx_metadata, .. })
            | DebeziumMode::FullInRange {
                projection: DebeziumDedupProjection { tx_metadata, .. },
                ..
            } => tx_metadata.as_ref(),
            DebeziumMode::None => None,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DebeziumDedupProjection {
    /// The column index containing the debezium source metadata
    pub source_idx: usize,
    /// The record index of the `source.snapshot` field
    pub snapshot_idx: usize,
    /// The upstream database specific fields
    pub source_projection: DebeziumSourceProjection,
    /// The column index containing the debezium transaction metadata
    pub transaction_idx: usize,
    /// The record index of the `transaction.total_order` field
    pub total_order_idx: usize,
    pub tx_metadata: Option<DebeziumTransactionMetadata>,
}

impl RustType<ProtoDebeziumDedupProjection> for DebeziumDedupProjection {
    fn into_proto(self: &Self) -> ProtoDebeziumDedupProjection {
        ProtoDebeziumDedupProjection {
            source_idx: self.source_idx.into_proto(),
            snapshot_idx: self.snapshot_idx.into_proto(),
            source_projection: Some(self.source_projection.into_proto()),
            transaction_idx: self.transaction_idx.into_proto(),
            total_order_idx: self.total_order_idx.into_proto(),
            tx_metadata: self.tx_metadata.into_proto(),
        }
    }

    fn from_proto(proto: ProtoDebeziumDedupProjection) -> Result<Self, TryFromProtoError> {
        Ok(DebeziumDedupProjection {
            source_idx: proto.source_idx.into_rust()?,
            snapshot_idx: proto.snapshot_idx.into_rust()?,
            source_projection: proto
                .source_projection
                .into_rust_if_some("ProtoDebeziumDedupProjection::source_projection")?,
            transaction_idx: proto.transaction_idx.into_rust()?,
            total_order_idx: proto.total_order_idx.into_rust()?,
            tx_metadata: proto.tx_metadata.into_rust()?,
        })
    }
}

/// Debezium generates records that contain metadata about the upstream database. The structure of
/// this metadata depends on the type of connector used. This struct records the relevant indices
/// in the record, calculated during planning, so that the dataflow operator can unpack the
/// structure and extract the relevant information.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DebeziumSourceProjection {
    MySql {
        file: usize,
        pos: usize,
        row: usize,
    },
    Postgres {
        sequence: Option<usize>,
        lsn: usize,
    },
    SqlServer {
        change_lsn: usize,
        event_serial_no: usize,
    },
}

impl RustType<ProtoDebeziumSourceProjection> for DebeziumSourceProjection {
    fn into_proto(self: &Self) -> ProtoDebeziumSourceProjection {
        use proto_debezium_source_projection::{Kind, ProtoMySql, ProtoPostgres, ProtoSqlServer};
        ProtoDebeziumSourceProjection {
            kind: Some(match self {
                DebeziumSourceProjection::MySql { file, pos, row } => Kind::MySql(ProtoMySql {
                    file: file.into_proto(),
                    pos: pos.into_proto(),
                    row: row.into_proto(),
                }),
                DebeziumSourceProjection::Postgres { sequence, lsn } => {
                    Kind::Postgres(ProtoPostgres {
                        sequence: sequence.into_proto(),
                        lsn: lsn.into_proto(),
                    })
                }
                DebeziumSourceProjection::SqlServer {
                    change_lsn,
                    event_serial_no,
                } => Kind::SqlServer(ProtoSqlServer {
                    change_lsn: change_lsn.into_proto(),
                    event_serial_no: event_serial_no.into_proto(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoDebeziumSourceProjection) -> Result<Self, TryFromProtoError> {
        use proto_debezium_source_projection::{Kind, ProtoMySql, ProtoPostgres, ProtoSqlServer};
        let kind = proto.kind.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoDebeziumSourceProjection::kind")
        })?;
        Ok(match kind {
            Kind::MySql(ProtoMySql { file, pos, row }) => DebeziumSourceProjection::MySql {
                file: file.into_rust()?,
                pos: pos.into_rust()?,
                row: row.into_rust()?,
            },
            Kind::Postgres(ProtoPostgres { sequence, lsn }) => DebeziumSourceProjection::Postgres {
                sequence: sequence.into_rust()?,
                lsn: lsn.into_rust()?,
            },
            Kind::SqlServer(ProtoSqlServer {
                change_lsn,
                event_serial_no,
            }) => DebeziumSourceProjection::SqlServer {
                change_lsn: change_lsn.into_rust()?,
                event_serial_no: event_serial_no.into_rust()?,
            },
        })
    }
}

/// Computes the indices of the value's relation description that appear in the key.
///
/// Returns an error if it detects a common columns between the two relations that has the same
/// name but a different type, if a key column is missing from the value, and if the key relation
/// has a column with no name.
fn match_key_indices(
    key_desc: &RelationDesc,
    value_desc: &RelationDesc,
) -> anyhow::Result<Vec<usize>> {
    let mut indices = Vec::new();
    for (name, key_type) in key_desc.iter() {
        let (index, value_type) = value_desc
            .get_by_name(name)
            .ok_or_else(|| anyhow!("Value schema missing primary key column: {}", name))?;

        if key_type == value_type {
            indices.push(index);
        } else {
            bail!(
                "key and value column types do not match: key {:?} vs. value {:?}",
                key_type,
                value_type
            );
        }
    }
    Ok(indices)
}

impl UnplannedSourceEnvelope {
    /// Transforms an `UnplannedSourceEnvelope` into a `SourceEnvelope`
    ///
    /// Panics if the input envelope is `UnplannedSourceEnvelope::Upsert` and
    /// key is not passed as `Some`
    fn into_source_envelope(
        self,
        key: Option<Vec<usize>>,
        key_arity: Option<usize>,
    ) -> SourceEnvelope {
        match self {
            UnplannedSourceEnvelope::Upsert(upsert_style) => {
                SourceEnvelope::Upsert(UpsertEnvelope {
                    style: upsert_style,
                    key_indices: key.expect("into_source_envelope to be passed correct parameters for UnplannedSourceEnvelope::Upsert"),
                })
            },
            UnplannedSourceEnvelope::Debezium(inner) => {
                SourceEnvelope::Debezium(inner)
            }
            UnplannedSourceEnvelope::None(key_envelope) => SourceEnvelope::None(NoneEnvelope {
                key_envelope,
                key_arity: key_arity.unwrap_or(0),
            }),
            UnplannedSourceEnvelope::CdcV2 => SourceEnvelope::CdcV2,
            UnplannedSourceEnvelope::DifferentialRow => SourceEnvelope::DifferentialRow,
        }
    }

    /// Computes the output relation of this envelope when applied on top of the decoded key and
    /// value relation desc
    pub fn desc(
        self,
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        metadata_desc: RelationDesc,
    ) -> anyhow::Result<(SourceEnvelope, RelationDesc)> {
        Ok(match &self {
            UnplannedSourceEnvelope::None(key_envelope)
            | UnplannedSourceEnvelope::Upsert(UpsertStyle::Default(key_envelope)) => {
                let key_desc = match key_desc {
                    Some(desc) => desc,
                    None => {
                        return Ok((
                            self.into_source_envelope(None, None),
                            value_desc.concat(metadata_desc),
                        ))
                    }
                };
                let key_arity = key_desc.arity();

                let (keyed, key) = match key_envelope {
                    KeyEnvelope::None => (value_desc, None),
                    KeyEnvelope::Flattened => {
                        // Add the key columns as a key.
                        let key_indices: Vec<usize> = (0..key_desc.arity()).collect();
                        let key_desc = key_desc.with_key(key_indices.clone());
                        (key_desc.concat(value_desc), Some(key_indices))
                    }
                    KeyEnvelope::LegacyUpsert => {
                        let key_indices: Vec<usize> = (0..key_desc.arity()).collect();
                        let key_desc = key_desc.with_key(key_indices.clone());
                        let names = (0..key_desc.arity()).map(|i| format!("key{}", i));
                        // Rename key columns to "keyN"
                        (
                            key_desc.with_names(names).concat(value_desc),
                            Some(key_indices),
                        )
                    }
                    KeyEnvelope::Named(key_name) => {
                        let key_desc = {
                            // if the key has multiple objects, nest them as a record inside of a single name
                            if key_desc.arity() > 1 {
                                let key_type = key_desc.typ();
                                let key_as_record = RelationType::new(vec![ColumnType {
                                    nullable: false,
                                    scalar_type: ScalarType::Record {
                                        fields: key_desc
                                            .iter_names()
                                            .zip(key_type.column_types.iter())
                                            .map(|(name, ty)| (name.clone(), ty.clone()))
                                            .collect(),
                                        custom_id: None,
                                    },
                                }]);

                                RelationDesc::new(key_as_record, [key_name.to_string()])
                            } else {
                                key_desc.with_names([key_name.to_string()])
                            }
                        };
                        // In all cases the first column is the key
                        (key_desc.with_key(vec![0]).concat(value_desc), Some(vec![0]))
                    }
                };
                (
                    self.into_source_envelope(key, Some(key_arity)),
                    keyed.concat(metadata_desc),
                )
            }
            UnplannedSourceEnvelope::Debezium(DebeziumEnvelope { after_idx, .. })
            | UnplannedSourceEnvelope::Upsert(UpsertStyle::Debezium { after_idx }) => {
                match &value_desc.typ().column_types[*after_idx].scalar_type {
                    ScalarType::Record { fields, .. } => {
                        let mut desc = RelationDesc::from_names_and_types(fields.clone());
                        let key = key_desc.map(|k| match_key_indices(&k, &desc)).transpose()?;
                        if let Some(key) = key.clone() {
                            desc = desc.with_key(key);
                        }

                        let desc = match self {
                            UnplannedSourceEnvelope::Upsert(_) => desc.concat(metadata_desc),
                            _ => desc,
                        };

                        (self.into_source_envelope(key, None), desc)
                    }
                    ty => bail!(
                        "Incorrect type for Debezium value, expected Record, got {:?}",
                        ty
                    ),
                }
            }
            UnplannedSourceEnvelope::CdcV2 => {
                // the correct types

                // CdcV2 row data are in a record in a record in a list
                match &value_desc.typ().column_types[0].scalar_type {
                    ScalarType::List { element_type, .. } => match &**element_type {
                        ScalarType::Record { fields, .. } => {
                            // TODO maybe check this by name
                            match &fields[0].1.scalar_type {
                                ScalarType::Record { fields, .. } => (
                                    self.into_source_envelope(None, None),
                                    RelationDesc::from_names_and_types(fields.clone()),
                                ),
                                ty => {
                                    bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty)
                                }
                            }
                        }
                        ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                    },
                    ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                }
            }
            UnplannedSourceEnvelope::DifferentialRow => (
                self.into_source_envelope(None, None),
                value_desc.concat(metadata_desc),
            ),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnector {
    pub addrs: KafkaAddrs,
    pub topic: String,
    // Represents options specified by user when creating the source, e.g.
    // security settings.
    pub config_options: BTreeMap<String, String>,
    // Map from partition -> starting offset
    pub start_offsets: HashMap<i32, MzOffset>,
    pub group_id_prefix: Option<String>,
    pub cluster_id: Uuid,
    /// If present, include the timestamp as an output column of the source with the given name
    pub include_timestamp: Option<IncludedColumnPos>,
    /// If present, include the partition as an output column of the source with the given name.
    pub include_partition: Option<IncludedColumnPos>,
    /// If present, include the topic as an output column of the source with the given name.
    pub include_topic: Option<IncludedColumnPos>,
    /// If present, include the offset as an output column of the source with the given name.
    pub include_offset: Option<IncludedColumnPos>,
    pub include_headers: Option<IncludedColumnPos>,
}

impl Arbitrary for KafkaSourceConnector {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<KafkaAddrs>(),
            any::<String>(),
            any::<BTreeMap<String, String>>(),
            any::<HashMap<i32, MzOffset>>(),
            any::<Option<String>>(),
            any_uuid(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
        )
            .prop_map(
                |(
                    addrs,
                    topic,
                    config_options,
                    start_offsets,
                    group_id_prefix,
                    cluster_id,
                    include_timestamp,
                    include_partition,
                    include_topic,
                    include_offset,
                    include_headers,
                )| KafkaSourceConnector {
                    addrs,
                    topic,
                    config_options,
                    start_offsets,
                    group_id_prefix,
                    cluster_id,
                    include_timestamp,
                    include_partition,
                    include_topic,
                    include_offset,
                    include_headers,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoKafkaSourceConnector> for KafkaSourceConnector {
    fn into_proto(&self) -> ProtoKafkaSourceConnector {
        ProtoKafkaSourceConnector {
            addrs: Some((&self.addrs).into_proto()),
            topic: self.topic.clone(),
            config_options: self.config_options.clone().into_iter().collect(),
            start_offsets: self
                .start_offsets
                .iter()
                .map(|(k, v)| (*k, v.into_proto()))
                .collect(),
            group_id_prefix: self.group_id_prefix.clone(),
            cluster_id: Some(self.cluster_id.into_proto()),
            include_timestamp: self.include_timestamp.into_proto(),
            include_partition: self.include_partition.into_proto(),
            include_topic: self.include_topic.into_proto(),
            include_offset: self.include_offset.into_proto(),
            include_headers: self.include_headers.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaSourceConnector) -> Result<Self, TryFromProtoError> {
        let start_offsets: Result<_, TryFromProtoError> = proto
            .start_offsets
            .into_iter()
            .map(|(k, v)| MzOffset::from_proto(v).map(|v| (k, v)))
            .collect();
        Ok(KafkaSourceConnector {
            addrs: proto
                .addrs
                .into_rust_if_some("ProtoKafkaSourceConnector::addrs")?,
            topic: proto.topic,
            config_options: proto.config_options.into_iter().collect(),
            start_offsets: start_offsets?,
            group_id_prefix: proto.group_id_prefix,
            cluster_id: proto
                .cluster_id
                .into_rust_if_some("ProtoPostgresSourceConnector::details")?,
            include_timestamp: proto.include_timestamp.into_rust()?,
            include_partition: proto.include_partition.into_rust()?,
            include_topic: proto.include_topic.into_rust()?,
            include_offset: proto.include_offset.into_rust()?,
            include_headers: proto.include_headers.into_rust()?,
        })
    }
}

/// Legacy logic included something like an offset into almost data streams
///
/// Eventually we will require `INCLUDE <metadata>` for everything.
pub fn provide_default_metadata(
    envelope: &UnplannedSourceEnvelope,
    encoding: &encoding::DataEncoding,
) -> bool {
    let is_avro = matches!(encoding, encoding::DataEncoding::Avro(_));
    let is_stateless_dbz = match envelope {
        UnplannedSourceEnvelope::Debezium(_) => true,
        _ => false,
    };

    !is_avro && !is_stateless_dbz
}

#[derive(Arbitrary, Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Compression {
    Gzip,
    None,
}

impl RustType<ProtoCompression> for Compression {
    fn into_proto(&self) -> ProtoCompression {
        use proto_compression::Kind;
        ProtoCompression {
            kind: Some(match self {
                Compression::Gzip => Kind::Gzip(()),
                Compression::None => Kind::None(()),
            }),
        }
    }

    fn from_proto(proto: ProtoCompression) -> Result<Self, TryFromProtoError> {
        use proto_compression::Kind;
        Ok(match proto.kind {
            Some(Kind::Gzip(())) => Compression::Gzip,
            Some(Kind::None(())) => Compression::None,
            None => {
                return Err(TryFromProtoError::MissingField(
                    "ProtoCompression::kind".into(),
                ))
            }
        })
    }
}

/// A source of updates for a relational collection.
///
/// A source contains enough information to instantiate a stream of changes,
/// as well as related metadata about the columns, their types, and properties
/// of the collection.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceDesc {
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

impl RustType<ProtoSourceDesc> for SourceDesc {
    fn into_proto(self: &Self) -> ProtoSourceDesc {
        ProtoSourceDesc {
            connector: Some(self.connector.into_proto()),
            desc: Some(self.desc.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceDesc {
            connector: proto
                .connector
                .into_rust_if_some("ProtoSourceDesc::connector")?,
            desc: proto.desc.into_rust_if_some("ProtoSourceDesc::desc")?,
        })
    }
}

/// A `SourceConnector` describes how data is produced for a source, be
/// it from a local table, or some upstream service. It is the first
/// step of _rendering_ of a source, and describes only how to produce
/// a stream of messages associated with MzOffset's.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SourceConnector {
    External {
        connector: ExternalSourceConnector,
        encoding: encoding::SourceDataEncoding,
        envelope: SourceEnvelope,
        metadata_columns: Vec<IncludedColumnSource>,
        ts_frequency: Duration,
        timeline: Timeline,
    },

    /// A local "source" is fed by a local input handle.
    Local { timeline: Timeline },
}

impl Arbitrary for SourceConnector {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            (
                any::<ExternalSourceConnector>(),
                any::<encoding::SourceDataEncoding>(),
                any::<SourceEnvelope>(),
                any::<Vec<IncludedColumnSource>>(),
                any_duration(),
                any::<Timeline>(),
            )
                .prop_map(
                    |(connector, encoding, envelope, metadata_columns, ts_frequency, timeline)| {
                        SourceConnector::External {
                            connector,
                            encoding,
                            envelope,
                            metadata_columns,
                            ts_frequency,
                            timeline,
                        }
                    }
                ),
            any::<Timeline>().prop_map(|timeline| SourceConnector::Local { timeline }),
        ]
        .boxed()
    }
}

impl RustType<ProtoSourceConnector> for SourceConnector {
    fn into_proto(self: &Self) -> ProtoSourceConnector {
        use proto_source_connector::{Kind, ProtoExternal, ProtoLocal};
        ProtoSourceConnector {
            kind: Some(match self {
                SourceConnector::External {
                    connector,
                    encoding,
                    envelope,
                    metadata_columns,
                    ts_frequency,
                    timeline,
                } => Kind::External(ProtoExternal {
                    connector: Some(connector.into_proto()),
                    encoding: Some(encoding.into_proto()),
                    envelope: Some(envelope.into_proto()),
                    metadata_columns: metadata_columns.into_proto(),
                    ts_frequency: Some(ts_frequency.into_proto()),
                    timeline: Some(timeline.into_proto()),
                }),
                SourceConnector::Local { timeline } => Kind::Local(ProtoLocal {
                    timeline: Some(timeline.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceConnector) -> Result<Self, TryFromProtoError> {
        use proto_source_connector::{Kind, ProtoExternal, ProtoLocal};
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSourceConnector::kind"))?;
        Ok(match kind {
            Kind::External(ProtoExternal {
                connector,
                encoding,
                envelope,
                metadata_columns,
                ts_frequency,
                timeline,
            }) => SourceConnector::External {
                connector: connector.into_rust_if_some("ProtoExternal::connector")?,
                encoding: encoding.into_rust_if_some("ProtoExternal::encoding")?,
                envelope: envelope.into_rust_if_some("ProtoExternal::envelope")?,
                metadata_columns: metadata_columns.into_rust()?,
                ts_frequency: ts_frequency.into_rust_if_some("ProtoExternal::ts_frequency")?,
                timeline: timeline.into_rust_if_some("ProtoExternal::timeline")?,
            },
            Kind::Local(ProtoLocal { timeline }) => SourceConnector::Local {
                timeline: timeline.into_rust_if_some("ProtoLocal::timeline")?,
            },
        })
    }
}

impl SourceConnector {
    /// Returns `true` if this connector yields input data (including
    /// timestamps) that is stable across restarts. This is important for
    /// exactly-once Sinks that need to ensure that the same data is written,
    /// even when failures/restarts happen.
    pub fn yields_stable_input(&self) -> bool {
        if let SourceConnector::External { connector, .. } = self {
            // Conservatively, set all Kafka/File sources as having stable inputs because
            // we know they will be read in a known, repeatable offset order (modulo compaction for some Kafka sources).
            match connector {
                // TODO(guswynn): does postgres count here as well?
                ExternalSourceConnector::Kafka(_) => true,
                // Currently, the Kinesis connector assigns "offsets" by counting the message in the order it was received
                // and this order is not replayable across different reads of the same Kinesis stream.
                ExternalSourceConnector::Kinesis(_) => false,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Returns `true` if this connector yields data that is
    /// append-only/monotonic. Append-monly means the source
    /// never produces retractions.
    // TODO(guswynn): consider enforcing this more completely at the
    // parsing/typechecking level, by not using an `envelope`
    // for sources like pg
    pub fn append_only(&self) -> bool {
        match self {
            // Postgres can produce retractions (deletes)
            SourceConnector::External {
                connector: ExternalSourceConnector::Postgres(_),
                ..
            } => false,
            // Other sources the `None` envelope are append-only.
            SourceConnector::External {
                envelope: SourceEnvelope::None(_),
                ..
            } => true,
            // Other combinations may produce retractions.
            SourceConnector::External {
                envelope:
                    SourceEnvelope::Debezium(_)
                    | SourceEnvelope::Upsert(_)
                    | SourceEnvelope::CdcV2
                    | SourceEnvelope::DifferentialRow,
                connector:
                    ExternalSourceConnector::S3(_)
                    | ExternalSourceConnector::Kafka(_)
                    | ExternalSourceConnector::Kinesis(_)
                    | ExternalSourceConnector::PubNub(_),
                ..
            } => false,
            // Local sources (i.e., tables) also support retractions (deletes)
            SourceConnector::Local { .. } => false,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            SourceConnector::External { connector, .. } => connector.name(),
            SourceConnector::Local { .. } => "local",
        }
    }

    pub fn timeline(&self) -> Timeline {
        match self {
            SourceConnector::External { timeline, .. } => timeline.clone(),
            SourceConnector::Local { timeline, .. } => timeline.clone(),
        }
    }

    pub fn requires_single_materialization(&self) -> bool {
        if let SourceConnector::External { connector, .. } = self {
            connector.requires_single_materialization()
        } else {
            false
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExternalSourceConnector {
    Kafka(KafkaSourceConnector),
    Kinesis(KinesisSourceConnector),
    S3(S3SourceConnector),
    Postgres(PostgresSourceConnector),
    PubNub(PubNubSourceConnector),
}

impl RustType<ProtoExternalSourceConnector> for ExternalSourceConnector {
    fn into_proto(&self) -> ProtoExternalSourceConnector {
        use proto_external_source_connector::Kind;
        ProtoExternalSourceConnector {
            kind: Some(match self {
                ExternalSourceConnector::Kafka(kafka) => Kind::Kafka(kafka.into_proto()),
                ExternalSourceConnector::Kinesis(kinesis) => Kind::Kinesis(kinesis.into_proto()),
                ExternalSourceConnector::S3(s3) => Kind::S3(s3.into_proto()),
                ExternalSourceConnector::Postgres(postgres) => {
                    Kind::Postgres(postgres.into_proto())
                }
                ExternalSourceConnector::PubNub(pubnub) => Kind::Pubnub(pubnub.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoExternalSourceConnector) -> Result<Self, TryFromProtoError> {
        use proto_external_source_connector::Kind;
        let kind = proto.kind.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoExternalSourceConnector::kind")
        })?;
        Ok(match kind {
            Kind::Kafka(kafka) => ExternalSourceConnector::Kafka(kafka.into_rust()?),
            Kind::Kinesis(kinesis) => ExternalSourceConnector::Kinesis(kinesis.into_rust()?),
            Kind::S3(s3) => ExternalSourceConnector::S3(s3.into_rust()?),
            Kind::Postgres(postgres) => ExternalSourceConnector::Postgres(postgres.into_rust()?),
            Kind::Pubnub(pubnub) => ExternalSourceConnector::PubNub(pubnub.into_rust()?),
        })
    }
}

impl ExternalSourceConnector {
    /// Returns the name and type of each additional metadata column that
    /// Materialize will automatically append to the source's inherent columns.
    ///
    /// Presently, each source type exposes precisely one metadata column that
    /// corresponds to some source-specific record counter. For example, file
    /// sources use a line number, while Kafka sources use a topic offset.
    ///
    /// The columns declared here must be kept in sync with the actual source
    /// implementations that produce these columns.
    pub fn metadata_columns(&self, include_defaults: bool) -> Vec<(&str, ColumnType)> {
        let mut columns = Vec::new();
        let default_col = |name| (name, ScalarType::Int64.nullable(false));
        match self {
            Self::Kafka(KafkaSourceConnector {
                include_partition: part,
                include_timestamp: time,
                include_topic: topic,
                include_offset: offset,
                include_headers: headers,
                ..
            }) => {
                let mut items = BTreeMap::new();
                // put the offset at the end if necessary
                if include_defaults && offset.is_none() {
                    items.insert(4, default_col("mz_offset"));
                }

                for (include, ty) in [
                    (offset, ScalarType::Int64),
                    (part, ScalarType::Int32),
                    (time, ScalarType::Timestamp),
                    (topic, ScalarType::String),
                    (
                        headers,
                        ScalarType::List {
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
                        },
                    ),
                ] {
                    if let Some(include) = include {
                        items.insert(include.pos + 1, (&include.name, ty.nullable(false)));
                    }
                }

                items.into_values().collect()
            }
            Self::Kinesis(_) => {
                if include_defaults {
                    columns.push(default_col("mz_offset"))
                };
                columns
            }
            // TODO: should we include object key and possibly object-internal offset here?
            Self::S3(_) => {
                if include_defaults {
                    columns.push(default_col("mz_record"))
                };
                columns
            }
            Self::Postgres(_) => vec![],
            Self::PubNub(_) => vec![],
        }
    }

    // TODO(bwm): get rid of this when we no longer have the notion of default metadata
    pub fn default_metadata_column_name(&self) -> Option<&str> {
        match self {
            ExternalSourceConnector::Kafka(_) => Some("mz_offset"),
            ExternalSourceConnector::Kinesis(_) => Some("mz_offset"),
            ExternalSourceConnector::S3(_) => Some("mz_record"),
            ExternalSourceConnector::Postgres(_) => None,
            ExternalSourceConnector::PubNub(_) => None,
        }
    }

    pub fn metadata_column_types(&self, include_defaults: bool) -> Vec<IncludedColumnSource> {
        match self {
            ExternalSourceConnector::Kafka(KafkaSourceConnector {
                include_partition: part,
                include_timestamp: time,
                include_topic: topic,
                include_offset: offset,
                include_headers: headers,
                ..
            }) => {
                // create a sorted list of column types based on the order they were declared in sql
                // TODO: should key be included in the sorted list? Breaking change, and it's
                // already special (it commonly multiple columns embedded in it).
                let mut items = BTreeMap::new();
                if include_defaults && offset.is_none() {
                    items.insert(4, IncludedColumnSource::DefaultPosition);
                }
                for (include, ty) in [
                    (offset, IncludedColumnSource::Offset),
                    (part, IncludedColumnSource::Partition),
                    (time, IncludedColumnSource::Timestamp),
                    (topic, IncludedColumnSource::Topic),
                    (headers, IncludedColumnSource::Headers),
                ] {
                    if let Some(include) = include {
                        items.insert(include.pos, ty);
                    }
                }

                items.into_values().collect()
            }

            ExternalSourceConnector::Kinesis(_) | ExternalSourceConnector::S3(_) => {
                if include_defaults {
                    vec![IncludedColumnSource::DefaultPosition]
                } else {
                    Vec::new()
                }
            }
            ExternalSourceConnector::Postgres(_) | ExternalSourceConnector::PubNub(_) => Vec::new(),
        }
    }

    /// Returns the name of the external source connector.
    pub fn name(&self) -> &'static str {
        match self {
            ExternalSourceConnector::Kafka(_) => "kafka",
            ExternalSourceConnector::Kinesis(_) => "kinesis",
            ExternalSourceConnector::S3(_) => "s3",
            ExternalSourceConnector::Postgres(_) => "postgres",
            ExternalSourceConnector::PubNub(_) => "pubnub",
        }
    }

    /// Optionally returns the name of the upstream resource this source corresponds to.
    /// (Currently only implemented for Kafka and Kinesis, to match old-style behavior
    ///  TODO: decide whether we want file paths and other upstream names to show up in metrics too.
    pub fn upstream_name(&self) -> Option<&str> {
        match self {
            ExternalSourceConnector::Kafka(KafkaSourceConnector { topic, .. }) => {
                Some(topic.as_str())
            }
            ExternalSourceConnector::Kinesis(KinesisSourceConnector { stream_name, .. }) => {
                Some(stream_name.as_str())
            }
            ExternalSourceConnector::S3(_) => None,
            ExternalSourceConnector::Postgres(_) => None,
            ExternalSourceConnector::PubNub(_) => None,
        }
    }

    pub fn requires_single_materialization(&self) -> bool {
        match self {
            ExternalSourceConnector::S3(c) => c.requires_single_materialization(),

            ExternalSourceConnector::Kafka(_)
            | ExternalSourceConnector::Kinesis(_)
            | ExternalSourceConnector::Postgres(_)
            | ExternalSourceConnector::PubNub(_) => false,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceConnector {
    pub stream_name: String,
    pub aws: AwsConfig,
}

impl RustType<ProtoKinesisSourceConnector> for KinesisSourceConnector {
    fn into_proto(&self) -> ProtoKinesisSourceConnector {
        ProtoKinesisSourceConnector {
            stream_name: self.stream_name.clone(),
            aws: Some(self.aws.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKinesisSourceConnector) -> Result<Self, TryFromProtoError> {
        Ok(KinesisSourceConnector {
            stream_name: proto.stream_name,
            aws: proto
                .aws
                .into_rust_if_some("ProtoKinesisSourceConnector::aws")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSourceConnector {
    pub conn: String,
    pub publication: String,
    pub details: PostgresSourceDetails,
}

impl RustType<ProtoPostgresSourceConnector> for PostgresSourceConnector {
    fn into_proto(&self) -> ProtoPostgresSourceConnector {
        ProtoPostgresSourceConnector {
            conn: self.conn.clone(),
            publication: self.publication.clone(),
            details: Some(self.details.into_proto()),
        }
    }

    fn from_proto(proto: ProtoPostgresSourceConnector) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourceConnector {
            conn: proto.conn,
            publication: proto.publication,
            details: proto
                .details
                .into_rust_if_some("ProtoPostgresSourceConnector::details")?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSourceDetails {
    pub tables: Vec<mz_postgres_util::desc::PostgresTableDesc>,
    pub slot: String,
}

impl RustType<ProtoPostgresSourceDetails> for PostgresSourceDetails {
    fn into_proto(&self) -> ProtoPostgresSourceDetails {
        ProtoPostgresSourceDetails {
            tables: self.tables.iter().map(|t| t.into_proto()).collect(),
            slot: self.slot.clone(),
        }
    }

    fn from_proto(proto: ProtoPostgresSourceDetails) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourceDetails {
            tables: proto
                .tables
                .into_iter()
                .map(mz_postgres_util::desc::PostgresTableDesc::from_proto)
                .collect::<Result<_, _>>()?,
            slot: proto.slot,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PubNubSourceConnector {
    pub subscribe_key: String,
    pub channel: String,
}

impl RustType<ProtoPubNubSourceConnector> for PubNubSourceConnector {
    fn into_proto(&self) -> ProtoPubNubSourceConnector {
        ProtoPubNubSourceConnector {
            subscribe_key: self.subscribe_key.clone(),
            channel: self.channel.clone(),
        }
    }

    fn from_proto(proto: ProtoPubNubSourceConnector) -> Result<Self, TryFromProtoError> {
        Ok(PubNubSourceConnector {
            subscribe_key: proto.subscribe_key,
            channel: proto.channel,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3SourceConnector {
    pub key_sources: Vec<S3KeySource>,
    pub pattern: Option<Glob>,
    pub aws: AwsConfig,
    pub compression: Compression,
}

fn any_glob() -> impl Strategy<Value = Glob> {
    r"[a-z][a-z0-9]{0,10}/?([a-z0-9]{0,5}/?){0,3}".prop_map(|s| {
        GlobBuilder::new(&s)
            .literal_separator(true)
            .backslash_escape(true)
            .build()
            .unwrap()
    })
}

impl Arbitrary for S3SourceConnector {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<Vec<S3KeySource>>(),
            proptest::option::of(any_glob()),
            any::<AwsConfig>(),
            any::<Compression>(),
        )
            .prop_map(
                |(key_sources, pattern, aws, compression)| S3SourceConnector {
                    key_sources,
                    pattern,
                    aws,
                    compression,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoS3SourceConnector> for S3SourceConnector {
    fn into_proto(&self) -> ProtoS3SourceConnector {
        ProtoS3SourceConnector {
            key_sources: self.key_sources.into_proto(),
            pattern: self.pattern.as_ref().map(|g| g.glob().into()),
            aws: Some(self.aws.into_proto()),
            compression: Some(self.compression.into_proto()),
        }
    }

    fn from_proto(proto: ProtoS3SourceConnector) -> Result<Self, TryFromProtoError> {
        Ok(S3SourceConnector {
            key_sources: proto.key_sources.into_rust()?,
            pattern: proto
                .pattern
                .map(|p| {
                    GlobBuilder::new(&p)
                        .literal_separator(true)
                        .backslash_escape(true)
                        .build()
                })
                .transpose()?,
            aws: proto.aws.into_rust_if_some("ProtoS3SourceConnector::aws")?,
            compression: proto
                .compression
                .into_rust_if_some("ProtoS3SourceConnector::compression")?,
        })
    }
}

impl S3SourceConnector {
    fn requires_single_materialization(&self) -> bool {
        // SQS Notifications are not durable, multiple sources depending on them will get
        // non-intersecting subsets of objects to read
        self.key_sources
            .iter()
            .any(|s| matches!(s, S3KeySource::SqsNotifications { .. }))
    }
}

/// A Source of Object Key names, the argument of the `DISCOVER OBJECTS` clause
#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum S3KeySource {
    /// Scan the S3 Bucket to discover keys to download
    Scan { bucket: String },
    /// Load object keys based on the contents of an S3 Notifications channel
    ///
    /// S3 notifications channels can be configured to go to SQS, which is the
    /// only target we currently support.
    SqsNotifications { queue: String },
}

impl RustType<ProtoS3KeySource> for S3KeySource {
    fn into_proto(&self) -> ProtoS3KeySource {
        use proto_s3_key_source::Kind;
        ProtoS3KeySource {
            kind: Some(match self {
                S3KeySource::Scan { bucket } => Kind::Scan(bucket.clone()),
                S3KeySource::SqsNotifications { queue } => Kind::SqsNotifications(queue.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoS3KeySource) -> Result<Self, TryFromProtoError> {
        use proto_s3_key_source::Kind;
        Ok(match proto.kind {
            Some(Kind::Scan(s)) => S3KeySource::Scan { bucket: s },
            Some(Kind::SqsNotifications(s)) => S3KeySource::SqsNotifications { queue: s },
            None => {
                return Err(TryFromProtoError::MissingField(
                    "ProtoS3KeySource::kind".into(),
                ))
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct SourceData(pub Result<Row, DataflowError>);

impl Deref for SourceData {
    type Target = Result<Row, DataflowError>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SourceData {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl RustType<ProtoSourceData> for SourceData {
    fn into_proto(&self) -> ProtoSourceData {
        use proto_source_data::Kind;
        ProtoSourceData {
            kind: Some(match &**self {
                Ok(row) => Kind::Ok(row.into_proto()),
                Err(err) => Kind::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceData) -> Result<Self, TryFromProtoError> {
        use proto_source_data::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Ok(row) => Ok(SourceData(Ok(row.into_rust()?))),
                Kind::Err(err) => Ok(SourceData(Err(err.into_rust()?))),
            },
            None => Result::Err(TryFromProtoError::missing_field("ProtoSourceData::kind")),
        }
    }
}

impl Codec for SourceData {
    fn codec_name() -> String {
        "protobuf[SourceData]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoSourceData::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}
