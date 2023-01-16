// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to the introduction of changing collections into `dataflow`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::BufMut;
use dec::OrderedDecimal;
use differential_dataflow::lattice::Lattice;
use globset::{Glob, GlobBuilder};
use itertools::Itertools;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};
use timely::scheduling::ActivateOnDrop;
use uuid::Uuid;

use mz_expr::{MirScalarExpr, PartitionId};
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::{Codec, Codec64};
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::{ColumnType, Datum, Diff, GlobalId, RelationDesc, RelationType, Row, ScalarType};
use mz_timely_util::order::{Interval, Partitioned, RangeBound};

use crate::controller::{CollectionMetadata, ResumptionFrontierCalculator};
use crate::types::connections::aws::AwsConfig;
use crate::types::connections::{KafkaConnection, PostgresConnection};
use crate::types::errors::DataflowError;
use crate::types::hosts::StorageHostConfig;
use crate::util::antichain::OffsetAntichain;

use self::encoding::{DataEncoding, DataEncodingInner, SourceDataEncoding};
use proto_ingestion_description::{ProtoSourceExport, ProtoSourceImport};
use proto_load_generator_source_connection::Generator as ProtoGenerator;

pub mod encoding;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_client.types.sources.rs"
));

/// A description of a source ingestion
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IngestionDescription<S = ()> {
    /// The source description
    pub desc: SourceDesc,
    /// Source collections made available to this ingestion.
    pub source_imports: BTreeMap<GlobalId, S>,
    /// Additional storage controller metadata needed to ingest this source
    pub ingestion_metadata: S,
    /// Collections to be exported by this ingestion.
    pub source_exports: BTreeMap<GlobalId, SourceExport<S>>,
    /// The address of a `clusterd` process on which to install the source.
    pub host_config: StorageHostConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceExport<S = ()> {
    /// The index of the exported output stream
    pub output_index: usize,
    /// The collection metadata needed to write the exported data
    pub storage_metadata: S,
}

#[async_trait]
impl<T: Timestamp + Lattice + Codec64> ResumptionFrontierCalculator<T>
    for IngestionDescription<CollectionMetadata>
{
    // A `WriteHandle` per used shard. Once we have source envelopes that keep additional shards we
    // have to specialize this some more.
    type State = Vec<WriteHandle<SourceData, (), T, Diff>>;

    async fn initialize_state(&self, client_cache: &mut PersistClientCache) -> Self::State {
        let mut handles = vec![];
        for (id, export) in self.source_exports.iter() {
            // Explicit destructuring to force a compile error when the metadata change
            let CollectionMetadata {
                persist_location,
                remap_shard: _,
                data_shard,
                // The status shard only contains non-definite status updates
                status_shard: _,
            } = &export.storage_metadata;
            let handle = client_cache
                .open(persist_location.clone())
                .await
                .expect("error creating persist client")
                .open_writer::<SourceData, (), T, Diff>(
                    *data_shard,
                    &format!("resumption data {}", id),
                )
                .await
                .unwrap();
            handles.push(handle);
        }

        let CollectionMetadata {
            persist_location,
            remap_shard,
            data_shard: _,
            // The status shard only contains non-definite status updates
            status_shard: _,
        } = &self.ingestion_metadata;
        let remap_handle = client_cache
            .open(persist_location.clone())
            .await
            .expect("error creating persist client")
            // TODO: Any way to plumb the GlobalId to this?
            .open_writer::<SourceData, (), T, Diff>(*remap_shard, "resumption remap")
            .await
            .unwrap();
        handles.push(remap_handle);

        handles
    }

    async fn calculate_resumption_frontier(&self, handles: &mut Self::State) -> Antichain<T> {
        // An ingestion can resume at the minimum of..
        let mut resume_upper = Antichain::new();

        // ..the upper frontier of each shard
        for handle in handles {
            handle.fetch_recent_upper().await;
            for t in handle.upper().elements() {
                resume_upper.insert(t.clone());
            }
        }

        // ..the upper of an implied envelope state shard. Eventually this could become actual
        // state shards and this section will be removed.
        let envelope_upper = match self.desc.envelope {
            // We can only resume with the None envelope, which is stateless,
            // or with the [Debezium] Upsert envelope, which is easy
            //   (re-ingest the last emitted state)
            SourceEnvelope::None(_) | SourceEnvelope::Upsert(_) => Antichain::new(),
            // Otherwise re-ingest everything
            _ => Antichain::from_elem(T::minimum()),
        };
        for t in envelope_upper {
            resume_upper.insert(t);
        }

        resume_upper
    }
}

impl<S> Arbitrary for IngestionDescription<S>
where
    S: Arbitrary + 'static,
{
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<SourceDesc>().boxed(),
            proptest::collection::btree_map(any::<GlobalId>(), any::<S>(), 1..4).boxed(),
            proptest::collection::btree_map(any::<GlobalId>(), any::<SourceExport<S>>(), 1..4)
                .boxed(),
            any::<S>().boxed(),
            any::<StorageHostConfig>().boxed(),
        )
            .prop_map(
                |(desc, source_imports, source_exports, ingestion_metadata, host_config)| Self {
                    desc,
                    source_imports,
                    source_exports,
                    ingestion_metadata,
                    host_config,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoIngestionDescription> for IngestionDescription<CollectionMetadata> {
    fn into_proto(&self) -> ProtoIngestionDescription {
        ProtoIngestionDescription {
            source_imports: self.source_imports.into_proto(),
            source_exports: self.source_exports.into_proto(),
            ingestion_metadata: Some(self.ingestion_metadata.into_proto()),
            desc: Some(self.desc.into_proto()),
            host_config: Some(self.host_config.into_proto()),
        }
    }

    fn from_proto(proto: ProtoIngestionDescription) -> Result<Self, TryFromProtoError> {
        Ok(IngestionDescription {
            source_imports: proto.source_imports.into_rust()?,
            source_exports: proto.source_exports.into_rust()?,
            desc: proto
                .desc
                .into_rust_if_some("ProtoIngestionDescription::desc")?,
            ingestion_metadata: proto
                .ingestion_metadata
                .into_rust_if_some("ProtoIngestionDescription::ingestion_metadata")?,
            host_config: proto
                .host_config
                .into_rust_if_some("ProtoIngestionDescription::host_config")?,
        })
    }
}

impl ProtoMapEntry<GlobalId, CollectionMetadata> for ProtoSourceImport {
    fn from_rust<'a>(entry: (&'a GlobalId, &'a CollectionMetadata)) -> Self {
        ProtoSourceImport {
            id: Some(entry.0.into_proto()),
            storage_metadata: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, CollectionMetadata), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSourceImport::id")?,
            self.storage_metadata
                .into_rust_if_some("ProtoSourceImport::storage_metadata")?,
        ))
    }
}

impl ProtoMapEntry<GlobalId, SourceExport<CollectionMetadata>> for ProtoSourceExport {
    fn from_rust<'a>(entry: (&'a GlobalId, &'a SourceExport<CollectionMetadata>)) -> Self {
        ProtoSourceExport {
            id: Some(entry.0.into_proto()),
            output_index: entry.1.output_index.into_proto(),
            storage_metadata: Some(entry.1.storage_metadata.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, SourceExport<CollectionMetadata>), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSourceExport::id")?,
            SourceExport {
                output_index: self.output_index.into_rust()?,
                storage_metadata: self
                    .storage_metadata
                    .into_rust_if_some("ProtoSourceExport::storage_metadata")?,
            },
        ))
    }
}

impl<S> Arbitrary for SourceExport<S>
where
    S: Arbitrary + 'static,
{
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (any::<usize>(), any::<S>())
            .prop_map(|(output_index, storage_metadata)| Self {
                output_index,
                storage_metadata,
            })
            .boxed()
    }
}

pub trait SourceTimestamp: timely::progress::Timestamp + Refines<()> + std::fmt::Display {
    fn from_compat_ts(pid: PartitionId, offset: MzOffset) -> Self;
    fn from_compat_frontier(frontier: OffsetAntichain) -> Antichain<Self>;
    fn into_compat_frontier(frontier: AntichainRef<'_, Self>) -> OffsetAntichain;
    fn encode_row(&self) -> Row;
    fn decode_row(row: &Row) -> Self;
}

impl SourceTimestamp for MzOffset {
    fn from_compat_ts(pid: PartitionId, offset: MzOffset) -> Self {
        assert_eq!(
            pid,
            PartitionId::None,
            "invalid non-partitioned partition {pid}"
        );
        offset
    }

    fn from_compat_frontier(frontier: OffsetAntichain) -> Antichain<Self> {
        let mut times = frontier.iter();
        match (times.next(), times.next()) {
            (Some((PartitionId::None, offset)), None) => Antichain::from_elem(*offset),
            (None, None) => Antichain::from_elem(Self::minimum()),
            _ => panic!("invalid non-partitioned compat frontier: {frontier:?}"),
        }
    }

    fn into_compat_frontier(frontier: AntichainRef<'_, Self>) -> OffsetAntichain {
        let mut ret = OffsetAntichain::new();
        if let Some(offset) = frontier.as_option() {
            if offset.offset > 0 {
                ret.insert(PartitionId::None, *offset);
            }
        }
        ret
    }

    fn encode_row(&self) -> Row {
        Row::pack([Datum::UInt64(self.offset)])
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::UInt64(offset)), None) => MzOffset::from(offset),
            _ => panic!("invalid row {row:?}"),
        }
    }
}

impl SourceTimestamp for Partitioned<i32, MzOffset> {
    fn from_compat_ts(pid: PartitionId, offset: MzOffset) -> Self {
        match pid {
            PartitionId::Kafka(pid) => Partitioned::with_partition(pid, offset),
            PartitionId::None => panic!("invalid partitioned partition {pid}"),
        }
    }

    fn from_compat_frontier(frontier: OffsetAntichain) -> Antichain<Self> {
        frontier.into()
    }

    fn into_compat_frontier(frontier: AntichainRef<'_, Self>) -> OffsetAntichain {
        frontier.to_owned().into()
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

impl differential_dataflow::difference::Semigroup for MzOffset {
    fn plus_equals(&mut self, rhs: &Self) {
        self.offset.plus_equals(&rhs.offset)
    }
    fn is_zero(&self) -> bool {
        self.offset.is_zero()
    }
}

impl mz_persist_types::Codec64 for MzOffset {
    fn codec_name() -> String {
        "MzOffset".to_string()
    }

    fn encode(&self) -> [u8; 8] {
        mz_persist_types::Codec64::encode(&self.offset)
    }

    fn decode(buf: [u8; 8]) -> Self {
        Self {
            offset: mz_persist_types::Codec64::decode(buf),
        }
    }
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

/// Convert from `PgLsn` to MzOffset
impl From<tokio_postgres::types::PgLsn> for MzOffset {
    fn from(lsn: tokio_postgres::types::PgLsn) -> Self {
        MzOffset { offset: lsn.into() }
    }
}

impl Timestamp for MzOffset {
    type Summary = MzOffset;

    fn minimum() -> Self {
        MzOffset {
            offset: Timestamp::minimum(),
        }
    }
}

impl PathSummary<MzOffset> for MzOffset {
    fn results_in(&self, src: &MzOffset) -> Option<MzOffset> {
        Some(MzOffset {
            offset: self.offset.results_in(&src.offset)?,
        })
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        Some(MzOffset {
            offset: PathSummary::<u64>::followed_by(&self.offset, &other.offset)?,
        })
    }
}

impl Refines<()> for MzOffset {
    fn to_inner(_: ()) -> Self {
        MzOffset::minimum()
    }
    fn to_outer(self) {}
    fn summarize(_: Self::Summary) {}
}

impl PartialOrder for MzOffset {
    fn less_equal(&self, other: &Self) -> bool {
        self.offset.less_equal(&other.offset)
    }
}

impl TotalOrder for MzOffset {}

/// Which piece of metadata a column corresponds to
#[derive(Arbitrary, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IncludedColumnSource {
    Partition,
    Offset,
    Timestamp,
    Topic,
    Headers,
}

impl RustType<ProtoIncludedColumnSource> for IncludedColumnSource {
    fn into_proto(&self) -> ProtoIncludedColumnSource {
        use proto_included_column_source::Kind;
        ProtoIncludedColumnSource {
            kind: Some(match self {
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
    /// Always use the given name for the key.
    ///
    /// * For a single-field key, this means that the column will get the given name.
    /// * For a multi-column key, the columns will get packed into a [`ScalarType::Record`], and
    ///   that Record will get the given name.
    Named(String),
}

impl RustType<ProtoKeyEnvelope> for KeyEnvelope {
    fn into_proto(&self) -> ProtoKeyEnvelope {
        use proto_key_envelope::Kind;
        ProtoKeyEnvelope {
            kind: Some(match self {
                KeyEnvelope::None => Kind::None(()),
                KeyEnvelope::Flattened => Kind::Flattened(()),
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

impl Timeline {
    const EPOCH_MILLISECOND_ID_CHAR: char = 'M';
    const EXTERNAL_ID_CHAR: char = 'E';
    const USER_ID_CHAR: char = 'U';

    fn id_char(&self) -> char {
        match self {
            Self::EpochMilliseconds => Self::EPOCH_MILLISECOND_ID_CHAR,
            Self::External(_) => Self::EXTERNAL_ID_CHAR,
            Self::User(_) => Self::USER_ID_CHAR,
        }
    }
}

impl RustType<ProtoTimeline> for Timeline {
    fn into_proto(&self) -> ProtoTimeline {
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

impl ToString for Timeline {
    fn to_string(&self) -> String {
        match self {
            Self::EpochMilliseconds => format!("{}", self.id_char()),
            Self::External(id) => format!("{}.{id}", self.id_char()),
            Self::User(id) => format!("{}.{id}", self.id_char()),
        }
    }
}

impl FromStr for Timeline {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err("empty timeline".to_string());
        }
        let mut chars = s.chars();
        match chars.next().expect("non-empty string") {
            Self::EPOCH_MILLISECOND_ID_CHAR => match chars.next() {
                None => Ok(Self::EpochMilliseconds),
                Some(_) => Err(format!("unknown timeline: {s}")),
            },
            Self::EXTERNAL_ID_CHAR => match chars.next() {
                Some('.') => Ok(Self::External(chars.as_str().to_string())),
                _ => Err(format!("unknown timeline: {s}")),
            },
            Self::USER_ID_CHAR => match chars.next() {
                Some('.') => Ok(Self::User(chars.as_str().to_string())),
                _ => Err(format!("unknown timeline: {s}")),
            },
            _ => Err(format!("unknown timeline: {s}")),
        }
    }
}

/// `SourceEnvelope`s describe how to turn a stream of messages from `SourceDesc`s
/// into a _differential stream_, that is, a stream of (data, time, diff)
/// triples.
///
/// PostgreSQL sources skip any explicit envelope handling, effectively
/// asserting that `SourceEnvelope` is `None` with `KeyEnvelope::None`.
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
}

impl RustType<ProtoSourceEnvelope> for SourceEnvelope {
    fn into_proto(&self) -> ProtoSourceEnvelope {
        use proto_source_envelope::Kind;
        ProtoSourceEnvelope {
            kind: Some(match self {
                SourceEnvelope::None(e) => Kind::None(e.into_proto()),
                SourceEnvelope::Debezium(e) => Kind::Debezium(e.into_proto()),
                SourceEnvelope::Upsert(e) => Kind::Upsert(e.into_proto()),
                SourceEnvelope::CdcV2 => Kind::CdcV2(()),
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct UpsertEnvelope {
    /// Full arity, including the key columns
    pub source_arity: usize,
    /// What style of Upsert we are using
    pub style: UpsertStyle,
    /// The indices of the keys in the full value row, used
    /// to deduplicate data in `upsert_core`
    pub key_indices: Vec<usize>,
}

impl Arbitrary for UpsertEnvelope {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<usize>(),
            any::<UpsertStyle>(),
            proptest::collection::vec(any::<usize>(), 1..4),
        )
            .prop_map(|(source_arity, style, key_indices)| Self {
                source_arity,
                style,
                key_indices,
            })
            .boxed()
    }
}

impl RustType<ProtoUpsertEnvelope> for UpsertEnvelope {
    fn into_proto(&self) -> ProtoUpsertEnvelope {
        ProtoUpsertEnvelope {
            source_arity: self.source_arity.into_proto(),
            style: Some(self.style.into_proto()),
            key_indices: self.key_indices.into_proto(),
        }
    }

    fn from_proto(proto: ProtoUpsertEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(UpsertEnvelope {
            source_arity: proto.source_arity.into_rust()?,
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
    fn into_proto(&self) -> ProtoUpsertStyle {
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
    /// Details about how to deduplicate the data in the topic.
    pub dedup: DebeziumDedupProjection,
}

impl RustType<ProtoDebeziumEnvelope> for DebeziumEnvelope {
    fn into_proto(&self) -> ProtoDebeziumEnvelope {
        ProtoDebeziumEnvelope {
            before_idx: self.before_idx.into_proto(),
            after_idx: self.after_idx.into_proto(),
            dedup: Some(self.dedup.into_proto()),
        }
    }

    fn from_proto(proto: ProtoDebeziumEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(DebeziumEnvelope {
            before_idx: proto.before_idx.into_rust()?,
            after_idx: proto.after_idx.into_rust()?,
            dedup: proto
                .dedup
                .into_rust_if_some("ProtoDebeziumEnvelope::dedup")?,
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
    /// The column index containing the debezium transaction metadata.
    pub data_transaction_idx: usize,
    pub data_transaction_id_idx: usize,
}

impl RustType<ProtoDebeziumTransactionMetadata> for DebeziumTransactionMetadata {
    fn into_proto(&self) -> ProtoDebeziumTransactionMetadata {
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
            data_transaction_idx: self.data_transaction_idx.into_proto(),
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
            data_transaction_idx: proto.data_transaction_idx.into_rust()?,
            data_transaction_id_idx: proto.data_transaction_id_idx.into_rust()?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DebeziumDedupProjection {
    /// The column index for the `op` field.
    pub op_idx: usize,
    /// The column index containing the debezium source metadata
    pub source_idx: usize,
    /// The record index of the `source.snapshot` field
    pub snapshot_idx: usize,
    /// The upstream database specific fields
    pub source_projection: DebeziumSourceProjection,
    /// Details about the transaction metadata.
    pub tx_metadata: Option<DebeziumTransactionMetadata>,
}

impl RustType<ProtoDebeziumDedupProjection> for DebeziumDedupProjection {
    fn into_proto(&self) -> ProtoDebeziumDedupProjection {
        ProtoDebeziumDedupProjection {
            op_idx: self.op_idx.into_proto(),
            source_idx: self.source_idx.into_proto(),
            snapshot_idx: self.snapshot_idx.into_proto(),
            source_projection: Some(self.source_projection.into_proto()),
            tx_metadata: self.tx_metadata.into_proto(),
        }
    }

    fn from_proto(proto: ProtoDebeziumDedupProjection) -> Result<Self, TryFromProtoError> {
        Ok(DebeziumDedupProjection {
            op_idx: proto.op_idx.into_rust()?,
            source_idx: proto.source_idx.into_rust()?,
            snapshot_idx: proto.snapshot_idx.into_rust()?,
            source_projection: proto
                .source_projection
                .into_rust_if_some("ProtoDebeziumDedupProjection::source_projection")?,
            tx_metadata: proto.tx_metadata.into_rust()?,
        })
    }
}

/// Debezium generates records that contain metadata about the upstream database. The structure of
/// this metadata depends on the type of connection used. This struct records the relevant indices
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
        sequence: usize,
        lsn: usize,
    },
    SqlServer {
        change_lsn: usize,
        event_serial_no: usize,
    },
}

impl RustType<ProtoDebeziumSourceProjection> for DebeziumSourceProjection {
    fn into_proto(&self) -> ProtoDebeziumSourceProjection {
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
    // TODO(petrosagg): This API looks very error prone. Can we statically enforce it somehow?
    fn into_source_envelope(
        self,
        key: Option<Vec<usize>>,
        key_arity: Option<usize>,
        source_arity: Option<usize>,
    ) -> SourceEnvelope {
        match self {
            UnplannedSourceEnvelope::Upsert(upsert_style) => {
                SourceEnvelope::Upsert(UpsertEnvelope {
                    style: upsert_style,
                    key_indices: key.expect("into_source_envelope to be passed correct parameters for UnplannedSourceEnvelope::Upsert"),
                    source_arity: source_arity.expect("into_source_envelope to be passed correct parameters for UnplannedSourceEnvelope::Upsert"),
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
                            self.into_source_envelope(None, None, None),
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
                let desc = keyed.concat(metadata_desc);
                (
                    self.into_source_envelope(key, Some(key_arity), Some(desc.arity())),
                    desc,
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

                        (
                            self.into_source_envelope(key, None, Some(desc.arity())),
                            desc,
                        )
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
                                    self.into_source_envelope(None, None, None),
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
        })
    }
}

pub trait SourceConnection: Clone {
    fn name(&self) -> &'static str;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KafkaSourceConnection {
    pub connection: KafkaConnection,
    pub connection_id: GlobalId,
    pub topic: String,
    // Map from partition -> starting offset
    pub start_offsets: HashMap<i32, i64>,
    pub group_id_prefix: Option<String>,
    pub environment_id: String,
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

impl SourceConnection for KafkaSourceConnection {
    fn name(&self) -> &'static str {
        "kafka"
    }
}

impl Arbitrary for KafkaSourceConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<KafkaConnection>(),
            any::<GlobalId>(),
            any::<String>(),
            proptest::collection::hash_map(any::<i32>(), any::<i64>(), 1..4),
            any::<Option<String>>(),
            any::<String>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
            any::<Option<IncludedColumnPos>>(),
        )
            .prop_map(
                |(
                    connection,
                    connection_id,
                    topic,
                    start_offsets,
                    group_id_prefix,
                    environment_id,
                    include_timestamp,
                    include_partition,
                    include_topic,
                    include_offset,
                    include_headers,
                )| KafkaSourceConnection {
                    connection,
                    connection_id,
                    topic,
                    start_offsets,
                    group_id_prefix,
                    environment_id,
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

impl RustType<ProtoKafkaSourceConnection> for KafkaSourceConnection {
    fn into_proto(&self) -> ProtoKafkaSourceConnection {
        ProtoKafkaSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            topic: self.topic.clone(),
            start_offsets: self.start_offsets.clone(),
            group_id_prefix: self.group_id_prefix.clone(),
            environment_id: None,
            environment_name: Some(self.environment_id.into_proto()),
            include_timestamp: self.include_timestamp.into_proto(),
            include_partition: self.include_partition.into_proto(),
            include_topic: self.include_topic.into_proto(),
            include_offset: self.include_offset.into_proto(),
            include_headers: self.include_headers.into_proto(),
        }
    }

    fn from_proto(proto: ProtoKafkaSourceConnection) -> Result<Self, TryFromProtoError> {
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
            environment_id: match (proto.environment_id, proto.environment_name) {
                (_, Some(name)) => name,
                (u128, _) => {
                    let uuid: Uuid =
                        u128.into_rust_if_some("ProtoKafkaSourceConnection::environment_id")?;
                    uuid.to_string()
                }
            },
            include_timestamp: proto.include_timestamp.into_rust()?,
            include_partition: proto.include_partition.into_rust()?,
            include_topic: proto.include_topic.into_rust()?,
            include_offset: proto.include_offset.into_rust()?,
            include_headers: proto.include_headers.into_rust()?,
        })
    }
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

/// An external source of updates for a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceDesc {
    pub connection: GenericSourceConnection,
    pub encoding: encoding::SourceDataEncoding,
    pub envelope: SourceEnvelope,
    pub metadata_columns: Vec<IncludedColumnSource>,
    pub timestamp_interval: Duration,
}

impl Arbitrary for SourceDesc {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GenericSourceConnection>(),
            any::<encoding::SourceDataEncoding>(),
            any::<SourceEnvelope>(),
            any::<Vec<IncludedColumnSource>>(),
            any::<Duration>(),
        )
            .prop_map(
                |(connection, encoding, envelope, metadata_columns, timestamp_interval)| Self {
                    connection,
                    encoding,
                    envelope,
                    metadata_columns,
                    timestamp_interval,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoSourceDesc> for SourceDesc {
    fn into_proto(&self) -> ProtoSourceDesc {
        ProtoSourceDesc {
            connection: Some(self.connection.into_proto()),
            encoding: Some(self.encoding.into_proto()),
            envelope: Some(self.envelope.into_proto()),
            metadata_columns: self.metadata_columns.into_proto(),
            timestamp_interval: Some(self.timestamp_interval.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceDesc {
            connection: proto
                .connection
                .into_rust_if_some("ProtoSourceDesc::connection")?,
            encoding: proto
                .encoding
                .into_rust_if_some("ProtoSourceDesc::encoding")?,
            envelope: proto
                .envelope
                .into_rust_if_some("ProtoSourceDesc::envelope")?,
            metadata_columns: proto.metadata_columns.into_rust()?,
            timestamp_interval: proto
                .timestamp_interval
                .into_rust_if_some("ProtoSourceDesc::timestamp_interval")?,
        })
    }
}

impl SourceDesc {
    /// Returns `true` if this connection yields data that is
    /// append-only/monotonic. Append-monly means the source
    /// never produces retractions.
    // TODO(guswynn): consider enforcing this more completely at the
    // parsing/typechecking level, by not using an `envelope`
    // for sources like pg
    pub fn monotonic(&self) -> bool {
        match self {
            // Postgres can produce retractions (deletes)
            SourceDesc {
                connection: GenericSourceConnection::Postgres(_),
                ..
            } => false,
            // Loadgen can produce retractions (deletes)
            SourceDesc {
                connection: GenericSourceConnection::LoadGenerator(_),
                ..
            } => false,
            // Other sources the `None` envelope are append-only.
            SourceDesc {
                envelope: SourceEnvelope::None(_),
                ..
            } => true,
            // Other combinations may produce retractions.
            SourceDesc {
                envelope:
                    SourceEnvelope::Debezium(_) | SourceEnvelope::Upsert(_) | SourceEnvelope::CdcV2,
                connection:
                    GenericSourceConnection::S3(_)
                    | GenericSourceConnection::Kafka(_)
                    | GenericSourceConnection::Kinesis(_)
                    | GenericSourceConnection::TestScript(_),
                ..
            } => false,
        }
    }

    /// The number of outputs this source will produce
    pub fn num_outputs(&self) -> usize {
        let subsources = match &self.connection {
            GenericSourceConnection::Kafka(_)
            | GenericSourceConnection::Kinesis(_)
            | GenericSourceConnection::S3(_)
            | GenericSourceConnection::TestScript(_) => 0,
            GenericSourceConnection::LoadGenerator(connection) => {
                connection.load_generator.views().len()
            }
            GenericSourceConnection::Postgres(connection) => {
                connection.publication_details.tables.len()
            }
        };
        // Every ingestion produces a main stream plus subsource streams
        subsources + 1
    }

    pub fn name(&self) -> &'static str {
        self.connection.name()
    }

    pub fn envelope(&self) -> &SourceEnvelope {
        &self.envelope
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GenericSourceConnection {
    Kafka(KafkaSourceConnection),
    Kinesis(KinesisSourceConnection),
    S3(S3SourceConnection),
    Postgres(PostgresSourceConnection),
    LoadGenerator(LoadGeneratorSourceConnection),
    TestScript(TestScriptSourceConnection),
}

impl GenericSourceConnection {
    pub fn connection_id(&self) -> Option<GlobalId> {
        use GenericSourceConnection::*;
        match self {
            Kafka(KafkaSourceConnection { connection_id, .. })
            | Kinesis(KinesisSourceConnection { connection_id, .. })
            | S3(S3SourceConnection { connection_id, .. })
            | Postgres(PostgresSourceConnection { connection_id, .. }) => Some(*connection_id),
            LoadGenerator(_) | TestScript(_) => None,
        }
    }
}

impl RustType<ProtoSourceConnection> for GenericSourceConnection {
    fn into_proto(&self) -> ProtoSourceConnection {
        use proto_source_connection::Kind;
        ProtoSourceConnection {
            kind: Some(match self {
                GenericSourceConnection::Kafka(kafka) => Kind::Kafka(kafka.into_proto()),
                GenericSourceConnection::Kinesis(kinesis) => Kind::Kinesis(kinesis.into_proto()),
                GenericSourceConnection::S3(s3) => Kind::S3(s3.into_proto()),
                GenericSourceConnection::Postgres(postgres) => {
                    Kind::Postgres(postgres.into_proto())
                }
                GenericSourceConnection::LoadGenerator(loadgen) => {
                    Kind::Loadgen(loadgen.into_proto())
                }
                GenericSourceConnection::TestScript(testscript) => {
                    Kind::Testscript(testscript.into_proto())
                }
            }),
        }
    }

    fn from_proto(proto: ProtoSourceConnection) -> Result<Self, TryFromProtoError> {
        use proto_source_connection::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSourceConnection::kind"))?;
        Ok(match kind {
            Kind::Kafka(kafka) => GenericSourceConnection::Kafka(kafka.into_rust()?),
            Kind::Kinesis(kinesis) => GenericSourceConnection::Kinesis(kinesis.into_rust()?),
            Kind::S3(s3) => GenericSourceConnection::S3(s3.into_rust()?),
            Kind::Postgres(postgres) => GenericSourceConnection::Postgres(postgres.into_rust()?),
            Kind::Loadgen(loadgen) => GenericSourceConnection::LoadGenerator(loadgen.into_rust()?),
            Kind::Testscript(testscript) => {
                GenericSourceConnection::TestScript(testscript.into_rust()?)
            }
        })
    }
}

impl GenericSourceConnection {
    /// Returns the name and type of each additional metadata column that
    /// Materialize will automatically append to the source's inherent columns.
    ///
    /// Presently, each source type exposes precisely one metadata column that
    /// corresponds to some source-specific record counter. For example, file
    /// sources use a line number, while Kafka sources use a topic offset.
    ///
    /// The columns declared here must be kept in sync with the actual source
    /// implementations that produce these columns.
    pub fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        match self {
            Self::Kafka(KafkaSourceConnection {
                include_partition: part,
                include_timestamp: time,
                include_topic: topic,
                include_offset: offset,
                include_headers: headers,
                ..
            }) => {
                let mut items = BTreeMap::new();
                for (include, ty) in [
                    (offset, ScalarType::UInt64),
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
                        items.insert(include.pos + 1, (&*include.name, ty.nullable(false)));
                    }
                }

                items.into_values().collect()
            }
            Self::Kinesis(_) => vec![],
            Self::S3(_) => vec![],
            Self::Postgres(_) => vec![],
            Self::LoadGenerator(_) => vec![],
            Self::TestScript(_) => vec![],
        }
    }

    pub fn metadata_column_types(&self) -> Vec<IncludedColumnSource> {
        match self {
            GenericSourceConnection::Kafka(KafkaSourceConnection {
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

            GenericSourceConnection::Kinesis(_)
            | GenericSourceConnection::S3(_)
            | GenericSourceConnection::Postgres(_)
            | GenericSourceConnection::LoadGenerator(_)
            | GenericSourceConnection::TestScript(_) => Vec::new(),
        }
    }

    /// Returns the name of the external source connection.
    pub fn name(&self) -> &'static str {
        match self {
            GenericSourceConnection::Kafka(c) => c.name(),
            GenericSourceConnection::Kinesis(c) => c.name(),
            GenericSourceConnection::S3(c) => c.name(),
            GenericSourceConnection::Postgres(c) => c.name(),
            GenericSourceConnection::LoadGenerator(c) => c.name(),
            GenericSourceConnection::TestScript(c) => c.name(),
        }
    }

    /// Optionally returns the name of the upstream resource this source corresponds to.
    /// (Currently only implemented for Kafka and Kinesis, to match old-style behavior
    ///  TODO: decide whether we want file paths and other upstream names to show up in metrics too.
    pub fn upstream_name(&self) -> Option<&str> {
        match self {
            GenericSourceConnection::Kafka(KafkaSourceConnection { topic, .. }) => {
                Some(topic.as_str())
            }
            GenericSourceConnection::Kinesis(KinesisSourceConnection { stream_name, .. }) => {
                Some(stream_name.as_str())
            }
            GenericSourceConnection::S3(_) => None,
            GenericSourceConnection::Postgres(_) => None,
            GenericSourceConnection::LoadGenerator(_) => None,
            GenericSourceConnection::TestScript(_) => None,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KinesisSourceConnection {
    pub connection_id: GlobalId,
    pub stream_name: String,
    pub aws: AwsConfig,
}

impl SourceConnection for KinesisSourceConnection {
    fn name(&self) -> &'static str {
        "kinesis"
    }
}

impl RustType<ProtoKinesisSourceConnection> for KinesisSourceConnection {
    fn into_proto(&self) -> ProtoKinesisSourceConnection {
        ProtoKinesisSourceConnection {
            stream_name: self.stream_name.clone(),
            aws: Some(self.aws.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
        }
    }

    fn from_proto(proto: ProtoKinesisSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(KinesisSourceConnection {
            stream_name: proto.stream_name,
            aws: proto
                .aws
                .into_rust_if_some("ProtoKinesisSourceConnection::aws")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoKinesisSourceConnection::connection_id")?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSourceConnection {
    pub connection_id: GlobalId,
    pub connection: PostgresConnection,
    /// The cast expressions to convert the incoming string encoded rows to
    /// their target types, keyed by their position in the source.
    pub table_casts: HashMap<usize, Vec<MirScalarExpr>>,
    pub publication: String,
    pub publication_details: PostgresSourcePublicationDetails,
}

impl Arbitrary for PostgresSourceConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<PostgresConnection>(),
            any::<GlobalId>(),
            proptest::collection::hash_map(
                any::<usize>(),
                proptest::collection::vec(any::<MirScalarExpr>(), 1..4),
                1..4,
            ),
            any::<String>(),
            any::<PostgresSourcePublicationDetails>(),
        )
            .prop_map(
                |(connection, connection_id, table_casts, publication, details)| Self {
                    connection,
                    connection_id,
                    table_casts,
                    publication,
                    publication_details: details,
                },
            )
            .boxed()
    }
}

impl SourceConnection for PostgresSourceConnection {
    fn name(&self) -> &'static str {
        "postgres"
    }
}

impl RustType<ProtoPostgresSourceConnection> for PostgresSourceConnection {
    fn into_proto(&self) -> ProtoPostgresSourceConnection {
        use proto_postgres_source_connection::ProtoPostgresTableCast;
        let mut table_casts = Vec::with_capacity(self.table_casts.len());
        let mut table_cast_pos = Vec::with_capacity(self.table_casts.len());
        for (pos, table_cast_cols) in self.table_casts.iter() {
            table_casts.push(ProtoPostgresTableCast {
                column_casts: table_cast_cols
                    .iter()
                    .cloned()
                    .map(|cast| cast.into_proto())
                    .collect(),
            });
            table_cast_pos.push(mz_ore::cast::usize_to_u64(*pos));
        }

        ProtoPostgresSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            publication: self.publication.clone(),
            details: Some(self.publication_details.into_proto()),
            table_casts,
            table_cast_pos,
        }
    }

    fn from_proto(proto: ProtoPostgresSourceConnection) -> Result<Self, TryFromProtoError> {
        // If we get the wrong number of table cast positions, we have to just
        // accept all of the table casts. This is somewhat harmless, as the
        // worst thing that happens is that we generate unused snapshots from
        // the upstream PG publication, and this will (hopefully) correct
        // itself on the next version upgrade.
        let table_cast_pos = if proto.table_casts.len() == proto.table_cast_pos.len() {
            proto.table_cast_pos
        } else {
            (1..proto.table_casts.len() + 1)
                .map(mz_ore::cast::usize_to_u64)
                .collect()
        };

        let mut table_casts = HashMap::new();
        for (pos, cast) in table_cast_pos
            .into_iter()
            .zip_eq(proto.table_casts.into_iter())
        {
            let mut column_casts = vec![];
            for cast in cast.column_casts {
                column_casts.push(cast.into_rust()?);
            }
            table_casts.insert(mz_ore::cast::u64_to_usize(pos), column_casts);
        }

        Ok(PostgresSourceConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoPostgresSourceConnection::connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoPostgresSourceConnection::connection_id")?,
            publication: proto.publication,
            publication_details: proto
                .details
                .into_rust_if_some("ProtoPostgresSourceConnection::details")?,
            table_casts,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresSourcePublicationDetails {
    pub tables: Vec<mz_postgres_util::desc::PostgresTableDesc>,
    pub slot: String,
}

impl RustType<ProtoPostgresSourcePublicationDetails> for PostgresSourcePublicationDetails {
    fn into_proto(&self) -> ProtoPostgresSourcePublicationDetails {
        ProtoPostgresSourcePublicationDetails {
            tables: self.tables.iter().map(|t| t.into_proto()).collect(),
            slot: self.slot.clone(),
        }
    }

    fn from_proto(proto: ProtoPostgresSourcePublicationDetails) -> Result<Self, TryFromProtoError> {
        Ok(PostgresSourcePublicationDetails {
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
pub struct LoadGeneratorSourceConnection {
    pub load_generator: LoadGenerator,
    pub tick_micros: Option<u64>,
}

impl SourceConnection for LoadGeneratorSourceConnection {
    fn name(&self) -> &'static str {
        "load-generator"
    }
}
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LoadGenerator {
    Auction,
    Counter,
    Datums,
    Tpch {
        count_supplier: i64,
        count_part: i64,
        count_customer: i64,
        count_orders: i64,
        count_clerk: i64,
    },
}

impl LoadGenerator {
    fn data_encoding_inner(&self) -> DataEncodingInner {
        match self {
            LoadGenerator::Auction => DataEncodingInner::RowCodec(RelationDesc::empty()),
            LoadGenerator::Datums => {
                let mut desc =
                    RelationDesc::empty().with_column("rowid", ScalarType::Int64.nullable(false));
                let typs = ScalarType::enumerate();
                let mut names = HashSet::new();
                for typ in typs {
                    // Cut out variant information from the debug print.
                    let mut name = format!("_{:?}", typ)
                        .split(' ')
                        .next()
                        .unwrap()
                        .to_lowercase();
                    // Incase we ever have multiple variants of the same type, create
                    // unique names for them.
                    while names.contains(&name) {
                        name.push('_');
                    }
                    names.insert(name.clone());
                    desc = desc.with_column(name, typ.clone().nullable(true));
                }
                DataEncodingInner::RowCodec(desc)
            }
            LoadGenerator::Counter => DataEncodingInner::RowCodec(
                RelationDesc::empty().with_column("counter", ScalarType::Int64.nullable(false)),
            ),
            LoadGenerator::Tpch { .. } => DataEncodingInner::RowCodec(RelationDesc::empty()),
        }
    }

    pub fn data_encoding(&self) -> SourceDataEncoding {
        SourceDataEncoding::Single(DataEncoding::new(self.data_encoding_inner()))
    }

    /// Returns the list of table names and their column types that this generator generates
    pub fn views(&self) -> Vec<(&str, RelationDesc)> {
        match self {
            LoadGenerator::Auction => vec![
                (
                    "organizations",
                    RelationDesc::empty()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("name", ScalarType::String.nullable(false))
                        .with_key(vec![0]),
                ),
                (
                    "users",
                    RelationDesc::empty()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("org_id", ScalarType::Int64.nullable(false))
                        .with_column("name", ScalarType::String.nullable(false))
                        .with_key(vec![0]),
                ),
                (
                    "accounts",
                    RelationDesc::empty()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("org_id", ScalarType::Int64.nullable(false))
                        .with_column("balance", ScalarType::Int64.nullable(false))
                        .with_key(vec![0]),
                ),
                (
                    "auctions",
                    RelationDesc::empty()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("seller", ScalarType::Int64.nullable(false))
                        .with_column("item", ScalarType::String.nullable(false))
                        .with_column("end_time", ScalarType::TimestampTz.nullable(false))
                        .with_key(vec![0]),
                ),
                (
                    "bids",
                    RelationDesc::empty()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("buyer", ScalarType::Int64.nullable(false))
                        .with_column("auction_id", ScalarType::Int64.nullable(false))
                        .with_column("amount", ScalarType::Int32.nullable(false))
                        .with_column("bid_time", ScalarType::TimestampTz.nullable(false))
                        .with_key(vec![0]),
                ),
            ],
            LoadGenerator::Counter => vec![],
            LoadGenerator::Datums => vec![],
            LoadGenerator::Tpch { .. } => {
                let identifier = ScalarType::Int64.nullable(false);
                let decimal = ScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::try_from(2i64).unwrap()),
                }
                .nullable(false);
                vec![
                    (
                        "supplier",
                        RelationDesc::empty()
                            .with_column("s_suppkey", identifier.clone())
                            .with_column("s_name", ScalarType::String.nullable(false))
                            .with_column("s_address", ScalarType::String.nullable(false))
                            .with_column("s_nationkey", identifier.clone())
                            .with_column("s_phone", ScalarType::String.nullable(false))
                            .with_column("s_acctbal", decimal.clone())
                            .with_column("s_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                    (
                        "part",
                        RelationDesc::empty()
                            .with_column("p_partkey", identifier.clone())
                            .with_column("p_name", ScalarType::String.nullable(false))
                            .with_column("p_mfgr", ScalarType::String.nullable(false))
                            .with_column("p_brand", ScalarType::String.nullable(false))
                            .with_column("p_type", ScalarType::String.nullable(false))
                            .with_column("p_size", ScalarType::Int32.nullable(false))
                            .with_column("p_container", ScalarType::String.nullable(false))
                            .with_column("p_retailprice", decimal.clone())
                            .with_column("p_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                    (
                        "partsupp",
                        RelationDesc::empty()
                            .with_column("ps_partkey", identifier.clone())
                            .with_column("ps_suppkey", identifier.clone())
                            .with_column("ps_availqty", ScalarType::Int32.nullable(false))
                            .with_column("ps_supplycost", decimal.clone())
                            .with_column("ps_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0, 1]),
                    ),
                    (
                        "customer",
                        RelationDesc::empty()
                            .with_column("c_custkey", identifier.clone())
                            .with_column("c_name", ScalarType::String.nullable(false))
                            .with_column("c_address", ScalarType::String.nullable(false))
                            .with_column("c_nationkey", identifier.clone())
                            .with_column("c_phone", ScalarType::String.nullable(false))
                            .with_column("c_acctbal", decimal.clone())
                            .with_column("c_mktsegment", ScalarType::String.nullable(false))
                            .with_column("c_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                    (
                        "orders",
                        RelationDesc::empty()
                            .with_column("o_orderkey", identifier.clone())
                            .with_column("o_custkey", identifier.clone())
                            .with_column("o_orderstatus", ScalarType::String.nullable(false))
                            .with_column("o_totalprice", decimal.clone())
                            .with_column("o_orderdate", ScalarType::Date.nullable(false))
                            .with_column("o_orderpriority", ScalarType::String.nullable(false))
                            .with_column("o_clerk", ScalarType::String.nullable(false))
                            .with_column("o_shippriority", ScalarType::Int32.nullable(false))
                            .with_column("o_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                    (
                        "lineitem",
                        RelationDesc::empty()
                            .with_column("l_orderkey", identifier.clone())
                            .with_column("l_partkey", identifier.clone())
                            .with_column("l_suppkey", identifier.clone())
                            .with_column("l_linenumber", ScalarType::Int32.nullable(false))
                            .with_column("l_quantity", decimal.clone())
                            .with_column("l_extendedprice", decimal.clone())
                            .with_column("l_discount", decimal.clone())
                            .with_column("l_tax", decimal)
                            .with_column("l_returnflag", ScalarType::String.nullable(false))
                            .with_column("l_linestatus", ScalarType::String.nullable(false))
                            .with_column("l_shipdate", ScalarType::Date.nullable(false))
                            .with_column("l_commitdate", ScalarType::Date.nullable(false))
                            .with_column("l_receiptdate", ScalarType::Date.nullable(false))
                            .with_column("l_shipinstruct", ScalarType::String.nullable(false))
                            .with_column("l_shipmode", ScalarType::String.nullable(false))
                            .with_column("l_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0, 3]),
                    ),
                    (
                        "nation",
                        RelationDesc::empty()
                            .with_column("n_nationkey", identifier.clone())
                            .with_column("n_name", ScalarType::String.nullable(false))
                            .with_column("n_regionkey", identifier.clone())
                            .with_column("n_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                    (
                        "region",
                        RelationDesc::empty()
                            .with_column("r_regionkey", identifier)
                            .with_column("r_name", ScalarType::String.nullable(false))
                            .with_column("r_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0]),
                    ),
                ]
            }
        }
    }
}

pub trait Generator {
    /// Returns a function that produces rows and batch information.
    fn by_seed(
        &self,
        now: NowFn,
        seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row, i64)>>;
}

#[derive(Clone, Copy, Debug)]
pub enum GeneratorMessageType {
    InProgress,
    Finalized,
}

impl RustType<ProtoLoadGeneratorSourceConnection> for LoadGeneratorSourceConnection {
    fn into_proto(&self) -> ProtoLoadGeneratorSourceConnection {
        ProtoLoadGeneratorSourceConnection {
            generator: Some(match &self.load_generator {
                LoadGenerator::Auction => ProtoGenerator::Auction(()),
                LoadGenerator::Counter => ProtoGenerator::Counter(()),
                LoadGenerator::Tpch {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                } => ProtoGenerator::Tpch(ProtoTpchLoadGenerator {
                    count_supplier: *count_supplier,
                    count_part: *count_part,
                    count_customer: *count_customer,
                    count_orders: *count_orders,
                    count_clerk: *count_clerk,
                }),
                LoadGenerator::Datums => ProtoGenerator::Datums(()),
            }),
            tick_micros: self.tick_micros,
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorSourceConnection) -> Result<Self, TryFromProtoError> {
        let generator = proto.generator.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoLoadGeneratorSourceConnection::generator")
        })?;
        Ok(LoadGeneratorSourceConnection {
            load_generator: match generator {
                ProtoGenerator::Auction(()) => LoadGenerator::Auction,
                ProtoGenerator::Counter(()) => LoadGenerator::Counter,
                ProtoGenerator::Tpch(ProtoTpchLoadGenerator {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                }) => LoadGenerator::Tpch {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                },
                ProtoGenerator::Datums(()) => LoadGenerator::Datums,
            },
            tick_micros: proto.tick_micros,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TestScriptSourceConnection {
    pub desc_json: String,
}

impl SourceConnection for TestScriptSourceConnection {
    fn name(&self) -> &'static str {
        "testscript"
    }
}

impl RustType<ProtoTestScriptSourceConnection> for TestScriptSourceConnection {
    fn into_proto(&self) -> ProtoTestScriptSourceConnection {
        ProtoTestScriptSourceConnection {
            desc_json: self.desc_json.clone(),
        }
    }

    fn from_proto(proto: ProtoTestScriptSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(TestScriptSourceConnection {
            desc_json: proto.desc_json,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3SourceConnection {
    pub connection_id: GlobalId,
    pub key_sources: Vec<S3KeySource>,
    pub pattern: Option<Glob>,
    pub aws: AwsConfig,
    pub compression: Compression,
}

impl SourceConnection for S3SourceConnection {
    fn name(&self) -> &'static str {
        "s3"
    }
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

impl Arbitrary for S3SourceConnection {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<GlobalId>(),
            any::<Vec<S3KeySource>>(),
            proptest::option::of(any_glob()),
            any::<AwsConfig>(),
            any::<Compression>(),
        )
            .prop_map(|(connection_id, key_sources, pattern, aws, compression)| {
                S3SourceConnection {
                    connection_id,
                    key_sources,
                    pattern,
                    aws,
                    compression,
                }
            })
            .boxed()
    }
}

impl RustType<ProtoS3SourceConnection> for S3SourceConnection {
    fn into_proto(&self) -> ProtoS3SourceConnection {
        ProtoS3SourceConnection {
            connection_id: Some(self.connection_id.into_proto()),
            key_sources: self.key_sources.into_proto(),
            pattern: self.pattern.as_ref().map(|g| g.glob().into()),
            aws: Some(self.aws.into_proto()),
            compression: Some(self.compression.into_proto()),
        }
    }

    fn from_proto(proto: ProtoS3SourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(S3SourceConnection {
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoS3SourceConnection::connection_id")?,
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
            aws: proto
                .aws
                .into_rust_if_some("ProtoS3SourceConnection::aws")?,
            compression: proto
                .compression
                .into_rust_if_some("ProtoS3SourceConnection::compression")?,
        })
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

/// A `SourceToken` manages interest in a source.
///
/// When the `SourceToken` is dropped the associated source will be stopped.
pub struct SourceToken {
    pub(crate) _activator: Rc<ActivateOnDrop<()>>,
}

#[test]
fn test_timeline_parsing() {
    assert_eq!(Ok(Timeline::EpochMilliseconds), "M".parse());
    assert_eq!(Ok(Timeline::External("JOE".to_string())), "E.JOE".parse());
    assert_eq!(Ok(Timeline::User("MIKE".to_string())), "U.MIKE".parse());

    assert!("Materialize".parse::<Timeline>().is_err());
    assert!("Ejoe".parse::<Timeline>().is_err());
    assert!("Umike".parse::<Timeline>().is_err());
    assert!("Dance".parse::<Timeline>().is_err());
    assert!("".parse::<Timeline>().is_err());
}
