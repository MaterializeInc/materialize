// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to the introduction of changing collections into `dataflow`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, NullArray, StructArray};
use arrow::datatypes::{Field, Fields};
use bytes::{BufMut, Bytes};
use columnation::Columnation;
use itertools::EitherOrBoth::Both;
use itertools::Itertools;
use load_generator::LoadGeneratorSourceExportDetails;
use mz_ore::assert_none;
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema2};
use mz_persist_types::stats::{ColumnarStats, DynStats, OptionStats, PrimitiveStats, StructStats};
use mz_persist_types::stats2::ColumnarStatsBuilder;
use mz_persist_types::Codec;
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{
    arb_row_for_relation, ColumnType, Datum, GlobalId, ProtoRelationDesc, ProtoRow, RelationDesc,
    Row, RowColumnarDecoder, RowColumnarEncoder,
};
use mz_sql_parser::ast::{Ident, IdentError, UnresolvedItemName};
use proptest::collection::vec;
use proptest::prelude::any;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest::string::string_regex;
use proptest_derive::Arbitrary;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::{AlterError, CollectionMetadata};
use crate::errors::{DataflowError, ProtoDataflowError};
use crate::instances::StorageInstanceId;
use crate::sources::proto_ingestion_description::{ProtoSourceExport, ProtoSourceImport};
use crate::AlterCompatible;

pub mod encoding;
pub mod envelope;
pub mod kafka;
pub mod load_generator;
pub mod mysql;
pub mod postgres;

pub use crate::sources::envelope::SourceEnvelope;
pub use crate::sources::kafka::KafkaSourceConnection;
pub use crate::sources::load_generator::LoadGeneratorSourceConnection;
pub use crate::sources::mysql::{MySqlSourceConnection, MySqlSourceExportDetails};
pub use crate::sources::postgres::{PostgresSourceConnection, PostgresSourceExportDetails};

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.sources.rs"));

/// A fraternal twin of [`UnresolvedItemName`] that can implement [`Arbitrary`].
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ExportReference(pub Vec<Ident>);

impl proptest::prelude::Arbitrary for ExportReference {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // Generate a set of valid `Ident` strings.
        let string_strategy = string_regex("[a-zA-Z_][a-zA-Z0-9_$]{3,10}").unwrap();

        // We only ever expect between 1 and 3 elements of the reference. We
        // could theoretically expect more, but we can never expect 0.
        vec(string_strategy, 1..=3)
            .prop_map(|inner| {
                ExportReference(inner.into_iter().map(|i| Ident::new(i).unwrap()).collect())
            })
            .boxed()
    }
}

impl From<UnresolvedItemName> for ExportReference {
    fn from(value: UnresolvedItemName) -> Self {
        ExportReference(value.0)
    }
}

/// A description of a source ingestion
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct IngestionDescription<S: 'static = (), C: ConnectionAccess = InlinedConnection> {
    /// The source description.
    pub desc: SourceDesc<C>,
    /// Additional storage controller metadata needed to ingest this source
    pub ingestion_metadata: S,
    /// Collections to be exported by this ingestion.
    ///
    /// # Notes
    /// - For multi-output sources:
    ///     - Add subsources by adding a new [`SourceExport`].
    ///     - Remove subsources by removing the [`SourceExport`].
    ///
    ///   Re-rendering/executing the source after making these modifications
    ///   adds and drops the subsource, respectively.
    /// - This field includes the primary source's ID, which might need to be
    ///   filtered out to understand which exports are ingestion export
    ///   subsources.
    /// - This field does _not_ include the remap collection, which is tracked
    ///   in its own field.
    /// - This field should not be populated by the storage controller in
    ///   response to collections being created.
    #[proptest(
        strategy = "proptest::collection::btree_map(any::<GlobalId>(), any::<SourceExport<Option<ExportReference>, S>>(), 0..4)"
    )]
    pub source_exports: BTreeMap<GlobalId, SourceExport<Option<ExportReference>, S>>,
    /// The ID of the instance in which to install the source.
    pub instance_id: StorageInstanceId,
    /// The ID of this ingestion's remap/progress collection.
    pub remap_collection_id: GlobalId,
}

impl IngestionDescription {
    pub fn new(
        desc: SourceDesc,
        instance_id: StorageInstanceId,
        remap_collection_id: GlobalId,
    ) -> Self {
        Self {
            desc,
            ingestion_metadata: (),
            source_exports: BTreeMap::new(),
            instance_id,
            remap_collection_id,
        }
    }
}

impl<S> IngestionDescription<S> {
    /// Return an iterator over the `GlobalId`s of `self`'s subsources.
    pub fn subsource_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        // Expand self so that any new fields added generate a compiler error to
        // increase the likelihood of developers seeing this function.
        let IngestionDescription {
            desc: _,
            ingestion_metadata: _,
            source_exports,
            instance_id: _,
            remap_collection_id,
        } = &self;

        source_exports
            .keys()
            .copied()
            .chain(std::iter::once(*remap_collection_id))
    }
}

impl<S: Clone> IngestionDescription<S> {
    pub fn source_exports_with_output_indices(&self) -> BTreeMap<GlobalId, SourceExport<usize, S>> {
        let mut source_exports = BTreeMap::new();

        // `self.source_exports` contains all source-exports (e.g. subsources & tables) as well as
        // the primary source relation. It's not guaranteed that the primary source relation is
        // the first element in the map, however it much be set to output 0. The other elements
        // must maintain their map order, so we use this rather than the index to determine the
        // output index.
        let mut next_output = 1;
        for (
            id,
            SourceExport {
                ingestion_output: _,
                storage_metadata,
                details,
            },
        ) in self.source_exports.iter()
        {
            let ingestion_output = match details {
                // the primary source relation will use SourceExportDetails::None
                SourceExportDetails::None => 0,
                _ => {
                    let idx = next_output;
                    next_output += 1;
                    idx
                }
            };

            source_exports.insert(
                *id,
                SourceExport {
                    ingestion_output,
                    storage_metadata: storage_metadata.clone(),
                    details: details.clone(),
                },
            );
        }

        source_exports
    }
}

impl<S: Debug + Eq + PartialEq + AlterCompatible> AlterCompatible for IngestionDescription<S> {
    fn alter_compatible(
        &self,
        id: GlobalId,
        other: &IngestionDescription<S>,
    ) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let IngestionDescription {
            desc,
            ingestion_metadata,
            source_exports,
            instance_id,
            remap_collection_id,
        } = self;

        let compatibility_checks = [
            (desc.alter_compatible(id, &other.desc).is_ok(), "desc"),
            (
                ingestion_metadata == &other.ingestion_metadata,
                "ingestion_metadata",
            ),
            (
                source_exports
                    .iter()
                    .merge_join_by(&other.source_exports, |(l_key, _), (r_key, _)| {
                        l_key.cmp(r_key)
                    })
                    .all(|r| match r {
                        Both(
                            (
                                _,
                                SourceExport {
                                    ingestion_output: l_reference,
                                    storage_metadata: l_metadata,
                                    details: l_details,
                                },
                            ),
                            (
                                _,
                                SourceExport {
                                    ingestion_output: r_reference,
                                    storage_metadata: r_metadata,
                                    details: r_details,
                                },
                            ),
                        ) => {
                            l_reference == r_reference
                                && l_metadata.alter_compatible(id, r_metadata).is_ok()
                                && l_details.alter_compatible(id, r_details).is_ok()
                        }
                        _ => true,
                    }),
                "source_exports",
            ),
            (instance_id == &other.instance_id, "instance_id"),
            (
                remap_collection_id == &other.remap_collection_id,
                "remap_collection_id",
            ),
        ];
        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "IngestionDescription incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<IngestionDescription, R>
    for IngestionDescription<(), ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> IngestionDescription {
        let IngestionDescription {
            desc,
            ingestion_metadata,
            source_exports,
            instance_id,
            remap_collection_id,
        } = self;

        IngestionDescription {
            desc: desc.into_inline_connection(r),
            ingestion_metadata,
            source_exports,
            instance_id,
            remap_collection_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct SourceExport<O: proptest::prelude::Arbitrary, S = ()> {
    /// Which output from the ingestion this source refers to.
    pub ingestion_output: O,
    /// The collection metadata needed to write the exported data
    pub storage_metadata: S,
    /// Details necessary for the source to export data to this export's collection.
    pub details: SourceExportDetails,
}

impl RustType<ProtoIngestionDescription> for IngestionDescription<CollectionMetadata> {
    fn into_proto(&self) -> ProtoIngestionDescription {
        ProtoIngestionDescription {
            source_exports: self.source_exports.into_proto(),
            ingestion_metadata: Some(self.ingestion_metadata.into_proto()),
            desc: Some(self.desc.into_proto()),
            instance_id: Some(self.instance_id.into_proto()),
            remap_collection_id: Some(self.remap_collection_id.into_proto()),
        }
    }

    fn from_proto(proto: ProtoIngestionDescription) -> Result<Self, TryFromProtoError> {
        Ok(IngestionDescription {
            source_exports: proto.source_exports.into_rust()?,
            desc: proto
                .desc
                .into_rust_if_some("ProtoIngestionDescription::desc")?,
            ingestion_metadata: proto
                .ingestion_metadata
                .into_rust_if_some("ProtoIngestionDescription::ingestion_metadata")?,
            instance_id: proto
                .instance_id
                .into_rust_if_some("ProtoIngestionDescription::instance_id")?,
            remap_collection_id: proto
                .remap_collection_id
                .into_rust_if_some("ProtoIngestionDescription::remap_collection_id")?,
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

impl ProtoMapEntry<GlobalId, SourceExport<Option<ExportReference>, CollectionMetadata>>
    for ProtoSourceExport
{
    fn from_rust<'a>(
        (id, source_export): (
            &'a GlobalId,
            &'a SourceExport<Option<ExportReference>, CollectionMetadata>,
        ),
    ) -> Self {
        ProtoSourceExport {
            id: Some(id.into_proto()),
            ingestion_output: source_export
                .ingestion_output
                .iter()
                .map(|e| &e.0)
                .flatten()
                .map(|i| i.clone().into_string())
                .collect(),
            storage_metadata: Some(source_export.storage_metadata.into_proto()),
            details: Some(source_export.details.into_proto()),
        }
    }

    fn into_rust(
        self,
    ) -> Result<
        (
            GlobalId,
            SourceExport<Option<ExportReference>, CollectionMetadata>,
        ),
        TryFromProtoError,
    > {
        Ok((
            self.id.into_rust_if_some("ProtoSourceExport::id")?,
            SourceExport {
                ingestion_output: if self.ingestion_output.is_empty() {
                    None
                } else {
                    Some(ExportReference(
                        self.ingestion_output
                            .into_iter()
                            .map(|i| Ident::new(i).expect("proto encoding must roundtrip"))
                            .collect(),
                    ))
                },
                storage_metadata: self
                    .storage_metadata
                    .into_rust_if_some("ProtoSourceExport::storage_metadata")?,
                details: self
                    .details
                    .into_rust_if_some("ProtoSourceExport::details")?,
            },
        ))
    }
}

pub trait SourceTimestamp: Timestamp + Columnation + Refines<()> + std::fmt::Display {
    fn encode_row(&self) -> Row;
    fn decode_row(row: &Row) -> Self;
}

impl SourceTimestamp for MzOffset {
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
}

impl differential_dataflow::difference::IsZero for MzOffset {
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

impl columnation::Columnation for MzOffset {
    type InnerRegion = columnation::CopyRegion<MzOffset>;
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
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.offset.less_equal(&other.offset)
    }
}

impl TotalOrder for MzOffset {}

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

/// A connection to an external system
pub trait SourceConnection: Debug + Clone + PartialEq + AlterCompatible {
    /// The name of the external system (e.g kafka, postgres, etc).
    fn name(&self) -> &'static str;

    /// The name of the resource in the external system (e.g kafka topic) if any
    fn external_reference(&self) -> Option<&str>;

    /// The schema of this connection's key rows.
    // This is mostly setting the stage for the subsequent PRs that will attempt to compute and
    // typecheck subsequent stages of the pipeline using the types of the earlier stages of the
    // pipeline.
    fn key_desc(&self) -> RelationDesc;

    /// The schema of this connection's value rows.
    // This is mostly setting the stage for the subsequent PRs that will attempt to compute and
    // typecheck subsequent stages of the pipeline using the types of the earlier stages of the
    // pipeline.
    fn value_desc(&self) -> RelationDesc;

    /// The schema of this connection's timestamp type. This will also be the schema of the
    /// progress relation.
    fn timestamp_desc(&self) -> RelationDesc;

    /// The id of the connection object (i.e the one obtained from running `CREATE CONNECTION`) in
    /// the catalog, if any.
    fn connection_id(&self) -> Option<GlobalId>;

    /// Returns metadata columns that this connection *instance* will produce once rendered. The
    /// columns are returned in the order specified by the user.
    fn metadata_columns(&self) -> Vec<(&str, ColumnType)>;
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
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct SourceDesc<C: ConnectionAccess = InlinedConnection> {
    pub connection: GenericSourceConnection<C>,
    pub encoding: Option<encoding::SourceDataEncoding<C>>,
    pub envelope: SourceEnvelope,
    pub timestamp_interval: Duration,
}

impl<R: ConnectionResolver> IntoInlineConnection<SourceDesc, R>
    for SourceDesc<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SourceDesc {
        let SourceDesc {
            connection,
            encoding,
            envelope,
            timestamp_interval,
        } = self;

        SourceDesc {
            connection: connection.into_inline_connection(&r),
            encoding: encoding.map(|e| e.into_inline_connection(r)),
            envelope,
            timestamp_interval,
        }
    }
}

impl RustType<ProtoSourceDesc> for SourceDesc {
    fn into_proto(&self) -> ProtoSourceDesc {
        ProtoSourceDesc {
            connection: Some(self.connection.into_proto()),
            encoding: self.encoding.into_proto(),
            envelope: Some(self.envelope.into_proto()),
            timestamp_interval: Some(self.timestamp_interval.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceDesc {
            connection: proto
                .connection
                .into_rust_if_some("ProtoSourceDesc::connection")?,
            encoding: proto.encoding.into_rust()?,
            envelope: proto
                .envelope
                .into_rust_if_some("ProtoSourceDesc::envelope")?,
            timestamp_interval: proto
                .timestamp_interval
                .into_rust_if_some("ProtoSourceDesc::timestamp_interval")?,
        })
    }
}

impl<C: ConnectionAccess> SourceDesc<C> {
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
            // MySQL can produce retractions (deletes)
            SourceDesc {
                connection: GenericSourceConnection::MySql(_),
                ..
            } => false,
            // Upsert and CdcV2 may produce retractions.
            SourceDesc {
                envelope: SourceEnvelope::Upsert(_) | SourceEnvelope::CdcV2,
                connection:
                    GenericSourceConnection::Kafka(_) | GenericSourceConnection::LoadGenerator(_),
                ..
            } => false,
            // Loadgen can produce retractions (deletes)
            SourceDesc {
                connection: GenericSourceConnection::LoadGenerator(g),
                ..
            } => g.load_generator.is_monotonic(),
            // Other sources with the `None` envelope are append-only.
            SourceDesc {
                envelope: SourceEnvelope::None(_),
                ..
            } => true,
        }
    }

    pub fn envelope(&self) -> &SourceEnvelope {
        &self.envelope
    }
}

impl<C: ConnectionAccess> AlterCompatible for SourceDesc<C> {
    /// Determines if `self` is compatible with another `SourceDesc`, in such a
    /// way that it is possible to turn `self` into `other` through a valid
    /// series of transformations (e.g. no transformation or `ALTER SOURCE`).
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let Self {
            connection,
            encoding,
            envelope,
            timestamp_interval,
        } = &self;

        let compatibility_checks = [
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (
                match (encoding, &other.encoding) {
                    (Some(s), Some(o)) => s.alter_compatible(id, o).is_ok(),
                    (s, o) => s == o,
                },
                "encoding",
            ),
            (envelope == &other.envelope, "envelope"),
            (
                timestamp_interval == &other.timestamp_interval,
                "timestamp_interval",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SourceDesc incompatible {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GenericSourceConnection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaSourceConnection<C>),
    Postgres(PostgresSourceConnection<C>),
    MySql(MySqlSourceConnection<C>),
    LoadGenerator(LoadGeneratorSourceConnection),
}

impl<C: ConnectionAccess> From<KafkaSourceConnection<C>> for GenericSourceConnection<C> {
    fn from(conn: KafkaSourceConnection<C>) -> Self {
        Self::Kafka(conn)
    }
}

impl<C: ConnectionAccess> From<PostgresSourceConnection<C>> for GenericSourceConnection<C> {
    fn from(conn: PostgresSourceConnection<C>) -> Self {
        Self::Postgres(conn)
    }
}

impl<C: ConnectionAccess> From<MySqlSourceConnection<C>> for GenericSourceConnection<C> {
    fn from(conn: MySqlSourceConnection<C>) -> Self {
        Self::MySql(conn)
    }
}

impl<C: ConnectionAccess> From<LoadGeneratorSourceConnection> for GenericSourceConnection<C> {
    fn from(conn: LoadGeneratorSourceConnection) -> Self {
        Self::LoadGenerator(conn)
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<GenericSourceConnection, R>
    for GenericSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> GenericSourceConnection {
        match self {
            GenericSourceConnection::Kafka(kafka) => {
                GenericSourceConnection::Kafka(kafka.into_inline_connection(r))
            }
            GenericSourceConnection::Postgres(pg) => {
                GenericSourceConnection::Postgres(pg.into_inline_connection(r))
            }
            GenericSourceConnection::MySql(mysql) => {
                GenericSourceConnection::MySql(mysql.into_inline_connection(r))
            }
            GenericSourceConnection::LoadGenerator(lg) => {
                GenericSourceConnection::LoadGenerator(lg)
            }
        }
    }
}

impl<C: ConnectionAccess> SourceConnection for GenericSourceConnection<C> {
    fn name(&self) -> &'static str {
        match self {
            Self::Kafka(conn) => conn.name(),
            Self::Postgres(conn) => conn.name(),
            Self::MySql(conn) => conn.name(),
            Self::LoadGenerator(conn) => conn.name(),
        }
    }

    fn external_reference(&self) -> Option<&str> {
        match self {
            Self::Kafka(conn) => conn.external_reference(),
            Self::Postgres(conn) => conn.external_reference(),
            Self::MySql(conn) => conn.external_reference(),
            Self::LoadGenerator(conn) => conn.external_reference(),
        }
    }

    fn key_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.key_desc(),
            Self::Postgres(conn) => conn.key_desc(),
            Self::MySql(conn) => conn.key_desc(),
            Self::LoadGenerator(conn) => conn.key_desc(),
        }
    }

    fn value_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.value_desc(),
            Self::Postgres(conn) => conn.value_desc(),
            Self::MySql(conn) => conn.value_desc(),
            Self::LoadGenerator(conn) => conn.value_desc(),
        }
    }

    fn timestamp_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.timestamp_desc(),
            Self::Postgres(conn) => conn.timestamp_desc(),
            Self::MySql(conn) => conn.timestamp_desc(),
            Self::LoadGenerator(conn) => conn.timestamp_desc(),
        }
    }

    fn connection_id(&self) -> Option<GlobalId> {
        match self {
            Self::Kafka(conn) => conn.connection_id(),
            Self::Postgres(conn) => conn.connection_id(),
            Self::MySql(conn) => conn.connection_id(),
            Self::LoadGenerator(conn) => conn.connection_id(),
        }
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        match self {
            Self::Kafka(conn) => conn.metadata_columns(),
            Self::Postgres(conn) => conn.metadata_columns(),
            Self::MySql(conn) => conn.metadata_columns(),
            Self::LoadGenerator(conn) => conn.metadata_columns(),
        }
    }
}

impl<C: ConnectionAccess> crate::AlterCompatible for GenericSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let r = match (self, other) {
            (Self::Kafka(conn), Self::Kafka(other)) => conn.alter_compatible(id, other),
            (Self::Postgres(conn), Self::Postgres(other)) => conn.alter_compatible(id, other),
            (Self::MySql(conn), Self::MySql(other)) => conn.alter_compatible(id, other),
            (Self::LoadGenerator(conn), Self::LoadGenerator(other)) => {
                conn.alter_compatible(id, other)
            }
            _ => Err(AlterError { id }),
        };

        if r.is_err() {
            tracing::warn!(
                "GenericSourceConnection incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
        }

        r
    }
}

impl RustType<ProtoSourceConnection> for GenericSourceConnection<InlinedConnection> {
    fn into_proto(&self) -> ProtoSourceConnection {
        use proto_source_connection::Kind;
        ProtoSourceConnection {
            kind: Some(match self {
                GenericSourceConnection::Kafka(kafka) => Kind::Kafka(kafka.into_proto()),
                GenericSourceConnection::Postgres(postgres) => {
                    Kind::Postgres(postgres.into_proto())
                }
                GenericSourceConnection::MySql(mysql) => Kind::Mysql(mysql.into_proto()),
                GenericSourceConnection::LoadGenerator(loadgen) => {
                    Kind::Loadgen(loadgen.into_proto())
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
            Kind::Postgres(postgres) => GenericSourceConnection::Postgres(postgres.into_rust()?),
            Kind::Mysql(mysql) => GenericSourceConnection::MySql(mysql.into_rust()?),
            Kind::Loadgen(loadgen) => GenericSourceConnection::LoadGenerator(loadgen.into_rust()?),
        })
    }
}

/// Details necessary for each source export to allow the source implementations
/// to export data to the export's collection.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceExportDetails {
    /// Used for the primary export of a source
    None,
    Kafka,
    Postgres(PostgresSourceExportDetails),
    MySql(MySqlSourceExportDetails),
    LoadGenerator(LoadGeneratorSourceExportDetails),
}

impl crate::AlterCompatible for SourceExportDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let r = match (self, other) {
            (Self::None, Self::None) => Ok(()),
            (Self::Kafka, Self::Kafka) => Ok(()),
            (Self::Postgres(s), Self::Postgres(o)) => s.alter_compatible(id, o),
            (Self::MySql(s), Self::MySql(o)) => s.alter_compatible(id, o),
            (Self::LoadGenerator(s), Self::LoadGenerator(o)) => s.alter_compatible(id, o),
            _ => Err(AlterError { id }),
        };

        if r.is_err() {
            tracing::warn!(
                "SourceExportDetails incompatible:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
        }

        r
    }
}

impl RustType<ProtoSourceExportDetails> for SourceExportDetails {
    fn into_proto(&self) -> ProtoSourceExportDetails {
        use proto_source_export_details::Kind;
        ProtoSourceExportDetails {
            kind: match self {
                SourceExportDetails::None => None,
                SourceExportDetails::Kafka => Some(Kind::Kafka(())),
                SourceExportDetails::Postgres(details) => {
                    Some(Kind::Postgres(details.into_proto()))
                }
                SourceExportDetails::MySql(details) => Some(Kind::Mysql(details.into_proto())),
                SourceExportDetails::LoadGenerator(details) => {
                    Some(Kind::Loadgen(details.into_proto()))
                }
            },
        }
    }

    fn from_proto(proto: ProtoSourceExportDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_export_details::Kind;
        Ok(match proto.kind {
            None => SourceExportDetails::None,
            Some(Kind::Kafka(_)) => SourceExportDetails::Kafka,
            Some(Kind::Postgres(details)) => SourceExportDetails::Postgres(details.into_rust()?),
            Some(Kind::Mysql(details)) => SourceExportDetails::MySql(details.into_rust()?),
            Some(Kind::Loadgen(details)) => {
                SourceExportDetails::LoadGenerator(details.into_rust()?)
            }
        })
    }
}

/// Details necessary to store in the `Details` option of a source export
/// statement (`CREATE SUBSOURCE` and `CREATE TABLE .. FROM SOURCE` statements),
/// to generate the appropriate `SourceExportDetails` struct during planning.
/// NOTE that this is serialized as proto to the catalog, so any changes here
/// must be backwards compatible or will require a migration.
/// We only support `CREATE SUBSOURCE` statements for Postgres and MySQL
/// for now, so we only need to support those here.
pub enum SourceExportStatementDetails {
    Postgres {
        table: mz_postgres_util::desc::PostgresTableDesc,
    },
    MySql {
        table: mz_mysql_util::MySqlTableDesc,
        initial_gtid_set: String,
    },
    LoadGenerator,
}

impl RustType<ProtoSourceExportStatementDetails> for SourceExportStatementDetails {
    fn into_proto(&self) -> ProtoSourceExportStatementDetails {
        match self {
            SourceExportStatementDetails::Postgres { table } => ProtoSourceExportStatementDetails {
                kind: Some(proto_source_export_statement_details::Kind::Postgres(
                    postgres::ProtoPostgresSourceExportStatementDetails {
                        table: Some(table.into_proto()),
                    },
                )),
            },
            SourceExportStatementDetails::MySql {
                table,
                initial_gtid_set,
            } => ProtoSourceExportStatementDetails {
                kind: Some(proto_source_export_statement_details::Kind::Mysql(
                    mysql::ProtoMySqlSourceExportStatementDetails {
                        table: Some(table.into_proto()),
                        initial_gtid_set: initial_gtid_set.clone(),
                    },
                )),
            },
            SourceExportStatementDetails::LoadGenerator => ProtoSourceExportStatementDetails {
                kind: Some(proto_source_export_statement_details::Kind::Loadgen(())),
            },
        }
    }

    fn from_proto(proto: ProtoSourceExportStatementDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_export_statement_details::Kind;
        Ok(match proto.kind {
            Some(Kind::Postgres(details)) => SourceExportStatementDetails::Postgres {
                table: mz_postgres_util::desc::PostgresTableDesc::from_proto(
                    details.table.ok_or_else(|| {
                        TryFromProtoError::missing_field(
                            "ProtoPostgresSourceExportStatementDetails::table",
                        )
                    })?,
                )?,
            },
            Some(Kind::Mysql(details)) => SourceExportStatementDetails::MySql {
                table: mz_mysql_util::MySqlTableDesc::from_proto(details.table.ok_or_else(
                    || {
                        TryFromProtoError::missing_field(
                            "ProtoMySqlSourceExportStatementDetails::table",
                        )
                    },
                )?)?,
                initial_gtid_set: details.initial_gtid_set,
            },
            Some(Kind::Loadgen(())) => SourceExportStatementDetails::LoadGenerator,
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoSourceExportStatementDetails::kind",
                ))
            }
        })
    }
}

#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SourceData(pub Result<Row, DataflowError>);

impl Default for SourceData {
    fn default() -> Self {
        SourceData(Ok(Row::default()))
    }
}

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
    type Storage = ProtoRow;
    type Schema = RelationDesc;

    fn codec_name() -> String {
        "protobuf[SourceData]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8], schema: &RelationDesc) -> Result<Self, String> {
        let mut val = SourceData::default();
        <Self as Codec>::decode_from(&mut val, buf, &mut None, schema)?;
        Ok(val)
    }

    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        storage: &mut Option<ProtoRow>,
        schema: &RelationDesc,
    ) -> Result<(), String> {
        // Optimize for common case of `Ok` by leaving a (cleared) `ProtoRow` in
        // the `Ok` variant of `ProtoSourceData`. prost's `Message::merge` impl
        // is smart about reusing the `Vec<Datum>` when it can.
        let mut proto = storage.take().unwrap_or_default();
        proto.clear();
        let mut proto = ProtoSourceData {
            kind: Some(proto_source_data::Kind::Ok(proto)),
        };
        proto.merge(buf).map_err(|err| err.to_string())?;
        match (proto.kind, &mut self.0) {
            // Again, optimize for the common case...
            (Some(proto_source_data::Kind::Ok(proto)), Ok(row)) => {
                let ret = row.decode_from_proto(&proto, schema);
                storage.replace(proto);
                ret
            }
            // ...otherwise fall back to the obvious thing.
            (kind, _) => {
                let proto = ProtoSourceData { kind };
                *self = proto.into_rust().map_err(|err| err.to_string())?;
                // Nothing to put back in storage.
                Ok(())
            }
        }
    }

    fn encode_schema(schema: &Self::Schema) -> Bytes {
        schema.into_proto().encode_to_vec().into()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        let proto = ProtoRelationDesc::decode(buf.as_ref()).expect("valid schema");
        proto.into_rust().expect("valid schema")
    }
}

/// Given a [`RelationDesc`] returns an arbitrary [`SourceData`].
pub fn arb_source_data_for_relation_desc(desc: &RelationDesc) -> impl Strategy<Value = SourceData> {
    let row_strat = arb_row_for_relation(desc).no_shrink();

    proptest::strategy::Union::new_weighted(vec![
        (50, row_strat.prop_map(|row| SourceData(Ok(row))).boxed()),
        (
            1,
            any::<DataflowError>()
                .prop_map(|err| SourceData(Err(err)))
                .no_shrink()
                .boxed(),
        ),
    ])
}

/// Describes how external references should be organized in a multi-level
/// hierarchy.
///
/// For both PostgreSQL and MySQL sources, these levels of reference are
/// intrinsic to the items which we're referencing. If there are other naming
/// schemas for other types of sources we discover, we might need to revisit
/// this.
pub trait ExternalCatalogReference {
    /// The "second" level of namespacing for the reference.
    fn schema_name(&self) -> &str;
    /// The lowest level of namespacing for the reference.
    fn item_name(&self) -> &str;
}

impl ExternalCatalogReference for mz_mysql_util::MySqlTableDesc {
    fn schema_name(&self) -> &str {
        &self.schema_name
    }

    fn item_name(&self) -> &str {
        &self.name
    }
}

impl ExternalCatalogReference for mz_postgres_util::desc::PostgresTableDesc {
    fn schema_name(&self) -> &str {
        &self.namespace
    }

    fn item_name(&self) -> &str {
        &self.name
    }
}

// This implementation provides a means of converting arbitrary objects into a
// `SubsourceCatalogReference`, e.g. load generator view names.
impl<'a> ExternalCatalogReference for (&'a str, &'a str) {
    fn schema_name(&self) -> &str {
        self.0
    }

    fn item_name(&self) -> &str {
        self.1
    }
}

/// Stores and resolves references to a `&[T: ExternalCatalogReference]`.
///
/// This is meant to provide an API to quickly look up a source's subsources.
///
/// For sources that do not provide any subsources, use the `Default`
/// implementation, which is empty and will not be able to resolve any
/// references.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceReferenceResolver {
    inner: BTreeMap<Ident, BTreeMap<Ident, BTreeMap<Ident, usize>>>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ExternalReferenceResolutionError {
    #[error("reference to {name} not found in source")]
    DoesNotExist { name: String },
    #[error(
        "reference to {name} is ambiguous, consider specifying an additional \
    layer of qualification"
    )]
    Ambiguous { name: String },
    #[error("invalid identifier: {0}")]
    Ident(#[from] IdentError),
}

impl<'a> SourceReferenceResolver {
    /// Constructs a new `SourceReferenceResolver` from a slice of `T:
    /// SubsourceCatalogReference`.
    ///
    /// # Errors
    /// - If any `&str` provided cannot be taken to an [`Ident`].
    pub fn new<T: ExternalCatalogReference>(
        database: &str,
        referenceable_items: &'a [T],
    ) -> Result<SourceReferenceResolver, ExternalReferenceResolutionError> {
        // An index from table name -> schema name -> database name -> index in
        // `referenceable_items`.
        let mut inner = BTreeMap::new();

        let database = Ident::new(database)?;

        for (reference_idx, item) in referenceable_items.iter().enumerate() {
            let item_name = Ident::new(item.item_name())?;
            let schema_name = Ident::new(item.schema_name())?;

            inner
                .entry(item_name)
                .or_insert_with(BTreeMap::new)
                .entry(schema_name)
                .or_insert_with(BTreeMap::new)
                .entry(database.clone())
                .or_insert(reference_idx);
        }

        Ok(SourceReferenceResolver { inner })
    }

    /// Returns the canonical reference and index from which it originated in
    /// the `referenceable_items` provided to [`Self::new`].
    ///
    /// # Args
    /// - `name` is `&[Ident]` to let users provide the inner element of either
    ///   [`UnresolvedItemName`] or [`ExportReference`].
    /// - `canonicalize_to_width` limits the number of elements in the returned
    ///   [`UnresolvedItemName`];this is useful if the source type requires
    ///   contriving database and schema names that a subsource should not
    ///   persist as its reference.
    ///
    /// # Errors
    /// - If `name` does not resolve to an item in `self.inner`.
    ///
    /// # Panics
    /// - If `canonicalize_to_width`` is not in `1..=3`.
    pub fn resolve(
        &self,
        name: &[Ident],
        canonicalize_to_width: usize,
    ) -> Result<(UnresolvedItemName, usize), ExternalReferenceResolutionError> {
        let (db, schema, idx) = self.resolve_inner(name)?;

        let item = name.last().expect("must have provided at least 1 element");

        let canonical_name = match canonicalize_to_width {
            1 => vec![item.clone()],
            2 => vec![schema.clone(), item.clone()],
            3 => vec![db.clone(), schema.clone(), item.clone()],
            o => panic!("canonicalize_to_width values must be 1..=3, but got {}", o),
        };

        Ok((UnresolvedItemName(canonical_name), idx))
    }

    /// Returns the index from which it originated in the `referenceable_items`
    /// provided to [`Self::new`].
    ///
    /// # Args
    /// `name` is `&[Ident]` to let users provide the inner element of either
    /// [`UnresolvedItemName`] or [`ExportReference`].
    ///
    /// # Errors
    /// - If `name` does not resolve to an item in `self.inner`.
    pub fn resolve_idx(&self, name: &[Ident]) -> Result<usize, ExternalReferenceResolutionError> {
        let (_db, _schema, idx) = self.resolve_inner(name)?;
        Ok(idx)
    }

    /// Returns the index from which it originated in the `referenceable_items`
    /// provided to [`Self::new`].
    ///
    /// # Args
    /// `name` is `&[Ident]` to let users provide the inner element of either
    /// [`UnresolvedItemName`] or [`ExportReference`].
    ///
    /// # Return
    /// Returns a tuple whose elements are:
    /// 1. The "database"- or top-level namespace of the reference.
    /// 2. The "schema"- or second-level namespace of the reference.
    /// 3. The index to find the item in `referenceable_items` argument provided
    ///    to `SourceReferenceResolver::new`.
    ///
    /// # Errors
    /// - If `name` does not resolve to an item in `self.inner`.
    fn resolve_inner<'name: 'a>(
        &'a self,
        name: &'name [Ident],
    ) -> Result<(&'a Ident, &'a Ident, usize), ExternalReferenceResolutionError> {
        let get_provided_name = || UnresolvedItemName(name.to_vec()).to_string();

        // Names must be composed of 1..=3 elements.
        if !(1..=3).contains(&name.len()) {
            Err(ExternalReferenceResolutionError::DoesNotExist {
                name: get_provided_name(),
            })?;
        }

        // Fill on the leading elements with `None` if they aren't present.
        let mut names = std::iter::repeat(None)
            .take(3 - name.len())
            .chain(name.iter().map(Some));

        let database = names.next().flatten();
        let schema = names.next().flatten();
        let item = names
            .next()
            .flatten()
            .expect("must have provided the item name");

        assert_none!(names.next(), "expected a 3-element iterator");

        let schemas =
            self.inner
                .get(item)
                .ok_or_else(|| ExternalReferenceResolutionError::DoesNotExist {
                    name: get_provided_name(),
                })?;

        let schema = match schema {
            Some(schema) => schema,
            None => schemas.keys().exactly_one().map_err(|_e| {
                ExternalReferenceResolutionError::Ambiguous {
                    name: get_provided_name(),
                }
            })?,
        };

        let databases =
            schemas
                .get(schema)
                .ok_or_else(|| ExternalReferenceResolutionError::DoesNotExist {
                    name: get_provided_name(),
                })?;

        let database = match database {
            Some(database) => database,
            None => databases.keys().exactly_one().map_err(|_e| {
                ExternalReferenceResolutionError::Ambiguous {
                    name: get_provided_name(),
                }
            })?,
        };

        let reference_idx = databases.get(database).ok_or_else(|| {
            ExternalReferenceResolutionError::DoesNotExist {
                name: get_provided_name(),
            }
        })?;

        Ok((database, schema, *reference_idx))
    }
}

/// A decoder for [`Row`]s within [`SourceData`].
///
/// This type exists as a wrapper around [`RowColumnarDecoder`] to handle the
/// case where the [`RelationDesc`] we're encoding with has no columns. See
/// [`SourceDataRowColumnarEncoder`] for more details.
#[derive(Debug)]
pub enum SourceDataRowColumnarDecoder {
    Row(RowColumnarDecoder),
    EmptyRow,
}

impl SourceDataRowColumnarDecoder {
    pub fn decode(&self, idx: usize, row: &mut Row) {
        match self {
            SourceDataRowColumnarDecoder::Row(decoder) => decoder.decode(idx, row),
            SourceDataRowColumnarDecoder::EmptyRow => {
                // Create a packer just to clear the Row.
                row.packer();
            }
        }
    }
}

#[derive(Debug)]
pub struct SourceDataColumnarDecoder {
    row_decoder: SourceDataRowColumnarDecoder,
    err_decoder: BinaryArray,
}

impl SourceDataColumnarDecoder {
    pub fn new(col: StructArray, desc: &RelationDesc) -> Result<Self, anyhow::Error> {
        // TODO(parkmcar): We should validate the fields here.
        let (_fields, arrays, nullability) = col.into_parts();

        if nullability.is_some() {
            anyhow::bail!("SourceData is not nullable, but found {nullability:?}");
        }
        if arrays.len() != 2 {
            anyhow::bail!("SourceData should only have two fields, found {arrays:?}");
        }

        let errs = arrays[1]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("expected BinaryArray, found {:?}", arrays[1]))?;

        let row_decoder = match arrays[0].data_type() {
            arrow::datatypes::DataType::Struct(_) => {
                let rows = arrays[0]
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        anyhow::anyhow!("expected StructArray, found {:?}", arrays[0])
                    })?;
                let decoder = RowColumnarDecoder::new(rows.clone(), desc)?;
                SourceDataRowColumnarDecoder::Row(decoder)
            }
            arrow::datatypes::DataType::Null => SourceDataRowColumnarDecoder::EmptyRow,
            other => anyhow::bail!("expected Struct or Null Array, found {other:?}"),
        };

        Ok(SourceDataColumnarDecoder {
            row_decoder,
            err_decoder: errs.clone(),
        })
    }
}

impl ColumnDecoder<SourceData> for SourceDataColumnarDecoder {
    fn decode(&self, idx: usize, val: &mut SourceData) {
        let err_null = self.err_decoder.is_null(idx);
        let row_null = match &self.row_decoder {
            SourceDataRowColumnarDecoder::Row(decoder) => decoder.is_null(idx),
            SourceDataRowColumnarDecoder::EmptyRow => !err_null,
        };

        match (row_null, err_null) {
            (true, false) => {
                let err = self.err_decoder.value(idx);
                let err = ProtoDataflowError::decode(err)
                    .expect("proto should be valid")
                    .into_rust()
                    .expect("error should be valid");
                val.0 = Err(err);
            }
            (false, true) => {
                let row = match val.0.as_mut() {
                    Ok(row) => row,
                    Err(_) => {
                        val.0 = Ok(Row::default());
                        val.0.as_mut().unwrap()
                    }
                };
                self.row_decoder.decode(idx, row);
            }
            (true, true) => panic!("should have one of 'ok' or 'err'"),
            (false, false) => panic!("cannot have both 'ok' and 'err'"),
        }
    }

    fn is_null(&self, idx: usize) -> bool {
        let err_null = self.err_decoder.is_null(idx);
        let row_null = match &self.row_decoder {
            SourceDataRowColumnarDecoder::Row(decoder) => decoder.is_null(idx),
            SourceDataRowColumnarDecoder::EmptyRow => !err_null,
        };
        assert!(!err_null || !row_null, "SourceData should never be null!");

        false
    }

    fn stats(&self) -> ColumnarStats {
        let err_stats =
            OptionStats::<PrimitiveStats<Vec<u8>>>::from_column(&self.err_decoder).finish();
        let row_stats = match &self.row_decoder {
            SourceDataRowColumnarDecoder::Row(encoder) => encoder.stats(),
            SourceDataRowColumnarDecoder::EmptyRow => {
                let stats = OptionStats {
                    none: self.err_decoder.len() - self.err_decoder.null_count(),
                    some: StructStats {
                        len: self.err_decoder.len(),
                        cols: BTreeMap::default(),
                    },
                };
                stats.into_columnar_stats()
            }
        };

        let stats = [
            (
                SourceDataColumnarEncoder::OK_COLUMN_NAME.to_string(),
                row_stats.into_columnar_stats(),
            ),
            (
                SourceDataColumnarEncoder::ERR_COLUMN_NAME.to_string(),
                err_stats,
            ),
        ];
        let stats = StructStats {
            len: self.err_decoder.len(),
            cols: stats.into_iter().map(|(name, s)| (name, s)).collect(),
        };
        stats.into_columnar_stats()
    }
}

/// An encoder for [`Row`]s within [`SourceData`].
///
/// This type exists as a wrapper around [`RowColumnarEncoder`] to support
/// encoding empty [`Row`]s. A [`RowColumnarEncoder`] finishes as a
/// [`StructArray`] which is required to have at least one column, and thus
/// cannot support empty [`Row`]s.
#[derive(Debug)]
pub enum SourceDataRowColumnarEncoder {
    Row(RowColumnarEncoder),
    EmptyRow,
}

impl SourceDataRowColumnarEncoder {
    pub fn append(&mut self, row: &Row) {
        match self {
            SourceDataRowColumnarEncoder::Row(encoder) => encoder.append(row),
            SourceDataRowColumnarEncoder::EmptyRow => {
                assert_eq!(row.iter().count(), 0)
            }
        }
    }

    pub fn append_null(&mut self) {
        match self {
            SourceDataRowColumnarEncoder::Row(encoder) => encoder.append_null(),
            SourceDataRowColumnarEncoder::EmptyRow => (),
        }
    }
}

#[derive(Debug)]
pub struct SourceDataColumnarEncoder {
    row_encoder: SourceDataRowColumnarEncoder,
    err_encoder: BinaryBuilder,
}

impl SourceDataColumnarEncoder {
    const OK_COLUMN_NAME: &'static str = "ok";
    const ERR_COLUMN_NAME: &'static str = "err";

    pub fn new(desc: &RelationDesc) -> Self {
        let row_encoder = match RowColumnarEncoder::new(desc) {
            Some(encoder) => SourceDataRowColumnarEncoder::Row(encoder),
            None => {
                assert!(desc.typ().columns().is_empty());
                SourceDataRowColumnarEncoder::EmptyRow
            }
        };
        let err_encoder = BinaryBuilder::new();

        SourceDataColumnarEncoder {
            row_encoder,
            err_encoder,
        }
    }
}

impl ColumnEncoder<SourceData> for SourceDataColumnarEncoder {
    type FinishedColumn = StructArray;

    #[inline]
    fn append(&mut self, val: &SourceData) {
        match val.0.as_ref() {
            Ok(row) => {
                self.row_encoder.append(row);
                self.err_encoder.append_null();
            }
            Err(err) => {
                self.row_encoder.append_null();
                self.err_encoder
                    .append_value(err.into_proto().encode_to_vec());
            }
        }
    }

    #[inline]
    fn append_null(&mut self) {
        panic!("appending a null into SourceDataColumnarEncoder is not supported");
    }

    fn finish(self) -> Self::FinishedColumn {
        let SourceDataColumnarEncoder {
            row_encoder,
            mut err_encoder,
        } = self;

        let err_column = BinaryBuilder::finish(&mut err_encoder);
        let row_column: ArrayRef = match row_encoder {
            SourceDataRowColumnarEncoder::Row(encoder) => {
                let column = encoder.finish();
                Arc::new(column)
            }
            SourceDataRowColumnarEncoder::EmptyRow => Arc::new(NullArray::new(err_column.len())),
        };

        assert_eq!(row_column.len(), err_column.len());

        let fields = vec![
            Field::new(Self::OK_COLUMN_NAME, row_column.data_type().clone(), true),
            Field::new(Self::ERR_COLUMN_NAME, err_column.data_type().clone(), true),
        ];
        let arrays: Vec<Arc<dyn Array>> = vec![row_column, Arc::new(err_column)];
        StructArray::new(Fields::from(fields), arrays, None)
    }
}

impl Schema2<SourceData> for RelationDesc {
    type ArrowColumn = StructArray;
    type Statistics = StructStats;

    type Decoder = SourceDataColumnarDecoder;
    type Encoder = SourceDataColumnarEncoder;

    fn decoder(&self, col: Self::ArrowColumn) -> Result<Self::Decoder, anyhow::Error> {
        SourceDataColumnarDecoder::new(col, self)
    }

    fn encoder(&self) -> Result<Self::Encoder, anyhow::Error> {
        Ok(SourceDataColumnarEncoder::new(self))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{build_compare, ArrayData};
    use bytes::Bytes;
    use mz_ore::assert_err;
    use mz_persist::indexed::columnar::arrow::realloc_array;
    use mz_persist::metrics::ColumnarMetrics;
    use mz_persist_types::parquet::EncodingConfig;
    use mz_persist_types::schema::{backward_compatible, Migration};
    use mz_repr::{arb_relation_desc_diff, PropRelationDescDiff, ProtoRelationDesc, ScalarType};
    use proptest::prelude::*;
    use proptest::strategy::{Union, ValueTree};

    use super::*;

    #[mz_ore::test]
    fn test_timeline_parsing() {
        assert_eq!(Ok(Timeline::EpochMilliseconds), "M".parse());
        assert_eq!(Ok(Timeline::External("JOE".to_string())), "E.JOE".parse());
        assert_eq!(Ok(Timeline::User("MIKE".to_string())), "U.MIKE".parse());

        assert_err!("Materialize".parse::<Timeline>());
        assert_err!("Ejoe".parse::<Timeline>());
        assert_err!("Umike".parse::<Timeline>());
        assert_err!("Dance".parse::<Timeline>());
        assert_err!("".parse::<Timeline>());
    }

    #[track_caller]
    fn roundtrip_source_data(desc: RelationDesc, datas: Vec<SourceData>, config: &EncodingConfig) {
        let metrics = ColumnarMetrics::disconnected();
        let mut encoder = <RelationDesc as Schema2<SourceData>>::encoder(&desc).unwrap();
        for data in &datas {
            encoder.append(data);
        }
        let col = encoder.finish();

        // Reallocate our arrays with lgalloc.
        let col = realloc_array(&col, &metrics);

        // Roundtrip through ProtoArray format.
        {
            let proto = col.to_data().into_proto();
            let bytes = proto.encode_to_vec();
            let proto = mz_persist_types::arrow::ProtoArrayData::decode(&bytes[..]).unwrap();
            let array_data: ArrayData = proto.into_rust().unwrap();

            let col_rnd = StructArray::from(array_data.clone());
            assert_eq!(col, col_rnd);

            let col_dyn = arrow::array::make_array(array_data);
            let col_dyn = col_dyn.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(&col, col_dyn);
        }

        // Encode to Parquet.
        let mut buf = Vec::new();
        let fields = Fields::from(vec![Field::new("k", col.data_type().clone(), false)]);
        let arrays: Vec<Arc<dyn Array>> = vec![Arc::new(col.clone())];
        mz_persist_types::parquet::encode_arrays(&mut buf, fields, arrays, config).unwrap();

        // Decode from Parquet.
        let buf = Bytes::from(buf);
        let mut reader = mz_persist_types::parquet::decode_arrays(buf).unwrap();
        let maybe_batch = reader.next();

        // If we didn't encode any data then our record_batch will be empty.
        let Some(record_batch) = maybe_batch else {
            assert!(datas.is_empty());
            return;
        };
        let record_batch = record_batch.unwrap();

        assert_eq!(record_batch.columns().len(), 1);
        let rnd_col = &record_batch.columns()[0];
        let rnd_col = rnd_col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();

        // Read back all of our data and assert it roundtrips.
        let mut rnd_data = SourceData(Ok(Row::default()));
        let decoder = <RelationDesc as Schema2<SourceData>>::decoder(&desc, rnd_col).unwrap();
        for (idx, og_data) in datas.into_iter().enumerate() {
            decoder.decode(idx, &mut rnd_data);
            assert_eq!(og_data, rnd_data);
        }

        // Verify that the RelationDesc itself roundtrips through
        // {encode,decode}_schema.
        let encoded_schema = SourceData::encode_schema(&desc);
        let roundtrip_desc = SourceData::decode_schema(&encoded_schema);
        assert_eq!(desc, roundtrip_desc);

        // Verify that the RelationDesc is backward compatible with itself (this
        // mostly checks for `unimplemented!` type panics).
        let migration =
            mz_persist_types::schema::backward_compatible(col.data_type(), col.data_type());
        let migration = migration.expect("should be backward compatible with self");
        // Also verify that the Fn doesn't do anything wonky.
        let migrated = migration.migrate(Arc::new(col.clone()));
        assert_eq!(col.data_type(), migrated.data_type());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn all_source_data_roundtrips() {
        let mut weights = vec![(500, Just(0..8)), (50, Just(8..32))];
        if std::env::var("PROPTEST_LARGE_DATA").is_ok() {
            weights.extend([
                (10, Just(32..128)),
                (5, Just(128..512)),
                (3, Just(512..2048)),
                (1, Just(2048..8192)),
            ]);
        }
        let num_rows = Union::new_weighted(weights);

        let strat = (any::<RelationDesc>(), num_rows).prop_flat_map(|(desc, num_rows)| {
            proptest::collection::vec(arb_source_data_for_relation_desc(&desc), num_rows)
                .prop_map(move |datas| (desc.clone(), datas))
        });

        proptest!(|((config, (desc, source_datas)) in (any::<EncodingConfig>(), strat))| {
            roundtrip_source_data(desc, source_datas, &config);
        });
    }

    fn is_sorted(array: &dyn Array) -> bool {
        let Ok(cmp) = build_compare(array, array) else {
            // TODO: arrow v51.0.0 doesn't support comparing structs. When
            // we migrate to v52+, the `build_compare` function is
            // deprecated and replaced by `make_comparator`, which does
            // support structs. At which point, this will work (and we
            // should switch this early return to an expect, if possible).
            return false;
        };
        (0..array.len())
            .tuple_windows()
            .all(|(i, j)| cmp(i, j).is_le())
    }

    fn get_data_type(schema: &impl Schema2<SourceData>) -> arrow::datatypes::DataType {
        use mz_persist_types::columnar::ColumnEncoder;
        let array = Schema2::encoder(schema).expect("valid schema").finish();
        Array::data_type(&array).clone()
    }

    #[track_caller]
    fn backward_compatible_testcase(
        old: &RelationDesc,
        new: &RelationDesc,
        migration: Migration,
        datas: &[SourceData],
    ) {
        let mut encoder = Schema2::<SourceData>::encoder(old).expect("valid schema");
        for data in datas {
            encoder.append(data);
        }
        let old = encoder.finish();
        let new = Schema2::<SourceData>::encoder(new)
            .expect("valid schema")
            .finish();
        let old: Arc<dyn Array> = Arc::new(old);
        let new: Arc<dyn Array> = Arc::new(new);
        let migrated = migration.migrate(Arc::clone(&old));
        assert_eq!(migrated.data_type(), new.data_type());

        // Check the sortedness preservation, if we can.
        if migration.preserves_order() && is_sorted(&old) {
            assert!(is_sorted(&new))
        }
    }

    #[mz_ore::test]
    fn backward_compatible_empty_add_column() {
        let old = RelationDesc::empty();
        let new = RelationDesc::from_names_and_types([("a", ScalarType::Bool.nullable(true))]);

        let old_data_type = get_data_type(&old);
        let new_data_type = get_data_type(&new);

        let migration = backward_compatible(&old_data_type, &new_data_type);
        assert!(migration.is_some());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn backward_compatible_migrate1() {
        let strat = (any::<RelationDesc>(), any::<RelationDesc>()).prop_flat_map(|(old, new)| {
            proptest::collection::vec(arb_source_data_for_relation_desc(&old), 2)
                .prop_map(move |datas| (old.clone(), new.clone(), datas))
        });

        proptest!(|((old, new, datas) in strat)| {
            let old_data_type = get_data_type(&old);
            let new_data_type = get_data_type(&new);

            if let Some(migration) = backward_compatible(&old_data_type, &new_data_type) {
                backward_compatible_testcase(&old, &new, migration, &datas);
            };
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn backward_compatible_migrate_from_common() {
        fn test_case(old: RelationDesc, diffs: Vec<PropRelationDescDiff>, datas: Vec<SourceData>) {
            // TODO(parkmycar): As we iterate on schema migrations more things should become compatible.
            let should_be_compatible = diffs.iter().all(|diff| match diff {
                // We only support adding nullable columns.
                PropRelationDescDiff::AddColumn {
                    typ: ColumnType { nullable, .. },
                    ..
                } => *nullable,
                // TODO(parkmycar): Re-enable DropColumn.
                // PropRelationDescDiff::DropColumn { .. } => true,
                _ => false,
            });

            let mut new = old.clone();
            for diff in diffs.into_iter() {
                diff.apply(&mut new)
            }

            let old_data_type = get_data_type(&old);
            let new_data_type = get_data_type(&new);

            if let Some(migration) = backward_compatible(&old_data_type, &new_data_type) {
                backward_compatible_testcase(&old, &new, migration, &datas);
            } else if should_be_compatible {
                panic!("new DataType was not compatible when it should have been!");
            }
        }

        let strat = any::<RelationDesc>()
            .prop_flat_map(|desc| {
                proptest::collection::vec(arb_source_data_for_relation_desc(&desc), 2)
                    .no_shrink()
                    .prop_map(move |datas| (desc.clone(), datas))
            })
            .prop_flat_map(|(desc, datas)| {
                arb_relation_desc_diff(&desc)
                    .prop_map(move |diffs| (desc.clone(), diffs, datas.clone()))
            });

        proptest!(|((old, diffs, datas) in strat)| {
            test_case(old, diffs, datas);
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn empty_relation_desc_roundtrips() {
        let empty = RelationDesc::empty();
        let rows = proptest::collection::vec(arb_source_data_for_relation_desc(&empty), 0..8)
            .prop_map(move |datas| (empty.clone(), datas));

        // Note: This case should be covered by the `all_source_data_roundtrips` test above, but
        // it's a special case that we explicitly want to exercise.
        proptest!(|((config, (desc, source_datas)) in (any::<EncodingConfig>(), rows))| {
            roundtrip_source_data(desc, source_datas, &config);
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn arrow_datatype_consistent() {
        fn test_case(desc: RelationDesc, datas: Vec<SourceData>) {
            let half = datas.len() / 2;

            let mut encoder_a = <RelationDesc as Schema2<SourceData>>::encoder(&desc).unwrap();
            for data in &datas[..half] {
                encoder_a.append(data);
            }
            let col_a = encoder_a.finish();

            let mut encoder_b = <RelationDesc as Schema2<SourceData>>::encoder(&desc).unwrap();
            for data in &datas[half..] {
                encoder_b.append(data);
            }
            let col_b = encoder_b.finish();

            // The DataType of the resulting column should not change based on what data was
            // encoded.
            assert_eq!(col_a.data_type(), col_b.data_type());
        }

        let num_rows = 12;
        let strat = any::<RelationDesc>().prop_flat_map(|desc| {
            proptest::collection::vec(arb_source_data_for_relation_desc(&desc), num_rows)
                .prop_map(move |datas| (desc.clone(), datas))
        });

        proptest!(|((desc, data) in strat)| {
            test_case(desc, data);
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn source_proto_serialization_stability() {
        let min_protos = 10;
        let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);
        let encoded = include_str!("snapshots/source-datas.txt");

        // Decode the pre-generated source datas
        let mut decoded: Vec<(RelationDesc, SourceData)> = encoded
            .lines()
            .map(|s| {
                let (desc, data) = s.split_once(',').expect("comma separated data");
                let desc = base64::decode_config(desc, base64_config).expect("valid base64");
                let data = base64::decode_config(data, base64_config).expect("valid base64");
                (desc, data)
            })
            .map(|(desc, data)| {
                let desc = ProtoRelationDesc::decode(&desc[..]).expect("valid proto");
                let desc = desc.into_rust().expect("valid proto");
                let data = SourceData::decode(&data, &desc).expect("valid proto");
                (desc, data)
            })
            .collect();

        // If there are fewer than the minimum examples, generate some new ones arbitrarily
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let strategy = RelationDesc::arbitrary().prop_flat_map(|desc| {
            arb_source_data_for_relation_desc(&desc).prop_map(move |data| (desc.clone(), data))
        });
        while decoded.len() < min_protos {
            let arbitrary_data = strategy
                .new_tree(&mut runner)
                .expect("source data")
                .current();
            decoded.push(arbitrary_data);
        }

        // Reencode and compare the strings
        let mut reencoded = String::new();
        let mut buf = vec![];
        for (desc, data) in decoded {
            buf.clear();
            desc.into_proto().encode(&mut buf).expect("success");
            base64::encode_config_buf(buf.as_slice(), base64_config, &mut reencoded);
            reencoded.push(',');

            buf.clear();
            data.encode(&mut buf);
            base64::encode_config_buf(buf.as_slice(), base64_config, &mut reencoded);
            reencoded.push('\n');
        }

        // Optimizations in Persist, particularly consolidation on read,
        // depend on a stable serialization for the serialized data.
        // For example, reordering proto fields could cause us
        // to generate a different (equivalent) serialization for a record,
        // and the two versions would not consolidate out.
        // This can impact correctness!
        //
        // If you need to change how SourceDatas are encoded, that's still fine...
        // but we'll also need to increase
        // the MINIMUM_CONSOLIDATED_VERSION as part of the same release.
        assert_eq!(
            encoded,
            reencoded.as_str(),
            "SourceData serde should be stable"
        )
    }
}
