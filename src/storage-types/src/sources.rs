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
use std::time::Duration;

use bytes::BufMut;
use itertools::EitherOrBoth::Both;
use itertools::Itertools;
use mz_persist_types::columnar::{
    ColumnFormat, ColumnGet, ColumnPush, Data, DataType, PartDecoder, PartEncoder, Schema,
};
use mz_persist_types::dyn_struct::{DynStruct, DynStructCfg, ValidityMut, ValidityRef};
use mz_persist_types::stats::StatsFn;
use mz_persist_types::Codec;
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{
    ColumnType, Datum, DatumDecoderT, DatumEncoderT, GlobalId, ProtoRow, RelationDesc, Row,
    RowDecoder, RowEncoder,
};
use mz_sql_parser::ast::UnresolvedItemName;
use proptest::prelude::any;
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
pub use crate::sources::mysql::MySqlSourceConnection;
pub use crate::sources::postgres::PostgresSourceConnection;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.sources.rs"));

/// A description of a source ingestion
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct IngestionDescription<S: 'static = (), C: ConnectionAccess = InlinedConnection> {
    /// The source description
    pub desc: SourceDesc<C>,
    /// Additional storage controller metadata needed to ingest this source
    pub ingestion_metadata: S,
    /// Collections to be exported by this ingestion.
    ///
    /// This field includes the primary source's ID, which might need to be
    /// filtered out to understand which exports are data-bearing subsources.
    ///
    /// Note that this does _not_ include the remap collection, which is tracked
    /// in its own field.
    #[proptest(
        strategy = "proptest::collection::btree_map(any::<GlobalId>(), any::<SourceExport<S>>(), 0..4)"
    )]
    pub source_exports: BTreeMap<GlobalId, SourceExport<S>>,
    /// The ID of the instance in which to install the source.
    pub instance_id: StorageInstanceId,
    /// The ID of this ingestion's remap/progress collection.
    pub remap_collection_id: GlobalId,
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
                                    output_index: _,
                                    storage_metadata: l_metadata,
                                },
                            ),
                            (
                                _,
                                SourceExport {
                                    output_index: _,
                                    storage_metadata: r_metadata,
                                },
                            ),
                        ) => {
                            // the output index may change, but the table's metadata
                            // may not
                            l_metadata.alter_compatible(id, r_metadata).is_ok()
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
pub struct SourceExport<S = ()> {
    /// The index of the exported output stream
    pub output_index: usize,
    /// The collection metadata needed to write the exported data
    pub storage_metadata: S,
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

pub trait SourceTimestamp: timely::progress::Timestamp + Refines<()> + std::fmt::Display {
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
    fn upstream_name(&self) -> Option<&str>;

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

    /// Returns the output position for `name`if this source contains it.
    fn output_idx_for_name(&self, name: &UnresolvedItemName) -> Option<usize>;
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

    fn upstream_name(&self) -> Option<&str> {
        match self {
            Self::Kafka(conn) => conn.upstream_name(),
            Self::Postgres(conn) => conn.upstream_name(),
            Self::MySql(conn) => conn.upstream_name(),
            Self::LoadGenerator(conn) => conn.upstream_name(),
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

    fn output_idx_for_name(&self, name: &UnresolvedItemName) -> Option<usize> {
        match self {
            Self::Kafka(conn) => conn.output_idx_for_name(name),
            Self::Postgres(conn) => conn.output_idx_for_name(name),
            Self::MySql(conn) => conn.output_idx_for_name(name),
            Self::LoadGenerator(conn) => conn.output_idx_for_name(name),
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

#[derive(Arbitrary, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct SourceData(pub Result<Row, DataflowError>);

#[cfg(test)]
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

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoSourceData::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }

    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        storage: &mut Option<ProtoRow>,
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
                let ret = row.decode_from_proto(&proto);
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
}

/// An implementation of [PartEncoder] for [SourceData].
///
/// This mostly delegates the encoding logic to [RowEncoder], but flatmaps in
/// an Err column.
#[derive(Debug)]
pub struct SourceDataEncoder<'a> {
    len: &'a mut usize,
    ok_validity: ValidityMut<'a>,
    ok: RowEncoder<'a>,
    err: &'a mut <Option<Vec<u8>> as Data>::Mut,
}

impl<'a> PartEncoder<'a, SourceData> for SourceDataEncoder<'a> {
    fn encode(&mut self, val: &SourceData) {
        *self.len += 1;
        match val.as_ref() {
            Ok(row) => {
                self.ok_validity.push(true);
                self.ok.inc_len();
                for (encoder, datum) in self.ok.col_encoders().iter_mut().zip(row.iter()) {
                    encoder.encode(datum);
                }
                ColumnPush::<Option<Vec<u8>>>::push(self.err, None);
            }
            Err(err) => {
                self.ok_validity.push(false);
                self.ok.inc_len();
                for encoder in self.ok.col_encoders() {
                    encoder.encode_default();
                }
                let err = err.into_proto().encode_to_vec();
                ColumnPush::<Option<Vec<u8>>>::push(self.err, Some(err.as_slice()));
            }
        }
    }
}

/// An implementation of [PartDecoder] for [SourceData].
///
/// This mostly delegates the encoding logic to [RowDecoder], but flatmaps in
/// an Err column.
#[derive(Debug)]
pub struct SourceDataDecoder<'a> {
    ok_validity: ValidityRef<'a>,
    ok: RowDecoder<'a>,
    err: &'a <Option<Vec<u8>> as Data>::Col,
}

impl<'a> PartDecoder<'a, SourceData> for SourceDataDecoder<'a> {
    fn decode(&self, idx: usize, val: &mut SourceData) {
        let err = ColumnGet::<Option<Vec<u8>>>::get(self.err, idx);
        match (self.ok_validity.get(idx), err) {
            (true, None) => {
                let mut packer = match val.0.as_mut() {
                    Ok(x) => x.packer(),
                    Err(_) => {
                        val.0 = Ok(Row::default());
                        val.0.as_mut().unwrap().packer()
                    }
                };
                for decoder in self.ok.col_decoders() {
                    decoder.decode(idx, &mut packer);
                }
            }
            (false, Some(err)) => {
                let err = ProtoDataflowError::decode(err)
                    .expect("proto should be valid")
                    .into_rust()
                    .expect("error should be valid");
                val.0 = Err(err);
            }
            (true, Some(_)) | (false, None) => {
                panic!("SourceData should have exactly one of ok or err")
            }
        };
    }
}

impl Schema<SourceData> for RelationDesc {
    type Encoder<'a> = SourceDataEncoder<'a>;

    type Decoder<'a> = SourceDataDecoder<'a>;

    fn columns(&self) -> DynStructCfg {
        let ok_schema = Schema::<Row>::columns(self);
        let cols = vec![
            (
                "ok".to_owned(),
                DataType {
                    optional: true,
                    format: ColumnFormat::Struct(ok_schema),
                },
                StatsFn::Default,
            ),
            (
                "err".to_owned(),
                DataType {
                    optional: true,
                    format: ColumnFormat::Bytes,
                },
                StatsFn::Default,
            ),
        ];
        DynStructCfg::from(cols)
    }

    fn decoder<'a>(
        &self,
        mut cols: mz_persist_types::dyn_struct::ColumnsRef<'a>,
    ) -> Result<Self::Decoder<'a>, String> {
        let ok = cols.col::<Option<DynStruct>>("ok")?;
        let err = cols.col::<Option<Vec<u8>>>("err")?;
        let () = cols.finish()?;
        let (ok_validity, ok) = RelationDesc::decoder(self, ok.as_opt_ref())?;
        Ok(SourceDataDecoder {
            ok_validity,
            ok,
            err,
        })
    }

    fn encoder<'a>(
        &self,
        mut cols: mz_persist_types::dyn_struct::ColumnsMut<'a>,
    ) -> Result<Self::Encoder<'a>, String> {
        let ok = cols.col::<Option<DynStruct>>("ok")?;
        let err = cols.col::<Option<Vec<u8>>>("err")?;
        let (len, ()) = cols.finish()?;
        let (ok_validity, ok) = RelationDesc::encoder(self, ok.as_opt_mut())?;
        Ok(SourceDataEncoder {
            len,
            ok_validity,
            ok,
            err,
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::ScalarType;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;

    use crate::errors::EnvelopeError;

    use super::*;

    #[mz_ore::test]
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

    fn scalar_type_columnar_roundtrip(scalar_type: ScalarType) {
        use mz_persist_types::columnar::validate_roundtrip;
        let mut rows = Vec::new();
        for datum in scalar_type.interesting_datums() {
            rows.push(SourceData(Ok(Row::pack(std::iter::once(datum)))));
        }
        rows.push(SourceData(Err(EnvelopeError::Flat("foo".into()).into())));

        // Non-nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.clone().nullable(false));
        for row in rows.iter() {
            assert_eq!(validate_roundtrip(&schema, row), Ok(()));
        }

        // Nullable version of the column.
        let schema = RelationDesc::empty().with_column("col", scalar_type.nullable(true));
        rows.push(SourceData(Ok(Row::pack(std::iter::once(Datum::Null)))));
        for row in rows.iter() {
            assert_eq!(validate_roundtrip(&schema, row), Ok(()));
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn all_scalar_types_columnar_roundtrip() {
        proptest!(|(scalar_type in any::<ScalarType>())| {
            // The proptest! macro interferes with rustfmt.
            scalar_type_columnar_roundtrip(scalar_type)
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn source_proto_serialization_stability() {
        let min_protos = 10;
        let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);
        let encoded = include_str!("snapshots/source-datas.txt");

        // Decode the pre-generated source datas
        let mut decoded: Vec<SourceData> = encoded
            .lines()
            .map(|s| base64::decode_config(s, base64_config).expect("valid base64"))
            .map(|b| SourceData::decode(&b).expect("valid proto"))
            .collect();

        // If there are fewer than the minimum examples, generate some new ones arbitrarily
        while decoded.len() < min_protos {
            let mut runner = proptest::test_runner::TestRunner::deterministic();
            let arbitrary_data: SourceData = SourceData::arbitrary()
                .new_tree(&mut runner)
                .expect("source data")
                .current();
            decoded.push(arbitrary_data);
        }

        // Reencode and compare the strings
        let mut reencoded = String::new();
        let mut buf = vec![];
        for s in decoded {
            buf.clear();
            s.encode(&mut buf);
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
