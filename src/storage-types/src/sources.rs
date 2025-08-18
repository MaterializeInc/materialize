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
use kafka::KafkaSourceExportDetails;
use load_generator::{LoadGeneratorOutput, LoadGeneratorSourceExportDetails};
use mz_ore::assert_none;
use mz_persist_types::Codec;
use mz_persist_types::arrow::ArrayOrd;
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema};
use mz_persist_types::stats::{
    ColumnNullStats, ColumnStatKinds, ColumnarStats, ColumnarStatsBuilder, PrimitiveStats,
    StructStats,
};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{
    CatalogItemId, Datum, GlobalId, ProtoRelationDesc, ProtoRow, RelationDesc, Row,
    RowColumnarDecoder, RowColumnarEncoder, arb_row_for_relation,
};
use mz_sql_parser::ast::{Ident, IdentError, UnresolvedItemName};
use proptest::prelude::any;
use proptest::strategy::Strategy;
use prost::Message;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::errors::{DataflowError, ProtoDataflowError};
use crate::instances::StorageInstanceId;
use crate::sources::sql_server::SqlServerSourceExportDetails;

pub mod encoding;
pub mod envelope;
pub mod kafka;
pub mod load_generator;
pub mod mysql;
pub mod postgres;
pub mod sql_server;

pub use crate::sources::envelope::SourceEnvelope;
pub use crate::sources::kafka::KafkaSourceConnection;
pub use crate::sources::load_generator::LoadGeneratorSourceConnection;
pub use crate::sources::mysql::{MySqlSourceConnection, MySqlSourceExportDetails};
pub use crate::sources::postgres::{PostgresSourceConnection, PostgresSourceExportDetails};
pub use crate::sources::sql_server::{SqlServerSource, SqlServerSourceExtras};

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.sources.rs"));

/// A description of a source ingestion
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct IngestionDescription<S: 'static = (), C: ConnectionAccess = InlinedConnection> {
    /// The source description.
    pub desc: SourceDesc<C>,
    /// Additional storage controller metadata needed to ingest this source
    pub ingestion_metadata: S,
    /// Collections to be exported by this ingestion.
    ///
    /// # Notes
    /// - For multi-output sources:
    ///     - Add exports by adding a new [`SourceExport`].
    ///     - Remove exports by removing the [`SourceExport`].
    ///
    ///   Re-rendering/executing the source after making these modifications
    ///   adds and drops the subsource, respectively.
    /// - This field includes the primary source's ID, which might need to be
    ///   filtered out to understand which exports are explicit ingestion exports.
    /// - This field does _not_ include the remap collection, which is tracked
    ///   in its own field.
    pub source_exports: BTreeMap<GlobalId, SourceExport<S>>,
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
    /// Return an iterator over the `GlobalId`s of `self`'s collections.
    /// This will contain ids for the remap collection, subsources,
    /// tables for this source, and the primary collection ID, even if
    /// no data will be exported to the primary collection.
    pub fn collection_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
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
                                    storage_metadata: l_metadata,
                                    details: l_details,
                                    data_config: l_data_config,
                                },
                            ),
                            (
                                _,
                                SourceExport {
                                    storage_metadata: r_metadata,
                                    details: r_details,
                                    data_config: r_data_config,
                                },
                            ),
                        ) => {
                            l_metadata.alter_compatible(id, r_metadata).is_ok()
                                && l_details.alter_compatible(id, r_details).is_ok()
                                && l_data_config.alter_compatible(id, r_data_config).is_ok()
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceExport<S = (), C: ConnectionAccess = InlinedConnection> {
    /// The collection metadata needed to write the exported data
    pub storage_metadata: S,
    /// Details necessary for the source to export data to this export's collection.
    pub details: SourceExportDetails,
    /// Config necessary to handle (e.g. decode and envelope) the data for this export.
    pub data_config: SourceExportDataConfig<C>,
}

pub trait SourceTimestamp:
    Timestamp + Columnation + Refines<()> + std::fmt::Display + Sync
{
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
    Copy, Clone, Default, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize,
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
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Hash)]
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

    /// Defines the key schema to use by default for this source connection type.
    /// This will be used for the primary export of the source and as the default
    /// pre-encoding key schema for the source.
    fn default_key_desc(&self) -> RelationDesc;

    /// Defines the value schema to use by default for this source connection type.
    /// This will be used for the primary export of the source and as the default
    /// pre-encoding value schema for the source.
    fn default_value_desc(&self) -> RelationDesc;

    /// The schema of this connection's timestamp type. This will also be the schema of the
    /// progress relation.
    fn timestamp_desc(&self) -> RelationDesc;

    /// The id of the connection object (i.e the one obtained from running `CREATE CONNECTION`) in
    /// the catalog, if any.
    fn connection_id(&self) -> Option<CatalogItemId>;

    /// If this source connection can output to a primary collection, contains the source-specific
    /// details of that export, else is set to `SourceExportDetails::None` to indicate that
    /// this source should not export to the primary collection.
    fn primary_export_details(&self) -> SourceExportDetails;

    /// Whether the source type supports read only mode.
    fn supports_read_only(&self) -> bool;

    /// Whether the source type prefers to run on only one replica of a multi-replica cluster.
    fn prefers_single_replica(&self) -> bool;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Compression {
    Gzip,
    None,
}

/// Defines the configuration for how to handle data that is exported for a given
/// Source Export.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceExportDataConfig<C: ConnectionAccess = InlinedConnection> {
    pub encoding: Option<encoding::SourceDataEncoding<C>>,
    pub envelope: SourceEnvelope,
}

impl<R: ConnectionResolver> IntoInlineConnection<SourceExportDataConfig, R>
    for SourceExportDataConfig<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SourceExportDataConfig {
        let SourceExportDataConfig { encoding, envelope } = self;

        SourceExportDataConfig {
            encoding: encoding.map(|e| e.into_inline_connection(r)),
            envelope,
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for SourceExportDataConfig<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let Self { encoding, envelope } = &self;

        let compatibility_checks = [
            (
                match (encoding, &other.encoding) {
                    (Some(s), Some(o)) => s.alter_compatible(id, o).is_ok(),
                    (s, o) => s == o,
                },
                "encoding",
            ),
            (envelope == &other.envelope, "envelope"),
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

impl<C: ConnectionAccess> SourceExportDataConfig<C> {
    /// Returns `true` if this connection yields data that is
    /// append-only/monotonic. Append-monly means the source
    /// never produces retractions.
    // TODO(guswynn): consider enforcing this more completely at the
    // parsing/typechecking level, by not using an `envelope`
    // for sources like pg
    pub fn monotonic(&self, connection: &GenericSourceConnection<C>) -> bool {
        match &self.envelope {
            // Upsert and CdcV2 may produce retractions.
            SourceEnvelope::Upsert(_) | SourceEnvelope::CdcV2 => false,
            SourceEnvelope::None(_) => {
                match connection {
                    // Postgres can produce retractions (deletes).
                    GenericSourceConnection::Postgres(_) => false,
                    // MySQL can produce retractions (deletes).
                    GenericSourceConnection::MySql(_) => false,
                    // SQL Server can produce retractions (deletes).
                    GenericSourceConnection::SqlServer(_) => false,
                    // Whether or not a Loadgen source can produce retractions varies.
                    GenericSourceConnection::LoadGenerator(g) => g.load_generator.is_monotonic(),
                    // Kafka exports with `None` envelope are append-only.
                    GenericSourceConnection::Kafka(_) => true,
                }
            }
        }
    }
}

/// An external source of updates for a relational collection.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceDesc<C: ConnectionAccess = InlinedConnection> {
    pub connection: GenericSourceConnection<C>,
    pub timestamp_interval: Duration,
    /// The data encoding and format of data to export to the
    /// primary collection for this source.
    /// TODO(database-issues#8620): This will be removed once sources no longer export
    /// to primary collections and only export to explicit SourceExports (tables).
    pub primary_export: SourceExportDataConfig<C>,
    pub primary_export_details: SourceExportDetails,
}

impl<R: ConnectionResolver> IntoInlineConnection<SourceDesc, R>
    for SourceDesc<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SourceDesc {
        let SourceDesc {
            connection,
            primary_export,
            primary_export_details,
            timestamp_interval,
        } = self;

        SourceDesc {
            connection: connection.into_inline_connection(&r),
            primary_export: primary_export.into_inline_connection(r),
            primary_export_details,
            timestamp_interval,
        }
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
            primary_export,
            primary_export_details,
            timestamp_interval,
        } = &self;

        let compatibility_checks = [
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (primary_export == &other.primary_export, "primary_export"),
            (
                primary_export_details == &other.primary_export_details,
                "primary_export_details",
            ),
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

impl SourceDesc<InlinedConnection> {
    /// Returns the SourceExport details for the primary export.
    /// TODO(database-issues#8620): This will be removed once sources no longer export
    /// to primary collections and only export to explicit SourceExports (tables).
    pub fn primary_source_export(&self) -> SourceExport<(), InlinedConnection> {
        SourceExport {
            storage_metadata: (),
            details: self.primary_export_details.clone(),
            data_config: self.primary_export.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GenericSourceConnection<C: ConnectionAccess = InlinedConnection> {
    Kafka(KafkaSourceConnection<C>),
    Postgres(PostgresSourceConnection<C>),
    MySql(MySqlSourceConnection<C>),
    SqlServer(SqlServerSource<C>),
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

impl<C: ConnectionAccess> From<SqlServerSource<C>> for GenericSourceConnection<C> {
    fn from(conn: SqlServerSource<C>) -> Self {
        Self::SqlServer(conn)
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
            GenericSourceConnection::SqlServer(sql_server) => {
                GenericSourceConnection::SqlServer(sql_server.into_inline_connection(r))
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
            Self::SqlServer(conn) => conn.name(),
            Self::LoadGenerator(conn) => conn.name(),
        }
    }

    fn external_reference(&self) -> Option<&str> {
        match self {
            Self::Kafka(conn) => conn.external_reference(),
            Self::Postgres(conn) => conn.external_reference(),
            Self::MySql(conn) => conn.external_reference(),
            Self::SqlServer(conn) => conn.external_reference(),
            Self::LoadGenerator(conn) => conn.external_reference(),
        }
    }

    fn default_key_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.default_key_desc(),
            Self::Postgres(conn) => conn.default_key_desc(),
            Self::MySql(conn) => conn.default_key_desc(),
            Self::SqlServer(conn) => conn.default_key_desc(),
            Self::LoadGenerator(conn) => conn.default_key_desc(),
        }
    }

    fn default_value_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.default_value_desc(),
            Self::Postgres(conn) => conn.default_value_desc(),
            Self::MySql(conn) => conn.default_value_desc(),
            Self::SqlServer(conn) => conn.default_value_desc(),
            Self::LoadGenerator(conn) => conn.default_value_desc(),
        }
    }

    fn timestamp_desc(&self) -> RelationDesc {
        match self {
            Self::Kafka(conn) => conn.timestamp_desc(),
            Self::Postgres(conn) => conn.timestamp_desc(),
            Self::MySql(conn) => conn.timestamp_desc(),
            Self::SqlServer(conn) => conn.timestamp_desc(),
            Self::LoadGenerator(conn) => conn.timestamp_desc(),
        }
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        match self {
            Self::Kafka(conn) => conn.connection_id(),
            Self::Postgres(conn) => conn.connection_id(),
            Self::MySql(conn) => conn.connection_id(),
            Self::SqlServer(conn) => conn.connection_id(),
            Self::LoadGenerator(conn) => conn.connection_id(),
        }
    }

    fn primary_export_details(&self) -> SourceExportDetails {
        match self {
            Self::Kafka(conn) => conn.primary_export_details(),
            Self::Postgres(conn) => conn.primary_export_details(),
            Self::MySql(conn) => conn.primary_export_details(),
            Self::SqlServer(conn) => conn.primary_export_details(),
            Self::LoadGenerator(conn) => conn.primary_export_details(),
        }
    }

    fn supports_read_only(&self) -> bool {
        match self {
            GenericSourceConnection::Kafka(conn) => conn.supports_read_only(),
            GenericSourceConnection::Postgres(conn) => conn.supports_read_only(),
            GenericSourceConnection::MySql(conn) => conn.supports_read_only(),
            GenericSourceConnection::SqlServer(conn) => conn.supports_read_only(),
            GenericSourceConnection::LoadGenerator(conn) => conn.supports_read_only(),
        }
    }

    fn prefers_single_replica(&self) -> bool {
        match self {
            GenericSourceConnection::Kafka(conn) => conn.prefers_single_replica(),
            GenericSourceConnection::Postgres(conn) => conn.prefers_single_replica(),
            GenericSourceConnection::MySql(conn) => conn.prefers_single_replica(),
            GenericSourceConnection::SqlServer(conn) => conn.prefers_single_replica(),
            GenericSourceConnection::LoadGenerator(conn) => conn.prefers_single_replica(),
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
            (Self::SqlServer(conn), Self::SqlServer(other)) => conn.alter_compatible(id, other),
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

/// Details necessary for each source export to allow the source implementations
/// to export data to the export's collection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SourceExportDetails {
    /// Used when the primary collection of a source isn't an export to
    /// output to.
    None,
    Kafka(KafkaSourceExportDetails),
    Postgres(PostgresSourceExportDetails),
    MySql(MySqlSourceExportDetails),
    SqlServer(SqlServerSourceExportDetails),
    LoadGenerator(LoadGeneratorSourceExportDetails),
}

impl crate::AlterCompatible for SourceExportDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }
        let r = match (self, other) {
            (Self::None, Self::None) => Ok(()),
            (Self::Kafka(s), Self::Kafka(o)) => s.alter_compatible(id, o),
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

/// Details necessary to store in the `Details` option of a source export
/// statement (`CREATE SUBSOURCE` and `CREATE TABLE .. FROM SOURCE` statements),
/// to generate the appropriate `SourceExportDetails` struct during planning.
/// NOTE that this is serialized as proto to the catalog, so any changes here
/// must be backwards compatible or will require a migration.
pub enum SourceExportStatementDetails {
    Postgres {
        table: mz_postgres_util::desc::PostgresTableDesc,
    },
    MySql {
        table: mz_mysql_util::MySqlTableDesc,
        initial_gtid_set: String,
    },
    SqlServer {
        table: mz_sql_server_util::desc::SqlServerTableDesc,
        capture_instance: Arc<str>,
        initial_lsn: mz_sql_server_util::cdc::Lsn,
    },
    LoadGenerator {
        output: LoadGeneratorOutput,
    },
    Kafka {},
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
            SourceExportStatementDetails::SqlServer {
                table,
                capture_instance,
                initial_lsn,
            } => ProtoSourceExportStatementDetails {
                kind: Some(proto_source_export_statement_details::Kind::SqlServer(
                    sql_server::ProtoSqlServerSourceExportStatementDetails {
                        table: Some(table.into_proto()),
                        capture_instance: capture_instance.to_string(),
                        initial_lsn: initial_lsn.as_bytes().to_vec(),
                    },
                )),
            },
            SourceExportStatementDetails::LoadGenerator { output } => {
                ProtoSourceExportStatementDetails {
                    kind: Some(proto_source_export_statement_details::Kind::Loadgen(
                        load_generator::ProtoLoadGeneratorSourceExportStatementDetails {
                            output: output.into_proto().into(),
                        },
                    )),
                }
            }
            SourceExportStatementDetails::Kafka {} => ProtoSourceExportStatementDetails {
                kind: Some(proto_source_export_statement_details::Kind::Kafka(
                    kafka::ProtoKafkaSourceExportStatementDetails {},
                )),
            },
        }
    }

    fn from_proto(proto: ProtoSourceExportStatementDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_export_statement_details::Kind;
        Ok(match proto.kind {
            Some(Kind::Postgres(details)) => SourceExportStatementDetails::Postgres {
                table: details
                    .table
                    .into_rust_if_some("ProtoPostgresSourceExportStatementDetails::table")?,
            },
            Some(Kind::Mysql(details)) => SourceExportStatementDetails::MySql {
                table: details
                    .table
                    .into_rust_if_some("ProtoMySqlSourceExportStatementDetails::table")?,

                initial_gtid_set: details.initial_gtid_set,
            },
            Some(Kind::SqlServer(details)) => SourceExportStatementDetails::SqlServer {
                table: details
                    .table
                    .into_rust_if_some("ProtoSqlServerSourceExportStatementDetails::table")?,
                capture_instance: details.capture_instance.into(),
                initial_lsn: mz_sql_server_util::cdc::Lsn::try_from(details.initial_lsn.as_slice())
                    .map_err(|e| TryFromProtoError::InvalidFieldError(e.to_string()))?,
            },
            Some(Kind::Loadgen(details)) => SourceExportStatementDetails::LoadGenerator {
                output: details
                    .output
                    .into_rust_if_some("ProtoLoadGeneratorSourceExportStatementDetails::output")?,
            },
            Some(Kind::Kafka(_details)) => SourceExportStatementDetails::Kafka {},
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoSourceExportStatementDetails::kind",
                ));
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

    fn validate(val: &Self, desc: &Self::Schema) -> Result<(), String> {
        match &val.0 {
            Ok(row) => Row::validate(row, desc),
            Err(_) => Ok(()),
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
pub fn arb_source_data_for_relation_desc(
    desc: &RelationDesc,
) -> impl Strategy<Value = SourceData> + use<> {
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

impl ExternalCatalogReference for &mz_mysql_util::MySqlTableDesc {
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

impl ExternalCatalogReference for &mz_sql_server_util::desc::SqlServerTableDesc {
    fn schema_name(&self) -> &str {
        &*self.schema_name
    }

    fn item_name(&self) -> &str {
        &*self.name
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
        "reference {name} is ambiguous, consider specifying an additional \
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
    /// - `name` is `&[Ident]` to let users provide the inner element of
    ///   [`UnresolvedItemName`].
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
    /// `name` is `&[Ident]` to let users provide the inner element of
    /// [`UnresolvedItemName`].
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
    /// `name` is `&[Ident]` to let users provide the inner element of
    /// [`UnresolvedItemName`].
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

    pub fn goodbytes(&self) -> usize {
        match self {
            SourceDataRowColumnarDecoder::Row(decoder) => decoder.goodbytes(),
            SourceDataRowColumnarDecoder::EmptyRow => 0,
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

    fn goodbytes(&self) -> usize {
        self.row_decoder.goodbytes() + ArrayOrd::Binary(self.err_decoder.clone()).goodbytes()
    }

    fn stats(&self) -> StructStats {
        let len = self.err_decoder.len();
        let err_stats = ColumnarStats {
            nulls: Some(ColumnNullStats {
                count: self.err_decoder.null_count(),
            }),
            values: PrimitiveStats::<Vec<u8>>::from_column(&self.err_decoder).into(),
        };
        // The top level struct is non-nullable and every entry is either an
        // `Ok(Row)` or an `Err(String)`. As a result, we can compute the number
        // of `Ok` entries by subtracting the number of `Err` entries from the
        // total count.
        let row_null_count = len - self.err_decoder.null_count();
        let row_stats = match &self.row_decoder {
            SourceDataRowColumnarDecoder::Row(encoder) => {
                // Sanity check that the number of row nulls/nones we calculated
                // using the error column matches what the row column thinks it
                // has.
                assert_eq!(encoder.null_count(), row_null_count);
                encoder.stats()
            }
            SourceDataRowColumnarDecoder::EmptyRow => StructStats {
                len,
                cols: BTreeMap::default(),
            },
        };
        let row_stats = ColumnarStats {
            nulls: Some(ColumnNullStats {
                count: row_null_count,
            }),
            values: ColumnStatKinds::Struct(row_stats),
        };

        let stats = [
            (
                SourceDataColumnarEncoder::OK_COLUMN_NAME.to_string(),
                row_stats,
            ),
            (
                SourceDataColumnarEncoder::ERR_COLUMN_NAME.to_string(),
                err_stats,
            ),
        ];
        StructStats {
            len,
            cols: stats.into_iter().map(|(name, s)| (name, s)).collect(),
        }
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
    pub(crate) fn goodbytes(&self) -> usize {
        match self {
            SourceDataRowColumnarEncoder::Row(e) => e.goodbytes(),
            SourceDataRowColumnarEncoder::EmptyRow => 0,
        }
    }

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

    fn goodbytes(&self) -> usize {
        self.row_encoder.goodbytes() + self.err_encoder.values_slice().len()
    }

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

impl Schema<SourceData> for RelationDesc {
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
    use arrow::array::{ArrayData, make_comparator};
    use base64::Engine;
    use bytes::Bytes;
    use mz_expr::EvalError;
    use mz_ore::assert_err;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::indexed::columnar::arrow::{realloc_any, realloc_array};
    use mz_persist::metrics::ColumnarMetrics;
    use mz_persist_types::parquet::EncodingConfig;
    use mz_persist_types::schema::{Migration, backward_compatible};
    use mz_persist_types::stats::{PartStats, PartStatsMetrics};
    use mz_repr::{
        ColumnIndex, DatumVec, PropRelationDescDiff, ProtoRelationDesc, RelationDescBuilder,
        RowArena, ScalarType, arb_relation_desc_diff, arb_relation_desc_projection,
    };
    use proptest::prelude::*;
    use proptest::strategy::{Union, ValueTree};

    use crate::stats::RelationPartStats;

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
    fn roundtrip_source_data(
        desc: &RelationDesc,
        datas: Vec<SourceData>,
        read_desc: &RelationDesc,
        config: &EncodingConfig,
    ) {
        let metrics = ColumnarMetrics::disconnected();
        let mut encoder = <RelationDesc as Schema<SourceData>>::encoder(desc).unwrap();
        for data in &datas {
            encoder.append(data);
        }
        let col = encoder.finish();

        // The top-level StructArray for SourceData should always be non-nullable.
        assert!(!col.is_nullable());

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
        let rnd_col = realloc_any(Arc::clone(rnd_col), &metrics);
        let rnd_col = rnd_col
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();

        // Try generating stats for the data, just to make sure we don't panic.
        let stats = <RelationDesc as Schema<SourceData>>::decoder_any(desc, &rnd_col)
            .expect("valid decoder")
            .stats();

        // Read back all of our data and assert it roundtrips.
        let mut rnd_data = SourceData(Ok(Row::default()));
        let decoder = <RelationDesc as Schema<SourceData>>::decoder(desc, rnd_col.clone()).unwrap();
        for (idx, og_data) in datas.iter().enumerate() {
            decoder.decode(idx, &mut rnd_data);
            assert_eq!(og_data, &rnd_data);
        }

        // Read back all of our data a second time with a projection applied, and make sure the
        // stats are valid.
        let stats_metrics = PartStatsMetrics::new(&MetricsRegistry::new());
        let stats = RelationPartStats {
            name: "test",
            metrics: &stats_metrics,
            stats: &PartStats { key: stats },
            desc: read_desc,
        };
        let mut datum_vec = DatumVec::new();
        let arena = RowArena::default();
        let decoder = <RelationDesc as Schema<SourceData>>::decoder(read_desc, rnd_col).unwrap();

        for (idx, og_data) in datas.iter().enumerate() {
            decoder.decode(idx, &mut rnd_data);
            match (&og_data.0, &rnd_data.0) {
                (Ok(og_row), Ok(rnd_row)) => {
                    // Filter down to just the Datums in the projection schema.
                    {
                        let datums = datum_vec.borrow_with(og_row);
                        let projected_datums =
                            datums.iter().enumerate().filter_map(|(idx, datum)| {
                                read_desc
                                    .contains_index(&ColumnIndex::from_raw(idx))
                                    .then_some(datum)
                            });
                        let og_projected_row = Row::pack(projected_datums);
                        assert_eq!(&og_projected_row, rnd_row);
                    }

                    // Validate the stats for all of our projected columns.
                    {
                        let proj_datums = datum_vec.borrow_with(rnd_row);
                        for (pos, (idx, _, _)) in read_desc.iter_all().enumerate() {
                            let spec = stats.col_stats(idx, &arena);
                            assert!(spec.may_contain(proj_datums[pos]));
                        }
                    }
                }
                (Err(_), Err(_)) => assert_eq!(og_data, &rnd_data),
                (_, _) => panic!("decoded to a different type? {og_data:?} {rnd_data:?}"),
            }
        }

        // Verify that the RelationDesc itself roundtrips through
        // {encode,decode}_schema.
        let encoded_schema = SourceData::encode_schema(desc);
        let roundtrip_desc = SourceData::decode_schema(&encoded_schema);
        assert_eq!(desc, &roundtrip_desc);

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

        // TODO(parkmycar): There are so many clones going on here, and maybe we can avoid them?
        let strat = (any::<RelationDesc>(), num_rows)
            .prop_flat_map(|(desc, num_rows)| {
                arb_relation_desc_projection(desc.clone())
                    .prop_map(move |read_desc| (desc.clone(), read_desc, num_rows.clone()))
            })
            .prop_flat_map(|(desc, read_desc, num_rows)| {
                proptest::collection::vec(arb_source_data_for_relation_desc(&desc), num_rows)
                    .prop_map(move |datas| (desc.clone(), datas, read_desc.clone()))
            });

        proptest!(|((config, (desc, source_datas, read_desc)) in (any::<EncodingConfig>(), strat))| {
            roundtrip_source_data(&desc, source_datas, &read_desc, &config);
        });
    }

    #[mz_ore::test]
    fn roundtrip_error_nulls() {
        let desc = RelationDescBuilder::default()
            .with_column(
                "ts",
                ScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .finish();
        let source_datas = vec![SourceData(Err(DataflowError::EvalError(
            EvalError::DateOutOfRange.into(),
        )))];
        let config = EncodingConfig::default();
        roundtrip_source_data(&desc, source_datas, &desc, &config);
    }

    fn is_sorted(array: &dyn Array) -> bool {
        let sort_options = arrow::compute::SortOptions::default();
        let Ok(cmp) = make_comparator(array, array, sort_options) else {
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

    fn get_data_type(schema: &impl Schema<SourceData>) -> arrow::datatypes::DataType {
        use mz_persist_types::columnar::ColumnEncoder;
        let array = Schema::encoder(schema).expect("valid schema").finish();
        Array::data_type(&array).clone()
    }

    #[track_caller]
    fn backward_compatible_testcase(
        old: &RelationDesc,
        new: &RelationDesc,
        migration: Migration,
        datas: &[SourceData],
    ) {
        let mut encoder = Schema::<SourceData>::encoder(old).expect("valid schema");
        for data in datas {
            encoder.append(data);
        }
        let old = encoder.finish();
        let new = Schema::<SourceData>::encoder(new)
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
    fn backward_compatible_project_away_all() {
        let old = RelationDesc::from_names_and_types([("a", ScalarType::Bool.nullable(true))]);
        let new = RelationDesc::empty();

        let old_data_type = get_data_type(&old);
        let new_data_type = get_data_type(&new);

        let migration = backward_compatible(&old_data_type, &new_data_type);
        assert!(migration.is_some());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn backward_compatible_migrate() {
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
        use mz_repr::ColumnType;
        fn test_case(old: RelationDesc, diffs: Vec<PropRelationDescDiff>, datas: Vec<SourceData>) {
            // TODO(parkmycar): As we iterate on schema migrations more things should become compatible.
            let should_be_compatible = diffs.iter().all(|diff| match diff {
                // We only support adding nullable columns.
                PropRelationDescDiff::AddColumn {
                    typ: ColumnType { nullable, .. },
                    ..
                } => *nullable,
                PropRelationDescDiff::DropColumn { .. } => true,
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
            roundtrip_source_data(&desc, source_datas, &desc, &config);
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn arrow_datatype_consistent() {
        fn test_case(desc: RelationDesc, datas: Vec<SourceData>) {
            let half = datas.len() / 2;

            let mut encoder_a = <RelationDesc as Schema<SourceData>>::encoder(&desc).unwrap();
            for data in &datas[..half] {
                encoder_a.append(data);
            }
            let col_a = encoder_a.finish();

            let mut encoder_b = <RelationDesc as Schema<SourceData>>::encoder(&desc).unwrap();
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
        let encoded = include_str!("snapshots/source-datas.txt");

        // Decode the pre-generated source datas
        let mut decoded: Vec<(RelationDesc, SourceData)> = encoded
            .lines()
            .map(|s| {
                let (desc, data) = s.split_once(',').expect("comma separated data");
                let desc = base64::engine::general_purpose::STANDARD
                    .decode(desc)
                    .expect("valid base64");
                let data = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .expect("valid base64");
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
            base64::engine::general_purpose::STANDARD.encode_string(buf.as_slice(), &mut reencoded);
            reencoded.push(',');

            buf.clear();
            data.encode(&mut buf);
            base64::engine::general_purpose::STANDARD.encode_string(buf.as_slice(), &mut reencoded);
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
