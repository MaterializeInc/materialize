// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to mysql sources

use std::fmt;
use std::io;
use std::num::NonZeroU64;

use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{ColumnType, Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_timely_util::order::Partitioned;
use mz_timely_util::order::Step;
use once_cell::sync::Lazy;
use proptest::prelude::any;
use proptest::strategy::Strategy;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use timely::progress::Antichain;
use uuid::Uuid;

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{SourceConnection, SourceTimestamp};
use crate::AlterCompatible;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.mysql.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlColumnRef {
    pub schema_name: String,
    pub table_name: String,
    pub column_name: String,
}

impl RustType<ProtoMySqlColumnRef> for MySqlColumnRef {
    fn into_proto(&self) -> ProtoMySqlColumnRef {
        ProtoMySqlColumnRef {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
            column_name: self.column_name.clone(),
        }
    }

    fn from_proto(proto: ProtoMySqlColumnRef) -> Result<Self, TryFromProtoError> {
        Ok(MySqlColumnRef {
            schema_name: proto.schema_name,
            table_name: proto.table_name,
            column_name: proto.column_name,
        })
    }
}

pub struct IntoMySqlColumnRefError(String);

impl fmt::Display for IntoMySqlColumnRefError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<UnresolvedItemName> for MySqlColumnRef {
    type Error = IntoMySqlColumnRefError;

    /// Convert an UnresolvedItemName into a MySqlColumnRef
    fn try_from(item: UnresolvedItemName) -> Result<Self, Self::Error> {
        let mut iter = item.0.into_iter();
        match (iter.next(), iter.next(), iter.next()) {
            (Some(schema_name), Some(table_name), Some(column_name)) => Ok(MySqlColumnRef {
                // We need to be careful not to accidentally introduce postgres/mz-style
                // quoting in the schema_name, table_name, or column_name fields, which
                // happens if naively using the to_string()/fmt method on the Ident values.
                schema_name: schema_name.into_string(),
                table_name: table_name.into_string(),
                column_name: column_name.into_string(),
            }),
            ref other => Err(IntoMySqlColumnRefError(format!(
                "expected fully-qualified column name, got: {:?}",
                other
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
    pub connection: C::MySql,
    pub details: MySqlSourceDetails,
    pub text_columns: Vec<MySqlColumnRef>,
    pub ignore_columns: Vec<MySqlColumnRef>,
}

impl<R: ConnectionResolver> IntoInlineConnection<MySqlSourceConnection, R>
    for MySqlSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> MySqlSourceConnection {
        let MySqlSourceConnection {
            connection_id,
            connection,
            details,
            text_columns,
            ignore_columns,
        } = self;

        MySqlSourceConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_mysql(),
            details,
            text_columns,
            ignore_columns,
        }
    }
}

pub static MYSQL_PROGRESS_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("source_id_lower", ScalarType::Uuid.nullable(false))
        .with_column("source_id_upper", ScalarType::Uuid.nullable(false))
        .with_column("transaction_id", ScalarType::UInt64.nullable(true))
});

impl<C: ConnectionAccess> SourceConnection for MySqlSourceConnection<C> {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn upstream_name(&self) -> Option<&str> {
        None
    }

    fn key_desc(&self) -> RelationDesc {
        RelationDesc::empty()
    }

    fn value_desc(&self) -> RelationDesc {
        // The MySQL source only outputs data to its subsources. The catalog object
        // representing the source itself is just an empty relation with no columns
        RelationDesc::empty()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        MYSQL_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        Some(self.connection_id)
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        vec![]
    }

    fn output_idx_for_name(&self, name: &mz_sql_parser::ast::UnresolvedItemName) -> Option<usize> {
        self.details
            .tables
            .iter()
            .position(|t| {
                let inner = &name.0;
                t.schema_name == inner[0].as_str() && t.name == inner[1].as_str()
            })
            .map(|idx| idx + 1)
    }
}

impl<C: ConnectionAccess> AlterCompatible for MySqlSourceConnection<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let MySqlSourceConnection {
            connection_id,
            connection,
            details,
            text_columns,
            ignore_columns,
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (details == &other.details, "details"),
            (text_columns == &other.text_columns, "text_columns"),
            (ignore_columns == &other.ignore_columns, "ignore_columns"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "MySqlSourceConnection incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl RustType<ProtoMySqlSourceConnection> for MySqlSourceConnection {
    fn into_proto(&self) -> ProtoMySqlSourceConnection {
        ProtoMySqlSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            details: Some(self.details.into_proto()),
            text_columns: self.text_columns.into_proto(),
            ignore_columns: self.ignore_columns.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMySqlSourceConnection) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceConnection {
            connection: proto
                .connection
                .into_rust_if_some("ProtoMySqlSourceConnection::connection")?,
            connection_id: proto
                .connection_id
                .into_rust_if_some("ProtoMySqlSourceConnection::connection_id")?,
            details: proto
                .details
                .into_rust_if_some("ProtoMySqlSourceConnection::details")?,
            text_columns: proto.text_columns.into_rust()?,
            ignore_columns: proto.ignore_columns.into_rust()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct MySqlSourceDetails {
    #[proptest(
        strategy = "proptest::collection::vec(any::<mz_mysql_util::MySqlTableDesc>(), 0..4)"
    )]
    pub tables: Vec<mz_mysql_util::MySqlTableDesc>,
    /// The initial 'gtid_executed' set for the source. This is used as the effective
    /// snapshot point, to ensure consistency if the source is interrupted but commits
    /// one or more tables before the initial snapshot of all tables is complete.
    #[proptest(strategy = "any_gtidset()")]
    pub initial_gtid_set: String,
}

fn any_gtidset() -> impl Strategy<Value = String> {
    any::<(u128, u64)>().prop_map(|(uuid, tx_id)| format!("{}:{}", Uuid::from_u128(uuid), tx_id))
}

impl RustType<ProtoMySqlSourceDetails> for MySqlSourceDetails {
    fn into_proto(&self) -> ProtoMySqlSourceDetails {
        ProtoMySqlSourceDetails {
            tables: self.tables.iter().map(|t| t.into_proto()).collect(),
            initial_gtid_set: self.initial_gtid_set.clone(),
        }
    }

    fn from_proto(proto: ProtoMySqlSourceDetails) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceDetails {
            tables: proto
                .tables
                .into_iter()
                .map(mz_mysql_util::MySqlTableDesc::from_proto)
                .collect::<Result<_, _>>()?,
            initial_gtid_set: proto.initial_gtid_set,
        })
    }
}

/// Represents a MySQL transaction id
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum GtidState {
    // NOTE: The ordering of the variants is important for the derived order implementation
    /// Represents a MySQL server source-id that has not yet presented a GTID
    Absent,

    /// Represents an active MySQL server transaction-id for a given source.
    ///
    /// When used in a frontier / antichain, this represents the next transaction_id value that
    /// we expect to see for the corresponding source_id(s).
    /// When used as a timestamp, it represents an exact transaction_id value.
    Active(NonZeroU64),
}

impl fmt::Display for GtidState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GtidState::Absent => write!(f, "Absent"),
            GtidState::Active(id) => write!(f, "{}", id),
        }
    }
}

impl GtidState {
    pub const MAX: GtidState = GtidState::Active(NonZeroU64::MAX);
}

impl Timestamp for GtidState {
    // No need to describe complex summaries
    type Summary = ();

    fn minimum() -> Self {
        GtidState::Absent
    }
}

impl TotalOrder for GtidState {}

impl PartialOrder for GtidState {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl PathSummary<GtidState> for () {
    fn results_in(&self, src: &GtidState) -> Option<GtidState> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl Refines<()> for GtidState {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) -> () {}

    fn summarize(_path: Self::Summary) -> <() as Timestamp>::Summary {}
}

/// This type is used to represent the the progress of each MySQL GTID 'source_id' in the
/// ingestion dataflow.
///
/// A MySQL GTID consists of a source_id (UUID) and transaction_id (non-zero u64).
///
/// For the purposes of this documentation effort, the term "source" refers to a MySQL
/// server that is being replicated from:
/// <https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html>
///
/// Each source_id represents a unique MySQL server, and the transaction_id
/// monotonically increases for each source, representing the position of the transaction
/// relative to other transactions on the same source.
///
/// Paritioining is by source_id which can be a singular UUID to represent a single source_id
/// or a range of UUIDs to represent multiple sources.
///
/// The value of the partition is the NEXT transaction_id that we expect to see for the
/// corresponding source(s), represented a GtidState::Next(transaction_id).
///
/// GtidState::Absent represents that no transactions have been seen for the
/// corresponding source(s).
///
/// A complete Antichain of this type represents a frontier of the future transactions
/// that we might see.
pub type GtidPartition = Partitioned<Uuid, GtidState>;

impl SourceTimestamp for GtidPartition {
    fn encode_row(&self) -> Row {
        let ts = match self.timestamp() {
            GtidState::Absent => Datum::Null,
            GtidState::Active(id) => Datum::UInt64(id.get()),
        };
        Row::pack(&[
            Datum::Uuid(self.interval().lower),
            Datum::Uuid(self.interval().upper),
            ts,
        ])
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next(), datums.next(), datums.next()) {
            (Some(Datum::Uuid(lower)), Some(Datum::Uuid(upper)), Some(Datum::UInt64(ts)), None) => {
                match ts {
                    0 => Partitioned::new_range(lower, upper, GtidState::Absent),
                    ts => Partitioned::new_range(
                        lower,
                        upper,
                        GtidState::Active(NonZeroU64::new(ts).unwrap()),
                    ),
                }
            }
            (Some(Datum::Uuid(lower)), Some(Datum::Uuid(upper)), Some(Datum::Null), None) => {
                Partitioned::new_range(lower, upper, GtidState::Absent)
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}

/// Parses a GTID Set string received from a MySQL server (e.g. from @@gtid_purged or @@gtid_executed).
///
/// Returns the frontier of all future GTIDs that are not contained in the provided GTID set.
///
/// This includes singlular partitions that represent each UUID seen in the GTID Set, and range
/// partitions that represent the missing UUIDs between the singular partitions, which are
/// each set to GtidState::Absent.
///
/// TODO(roshan): Add compatibility for MySQL 8.3 'Tagged' GTIDs
pub fn gtid_set_frontier(gtid_set_str: &str) -> Result<Antichain<GtidPartition>, io::Error> {
    let mut partitions = Antichain::new();
    let mut gap_lower = Some(Uuid::nil());
    for mut gtid_str in gtid_set_str.split(',') {
        if gtid_str.is_empty() {
            continue;
        };
        gtid_str = gtid_str.trim();
        let (uuid, intervals) = gtid_str.split_once(':').ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid gtid: {}", gtid_str),
            )
        })?;

        let uuid = Uuid::parse_str(uuid).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid uuid in gtid: {}: {}", uuid, e),
            )
        })?;

        // From the MySQL docs:
        // "When GTID sets are returned from server variables, UUIDs are in alphabetical order,
        // and numeric intervals are merged and in ascending order."
        // For our purposes, we need to ensure that all intervals are consecutive which means there
        // should be at most one interval per GTID.
        // TODO: should this same restriction be done when parsing a @@GTID_PURGED value? In that
        // case the intervals might not be guaranteed to be consecutive, depending on how purging
        // is implemented?
        let mut intervals = intervals.split(':');
        let end = match (intervals.next(), intervals.next()) {
            (Some(interval_str), None) => {
                let mut vals_iter = interval_str.split('-').map(str::parse::<u64>);
                let start = vals_iter
                    .next()
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("couldn't parse int: {}", interval_str),
                        )
                    })?
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("couldn't parse int: {}: {}", interval_str, e),
                        )
                    })?;
                match vals_iter.next() {
                    Some(Ok(end)) => end,
                    None => start,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid gtid interval: {}", interval_str),
                        ))
                    }
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("gtid with non-consecutive intervals found! {}", gtid_str),
                ))
            }
        };
        // Create a partition representing all the UUIDs in the gap between the previous one and this one
        if let Some(gap_upper) = uuid.backward_checked(1) {
            let gap_lower = gap_lower.expect("uuids are in alphabetical order");
            if gap_upper >= gap_lower {
                partitions.insert(GtidPartition::new_range(
                    gap_lower,
                    gap_upper,
                    GtidState::Absent,
                ));
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "gtid set not presented in alphabetical uuid order: {}",
                        gtid_set_str
                    ),
                ));
            }
        }
        gap_lower = uuid.forward_checked(1);
        // Insert a partition representing the 'next' GTID that might be seen from this source
        partitions.insert(GtidPartition::new_singleton(
            uuid,
            GtidState::Active(NonZeroU64::new(end + 1).unwrap()),
        ));
    }

    // Add the final range partition if there is a gap between the last partition and the maximum
    if let Some(gap_lower) = gap_lower {
        partitions.insert(GtidPartition::new_range(
            gap_lower,
            Uuid::max(),
            GtidState::Absent,
        ));
    }

    Ok(partitions)
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::num::NonZeroU64;

    #[mz_ore::test]
    fn test_gtid_set_frontier_valid() {
        let gtid_set_str =
            "14c1b43a-eb64-11eb-8a9a-0242ac130002:1, 2174B383-5441-11E8-B90A-C80AA9429562:1-3, 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-19";
        let result = gtid_set_frontier(gtid_set_str).unwrap();
        assert_eq!(result.len(), 7);
        assert_eq!(
            result,
            Antichain::from_iter(vec![
                GtidPartition::new_range(
                    Uuid::nil(),
                    Uuid::parse_str("14c1b43a-eb64-11eb-8a9a-0242ac130001").unwrap(),
                    GtidState::Absent,
                ),
                GtidPartition::new_singleton(
                    Uuid::parse_str("14c1b43a-eb64-11eb-8a9a-0242ac130002").unwrap(),
                    GtidState::Active(NonZeroU64::new(2).unwrap()),
                ),
                GtidPartition::new_range(
                    Uuid::parse_str("14c1b43a-eb64-11eb-8a9a-0242ac130003").unwrap(),
                    Uuid::parse_str("2174B383-5441-11E8-B90A-C80AA9429561").unwrap(),
                    GtidState::Absent,
                ),
                GtidPartition::new_singleton(
                    Uuid::parse_str("2174B383-5441-11E8-B90A-C80AA9429562").unwrap(),
                    GtidState::Active(NonZeroU64::new(4).unwrap()),
                ),
                GtidPartition::new_range(
                    Uuid::parse_str("2174B383-5441-11E8-B90A-C80AA9429563").unwrap(),
                    Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429561").unwrap(),
                    GtidState::Absent,
                ),
                GtidPartition::new_singleton(
                    Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429562").unwrap(),
                    GtidState::Active(NonZeroU64::new(20).unwrap()),
                ),
                GtidPartition::new_range(
                    Uuid::parse_str("3E11FA47-71CA-11E1-9E33-C80AA9429563").unwrap(),
                    Uuid::max(),
                    GtidState::Absent,
                ),
            ]),
        )
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_non_alphabetical_uuids() {
        let gtid_set_str =
            "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-19, 2174B383-5441-11E8-B90A-C80AA9429562:1-3";
        let result = gtid_set_frontier(gtid_set_str);
        assert!(result.is_err());
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_non_consecutive() {
        let gtid_set_str =
            "2174B383-5441-11E8-B90A-C80AA9429562:1-3:5-8, 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-19";
        let result = gtid_set_frontier(gtid_set_str);
        assert!(result.is_err());
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_invalid_uuid() {
        let gtid_set_str =
            "14c1b43a-eb64-11eb-8a9a-0242ac130002:1-5,24DA167-0C0C-11E8-8442-00059A3C7B00:1";
        let result = gtid_set_frontier(gtid_set_str);
        assert!(result.is_err());
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_invalid_interval() {
        let gtid_set_str =
            "14c1b43a-eb64-11eb-8a9a-0242ac130002:1-5,14c1b43a-eb64-11eb-8a9a-0242ac130003:1-3:4";
        let result = gtid_set_frontier(gtid_set_str);
        assert!(result.is_err());
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_empty_string() {
        let gtid_set_str = "";
        let result = gtid_set_frontier(gtid_set_str).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result,
            Antichain::from_elem(GtidPartition::new_range(
                Uuid::nil(),
                Uuid::max(),
                GtidState::Absent,
            ))
        );
    }
}
