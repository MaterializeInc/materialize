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
use std::sync::LazyLock;

use mz_proto::{RustType, TryFromProtoError};
use mz_repr::CatalogItemId;
use mz_repr::GlobalId;
use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};
use mz_timely_util::order::Partitioned;
use mz_timely_util::order::Step;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::Antichain;
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use uuid::Uuid;

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::sources::{SourceConnection, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.mysql.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: CatalogItemId,
    pub connection: C::MySql,
    pub details: MySqlSourceDetails,
}

impl<R: ConnectionResolver> IntoInlineConnection<MySqlSourceConnection, R>
    for MySqlSourceConnection<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> MySqlSourceConnection {
        let MySqlSourceConnection {
            connection_id,
            connection,
            details,
        } = self;

        MySqlSourceConnection {
            connection_id,
            connection: r.resolve_connection(connection).unwrap_mysql(),
            details,
        }
    }
}

pub static MYSQL_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("source_id_lower", SqlScalarType::Uuid.nullable(false))
        .with_column("source_id_upper", SqlScalarType::Uuid.nullable(false))
        .with_column("transaction_id", SqlScalarType::UInt64.nullable(true))
        .finish()
});

impl MySqlSourceConnection {
    pub async fn fetch_write_frontier(
        self,
        storage_configuration: &crate::configuration::StorageConfiguration,
    ) -> Result<timely::progress::Antichain<GtidPartition>, anyhow::Error> {
        let config = self
            .connection
            .config(
                &storage_configuration.connection_context.secrets_reader,
                storage_configuration,
                mz_ore::future::InTask::No,
            )
            .await?;

        let mut conn = config
            .connect(
                "mysql fetch_write_frontier",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;

        let current_gtid_set =
            mz_mysql_util::query_sys_var(&mut conn, "global.gtid_executed").await?;

        let current_upper = gtid_set_frontier(&current_gtid_set)?;

        Ok(current_upper)
    }
}

impl<C: ConnectionAccess> SourceConnection for MySqlSourceConnection<C> {
    fn name(&self) -> &'static str {
        "mysql"
    }

    fn external_reference(&self) -> Option<&str> {
        None
    }

    fn default_key_desc(&self) -> RelationDesc {
        RelationDesc::empty()
    }

    fn default_value_desc(&self) -> RelationDesc {
        // The MySQL source only outputs data to its subsources. The catalog object
        // representing the source itself is just an empty relation with no columns
        RelationDesc::empty()
    }

    fn timestamp_desc(&self) -> RelationDesc {
        MYSQL_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        Some(self.connection_id)
    }

    fn supports_read_only(&self) -> bool {
        false
    }

    fn prefers_single_replica(&self) -> bool {
        true
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
        } = self;

        let compatibility_checks = [
            (connection_id == &other.connection_id, "connection_id"),
            (
                connection.alter_compatible(id, &other.connection).is_ok(),
                "connection",
            ),
            (
                details.alter_compatible(id, &other.details).is_ok(),
                "details",
            ),
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

/// This struct allows storing any mysql-specific details for a source, serialized as
/// an option in the `CREATE SOURCE` statement. It was previously used but is not currently
/// necessary, though we keep it around to maintain conformity with other sources.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceDetails {}

impl RustType<ProtoMySqlSourceDetails> for MySqlSourceDetails {
    fn into_proto(&self) -> ProtoMySqlSourceDetails {
        ProtoMySqlSourceDetails {}
    }

    fn from_proto(_proto: ProtoMySqlSourceDetails) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceDetails {})
    }
}

impl AlterCompatible for MySqlSourceDetails {
    fn alter_compatible(
        &self,
        _id: GlobalId,
        _other: &Self,
    ) -> Result<(), crate::controller::AlterError> {
        Ok(())
    }
}

/// Specifies the details of a MySQL source export.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceExportDetails {
    pub table: mz_mysql_util::MySqlTableDesc,
    /// The initial 'gtid_executed' set for this export.
    /// This is used as the effective snapshot point for this export to ensure correctness
    /// if the source is interrupted but commits one or more tables before the initial snapshot
    /// of all tables is complete.
    pub initial_gtid_set: String,
    pub text_columns: Vec<String>,
    pub exclude_columns: Vec<String>,
}

impl AlterCompatible for MySqlSourceExportDetails {
    fn alter_compatible(
        &self,
        _id: GlobalId,
        _other: &Self,
    ) -> Result<(), crate::controller::AlterError> {
        // compatibility checks are performed against the upstream table in the source
        // render operators instead
        let Self {
            table: _,
            initial_gtid_set: _,
            text_columns: _,
            exclude_columns: _,
        } = self;
        Ok(())
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

/// This type is used to represent the progress of each MySQL GTID 'source_id' in the
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
                        ));
                    }
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("gtid with non-consecutive intervals found! {}", gtid_str),
                ));
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

    use mz_ore::assert_err;

    use super::*;
    use std::num::NonZeroU64;

    #[mz_ore::test]
    fn test_gtid_set_frontier_valid() {
        let gtid_set_str = "14c1b43a-eb64-11eb-8a9a-0242ac130002:1, 2174B383-5441-11E8-B90A-C80AA9429562:1-3, 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-19";
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
        assert_err!(result);
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_non_consecutive() {
        let gtid_set_str = "2174B383-5441-11E8-B90A-C80AA9429562:1-3:5-8, 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-19";
        let result = gtid_set_frontier(gtid_set_str);
        assert_err!(result);
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_invalid_uuid() {
        let gtid_set_str =
            "14c1b43a-eb64-11eb-8a9a-0242ac130002:1-5,24DA167-0C0C-11E8-8442-00059A3C7B00:1";
        let result = gtid_set_frontier(gtid_set_str);
        assert_err!(result);
    }

    #[mz_ore::test]
    fn test_gtid_set_frontier_invalid_interval() {
        let gtid_set_str =
            "14c1b43a-eb64-11eb-8a9a-0242ac130002:1-5,14c1b43a-eb64-11eb-8a9a-0242ac130003:1-3:4";
        let result = gtid_set_frontier(gtid_set_str);
        assert_err!(result);
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
