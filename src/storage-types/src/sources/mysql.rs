// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to mysql sources

use core::ops::Add;
use std::fmt;

use once_cell::sync::Lazy;
use proptest::prelude::{any, Arbitrary, BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};
use uuid::Uuid;

use mz_proto::{IntoRustIfSome, RustType, TryFromProtoError};
use mz_repr::{ColumnType, Datum, GlobalId, RelationDesc, Row, ScalarType};
use mz_timely_util::order::Partitioned;

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::sources::{SourceConnection, SourceTimestamp};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.mysql.rs"
));

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceConnection<C: ConnectionAccess = InlinedConnection> {
    pub connection_id: GlobalId,
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

impl<C: ConnectionAccess> Arbitrary for MySqlSourceConnection<C> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<C::MySql>(),
            any::<GlobalId>(),
            any::<MySqlSourceDetails>(),
        )
            .prop_map(|(connection, connection_id, details)| Self {
                connection,
                connection_id,
                details,
            })
            .boxed()
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

    fn timestamp_desc(&self) -> RelationDesc {
        MYSQL_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<GlobalId> {
        Some(self.connection_id)
    }

    fn metadata_columns(&self) -> Vec<(&str, ColumnType)> {
        vec![]
    }
}

// TODO(roshan): implement alter comaptibility logic
impl<C: ConnectionAccess> crate::AlterCompatible for MySqlSourceConnection<C> {}

impl RustType<ProtoMySqlSourceConnection> for MySqlSourceConnection {
    fn into_proto(&self) -> ProtoMySqlSourceConnection {
        ProtoMySqlSourceConnection {
            connection: Some(self.connection.into_proto()),
            connection_id: Some(self.connection_id.into_proto()),
            details: Some(self.details.into_proto()),
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
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MySqlSourceDetails {
    pub tables: Vec<mz_mysql_util::MySqlTableDesc>,
}

impl Arbitrary for MySqlSourceDetails {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        proptest::collection::vec(any::<mz_mysql_util::MySqlTableDesc>(), 1..4)
            .prop_map(|tables| Self { tables })
            .boxed()
    }
}

impl RustType<ProtoMySqlSourceDetails> for MySqlSourceDetails {
    fn into_proto(&self) -> ProtoMySqlSourceDetails {
        ProtoMySqlSourceDetails {
            tables: self.tables.iter().map(|t| t.into_proto()).collect(),
        }
    }

    fn from_proto(proto: ProtoMySqlSourceDetails) -> Result<Self, TryFromProtoError> {
        Ok(MySqlSourceDetails {
            tables: proto
                .tables
                .into_iter()
                .map(mz_mysql_util::MySqlTableDesc::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// Represents a MySQL transaction id
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum TransactionId {
    // Represents a transaction id that is not yet known
    Absent,

    // Represents the Active transaction_id that has been seen
    // This requires that the source is configured to apply updates in order
    // so we can assume all transactions were monotonically increasing and consecutive
    Active(u64),
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionId::Absent => write!(f, "Null"),
            TransactionId::Active(id) => write!(f, "{}", id),
        }
    }
}

impl From<TransactionId> for u64 {
    fn from(id: TransactionId) -> Self {
        match id {
            // TransactionIds start at 1, so we can use 0 to represent the absent id
            TransactionId::Absent => 0,
            TransactionId::Active(id) => id,
        }
    }
}

impl TransactionId {
    pub const MAX: TransactionId = TransactionId::Active(u64::MAX);
}

impl Add for TransactionId {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match self {
            TransactionId::Absent => other,
            TransactionId::Active(id) => match other {
                TransactionId::Absent => self,
                TransactionId::Active(other_id) => TransactionId::Active(id + other_id),
            },
        }
    }
}

impl Timestamp for TransactionId {
    // No need to describe complex summaries
    type Summary = ();

    fn minimum() -> Self {
        TransactionId::Absent
    }
}

impl From<&TransactionId> for u64 {
    fn from(id: &TransactionId) -> Self {
        match id {
            // TransactionIds start at 1, so we can use 0 to represent the absent id
            TransactionId::Absent => 0,
            TransactionId::Active(id) => *id,
        }
    }
}

impl TotalOrder for TransactionId {}

impl PartialOrder for TransactionId {
    fn less_equal(&self, other: &Self) -> bool {
        Into::<u64>::into(self).less_equal(&Into::<u64>::into(other))
    }
}

impl PathSummary<TransactionId> for () {
    fn results_in(&self, src: &TransactionId) -> Option<TransactionId> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl Refines<()> for TransactionId {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) -> () {}

    fn summarize(_path: Self::Summary) -> <() as Timestamp>::Summary {}
}

/// This type is used to track the progress of the ingestion dataflow, and represents
/// a 'Gtid Set' of transactions that have been committed on a MySQL cluster.
/// Each Gtid corresponds to a specific committed transaction, and contains the SourceId (a UUID)
/// of the server that committed the transaction, and the TransactionId (a u64) representing
/// the transaction's position.
/// This type partitions by SourceId and contains the highest TransactionId committed for each
/// SourceId 'range' in the Gtid Set. We use the highest TransactionId rather than a set of intervals
/// because we validate that the MySQL server has been configured maintain ordering of transactions
/// within a source, and that the TransactionIds are monotonically increasing.
/// A None TransactionId represents that no transactions have been seen for the corresponding
/// sources (which is useful to represent a frontier of all future possible sources)
pub type GtidPartition = Partitioned<Uuid, TransactionId>;

impl SourceTimestamp for GtidPartition {
    fn encode_row(&self) -> Row {
        let ts = match self.timestamp() {
            TransactionId::Absent => Datum::Null,
            TransactionId::Active(id) => Datum::from(*id),
        };
        Row::pack(&[
            Datum::from(self.interval().lower),
            Datum::from(self.interval().upper),
            ts,
        ])
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next(), datums.next(), datums.next()) {
            (Some(Datum::Uuid(lower)), Some(Datum::Uuid(upper)), Some(Datum::UInt64(ts)), None) => {
                Partitioned::new_range(lower, upper, TransactionId::Active(ts))
            }
            (Some(Datum::Uuid(lower)), Some(Datum::Uuid(upper)), Some(Datum::Null), None) => {
                Partitioned::new_range(lower, upper, TransactionId::Absent)
            }
            _ => panic!("invalid row {row:?}"),
        }
    }
}
