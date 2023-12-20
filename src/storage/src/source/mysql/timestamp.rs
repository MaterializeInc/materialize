// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use mz_expr::PartitionId;
use mz_repr::{Datum, Row};
use mz_storage_types::sources::{MzOffset, SourceTimestamp};
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};

/// Represents a MySQL transaction id. Its maximum value
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TransactionId(i64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TransactionId {
    pub fn new(id: i64) -> Self {
        assert!(id >= 0, "invalid negative transaction id: {id}");
        Self(id)
    }
}

impl Timestamp for TransactionId {
    // No need to describe complex summaries
    type Summary = ();

    fn minimum() -> Self {
        TransactionId(0)
    }
}

impl TotalOrder for TransactionId {}

impl PartialOrder for TransactionId {
    fn less_equal(&self, other: &Self) -> bool {
        self.0.less_equal(&other.0)
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

impl SourceTimestamp for TransactionId {
    fn from_compat_ts(pid: PartitionId, offset: MzOffset) -> Self {
        assert_eq!(
            pid,
            PartitionId::None,
            "invalid non-partitioned partition {pid}"
        );
        let id: i64 = offset.offset.try_into().expect("invalid transaction id");
        Self::new(id)
    }

    fn try_into_compat_ts(&self) -> Option<(PartitionId, MzOffset)> {
        let id: u64 = self.0.try_into().expect("verified non-negative");
        Some((PartitionId::None, MzOffset::from(id)))
    }

    fn encode_row(&self) -> Row {
        Row::pack([Datum::Int64(self.0)])
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::Int64(id)), None) => Self::new(id),
            _ => panic!("invalid row {row:?}"),
        }
    }
}
