// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::ops::Add;
use std::fmt;

use mz_repr::{Datum, Row};
use mz_storage_types::sources::SourceTimestamp;
use serde::{Deserialize, Serialize};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::{PathSummary, Refines, Timestamp};

// TODO: Implement GTID set parsing -> Partitioned timestamp
// like vitess: https://github.com/vitessio/vitess/blob/main/go/mysql/replication/mysql56_gtid_set.go

/// Represents a MySQL transaction id. Its maximum value
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TransactionId(u64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<TransactionId> for u64 {
    fn from(id: TransactionId) -> Self {
        id.0
    }
}

impl TransactionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub const MAX: TransactionId = TransactionId(u64::MAX);
}

impl Add for TransactionId {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self::new(self.0 + other.0)
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
    fn encode_row(&self) -> Row {
        Row::pack([Datum::UInt64(self.0)])
    }

    fn decode_row(row: &Row) -> Self {
        let mut datums = row.iter();
        match (datums.next(), datums.next()) {
            (Some(Datum::UInt64(id)), None) => Self::new(id),
            _ => panic!("invalid row {row:?}"),
        }
    }
}
