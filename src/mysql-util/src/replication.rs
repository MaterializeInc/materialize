// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::MySqlError;

/// Query a MySQL System Variable
pub async fn query_sys_var(conn: &mut Conn, name: &str) -> Result<String, MySqlError> {
    let value: String = conn
        .query_first(format!("SELECT @@{}", name))
        .await?
        .unwrap();
    Ok(value)
}

/// Verify a MySQL System Variable matches the expected value
async fn verify_sys_setting(
    conn: &mut Conn,
    setting: &str,
    expected: &str,
) -> Result<(), MySqlError> {
    match query_sys_var(conn, setting).await?.as_str() {
        actual if actual == expected => Ok(()),
        actual => Err(MySqlError::InvalidSystemSetting {
            setting: setting.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
        }),
    }
}

pub async fn ensure_full_row_binlog_format(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "log_bin", "1").await?;
    verify_sys_setting(conn, "binlog_format", "ROW").await?;
    verify_sys_setting(conn, "binlog_row_image", "FULL").await?;
    Ok(())
}

pub async fn ensure_gtid_consistency(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "gtid_mode", "ON").await?;
    verify_sys_setting(conn, "enforce_gtid_consistency", "ON").await?;
    Ok(())
}

// TODO: This only applies if connecting to a replica, to ensure that the replica preserves
// all transactions in the order they were committed on the primary which implies
// monotonically increasing transaction ids
pub async fn ensure_replication_commit_order(conn: &mut Conn) -> Result<(), MySqlError> {
    // This system variable was renamed between MySQL 5.7 and 8.0
    match verify_sys_setting(conn, "replica_preserve_commit_order", "1").await {
        Ok(_) => Ok(()),
        Err(_) => verify_sys_setting(conn, "slave_preserve_commit_order", "1").await,
    }
}

/// Represents either a GTID Interval or a single GTID point
/// If this is a single GTID point, start == end
#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq)]
pub struct GtidInterval {
    pub start: u64,
    pub end: u64,
}

impl fmt::Display for GtidInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A singular GTID interval is represented as a single point
        if self.start != self.end {
            write!(f, "{}-{}", self.start, self.end)
        } else {
            write!(f, "{}", self.start)
        }
    }
}

impl FromStr for GtidInterval {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vals: Vec<u64> = s
            .split('-')
            .map(|num| num.parse::<u64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid interval in gtid: {}: {}", s, e),
                )
            })?;
        if vals.len() == 1 {
            Ok(Self {
                start: vals[0],
                end: vals[0],
            })
        } else if vals.len() == 2 {
            Ok(Self {
                start: vals[0],
                end: vals[1],
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid gtid interval: {}", s),
            ))
        }
    }
}

/// Represents a MySQL GTID with a single Source-ID and a list of intervals
/// e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:4:5-9`
/// If this represents a single GTID point e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:5`
/// it will contain just one interval with the same start and end values
/// By convention, intervals will be non-overlapping and sorted in ascending order by start
/// value and consolidated to combine consecutive intervals
#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq)]
pub struct Gtid {
    pub uuid: uuid::Uuid,
    intervals: Vec<GtidInterval>,
}

impl Gtid {
    pub fn new(uuid: Uuid) -> Self {
        Self {
            uuid,
            intervals: vec![],
        }
    }

    pub fn earliest_transaction_id(&self) -> u64 {
        self.intervals
            .first()
            .unwrap_or_else(|| {
                panic!(
                    "expected at least one interval in gtid: {:?}",
                    self.intervals
                )
            })
            .start
    }

    pub fn latest_transaction_id(&self) -> u64 {
        // NOTE: Use the last interval; there might be discrete intervals if they have not been compacted yet
        // but interval values are guaranteed to be monotonically increasing as long as replica_preserve_commit_order=1
        // which is why we only care about the highest value
        let last_interval = self.intervals.last().unwrap_or_else(|| {
            panic!(
                "expected at least one interval in gtid: {:?}",
                self.intervals
            )
        });
        last_interval.end
    }

    pub fn add_interval(&mut self, new: GtidInterval) -> &mut Self {
        if let Some(last) = self.intervals.last_mut() {
            // Only allow adding the interval if it is after the last interval
            assert!(
                last.end < new.start,
                "interval must be after the last interval"
            );

            if new.start == last.end + 1 {
                // If the interval starts right after the last interval ends, just extend the last interval
                last.end = new.end;
            } else {
                self.intervals.push(new);
            }
        } else {
            // This is the first interval
            self.intervals.push(new);
        }

        self
    }

    pub fn intervals(&self) -> impl Iterator<Item = &GtidInterval> {
        self.intervals.iter()
    }
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            self.uuid
                .hyphenated()
                .encode_upper(&mut Uuid::encode_buffer()),
        )?;
        for interval in &self.intervals {
            write!(f, ":")?;
            interval.fmt(f)?;
        }
        fmt::Result::Ok(())
    }
}

impl FromStr for Gtid {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uuid, intervals) = s.split_once(':').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid gtid: {}", s))
        })?;
        let uuid = Uuid::parse_str(uuid).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid uuid in gtid: {}: {}", s, e),
            )
        })?;

        let mut new = Self {
            uuid,
            intervals: vec![],
        };
        for interval_str in intervals.split(':') {
            // add_interval() will consolidate consecutive intervals to save representation space
            // and make comparisons easier, even if the MySQL server has not yet compressed the intervals
            new.add_interval(GtidInterval::from_str(interval_str)?);
        }
        Ok(new)
    }
}

/// A representation of a MySQL GTID set (returned from the `gtid_executed` & `gtid_purged` system variables)
/// e.g. `2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19`
#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq)]
pub struct GtidSet {
    gtids: BTreeMap<Uuid, Gtid>,
}

impl GtidSet {
    pub fn new() -> Self {
        Self {
            gtids: BTreeMap::new(),
        }
    }

    pub fn add_gtid(&mut self, new: Gtid) -> &Self {
        if let Some(existing) = self.gtids.get_mut(&new.uuid) {
            for interval in new.intervals {
                existing.add_interval(interval);
            }
        } else {
            self.gtids.insert(new.uuid, new);
        }
        self
    }

    pub fn first(&self) -> Option<&Gtid> {
        self.gtids.values().next()
    }

    /// Returns an iterator over all GTIDs in the set
    /// ordered by UUID
    pub fn gtids(self) -> impl Iterator<Item = Gtid> {
        self.gtids.into_iter().map(|(_, gtid)| gtid)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, gtid) in self.gtids.values().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            gtid.fmt(f)?;
        }
        fmt::Result::Ok(())
    }
}

impl FromStr for GtidSet {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut new = Self {
            gtids: BTreeMap::new(),
        };

        for gtid_str in s.split(", ") {
            // add_gtid() will consolidate gtids to save representation space
            new.add_gtid(Gtid::from_str(gtid_str)?);
        }

        Ok(new)
    }
}
