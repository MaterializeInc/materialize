// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A time Date abstract data type.

use std::convert::TryFrom;
use std::fmt;
use std::ops::Sub;

use anyhow::anyhow;
use chrono::NaiveDate;
use mz_proto::{RustType, TryFromProtoError};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use thiserror::Error;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.date.rs"));

#[derive(Debug, Error)]
pub enum DateError {
    #[error("data out of range")]
    OutOfRange,
}

/// A Postgres-compatible Date. Additionally clamp valid dates for the range
/// that chrono supports to allow for safe string operations. Infinite dates are
/// not yet supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Hash, Deserialize)]
pub struct Date {
    /// Number of days from the postgres epoch (2000-01-01).
    days: i32,
}

impl RustType<ProtoDate> for Date {
    fn into_proto(&self) -> ProtoDate {
        ProtoDate { days: self.days }
    }

    fn from_proto(proto: ProtoDate) -> Result<Self, TryFromProtoError> {
        Ok(Date { days: proto.days })
    }
}

impl std::str::FromStr for Date {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        crate::strconv::parse_date(s).map_err(|e| anyhow!(e))
    }
}

static PG_EPOCH: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd(2000, 1, 1));

impl Date {
    pub const UNIX_EPOCH_TO_PG_EPOCH: i32 = 10957; // Number of days from 1970-01-01 to 2000-01-01.
    const CE_EPOCH_TO_PG_EPOCH: i32 = 730120; // Number of days since 0001-01-01 to 2000-01-01.
    pub const LOW_DAYS: i32 = -2451545; // 4714-11-24 BC

    /// Largest date support by Materialize. Although Postgres can go up to
    /// 5874897-12-31, chrono is limited to December 31, 262143, which we mirror
    /// here so we can use chrono's formatting methods and have guaranteed safe
    /// conversions.
    pub const HIGH_DAYS: i32 = 95_015_644;

    /// Constructs a new `Date` as the days since the postgres epoch
    /// (2000-01-01).
    pub fn from_pg_epoch(days: i32) -> Result<Date, DateError> {
        if days < Self::LOW_DAYS || days > Self::HIGH_DAYS {
            Err(DateError::OutOfRange)
        } else {
            Ok(Date { days })
        }
    }

    /// Constructs a new `Date` as the days since the Unix epoch.
    pub fn from_unix_epoch(unix_days: i32) -> Result<Date, DateError> {
        let pg_days = unix_days.saturating_sub(Self::UNIX_EPOCH_TO_PG_EPOCH);
        if pg_days == i32::MIN {
            return Err(DateError::OutOfRange);
        }
        Self::from_pg_epoch(pg_days)
    }

    /// Returns the number of days since the postgres epoch.
    pub fn pg_epoch_days(&self) -> i32 {
        self.days
    }

    /// Returns whether this is the infinity or -infinity date.
    ///
    /// Currently we do not support these, so this function is a light
    /// protection against if they are added for functions that will produce
    /// incorrect results for these values.
    pub fn is_finite(&self) -> bool {
        self.days != i32::MAX && self.days != i32::MIN
    }

    /// Returns the number of days since the Unix epoch.
    pub fn unix_epoch_days(&self) -> i32 {
        assert!(self.is_finite());
        // Guaranteed to be safe because we clamp the high date by less than the
        // result of this.
        self.days + Self::UNIX_EPOCH_TO_PG_EPOCH
    }
}

impl Sub for Date {
    type Output = i32;

    fn sub(self, rhs: Self) -> Self::Output {
        assert!(self.is_finite());
        self.days - rhs.days
    }
}

impl From<Date> for NaiveDate {
    fn from(date: Date) -> Self {
        Self::from(&date)
    }
}

impl From<&Date> for NaiveDate {
    fn from(date: &Date) -> Self {
        let days = date
            .pg_epoch_days()
            .checked_add(Date::CE_EPOCH_TO_PG_EPOCH)
            .expect("out of range date are prevented");
        NaiveDate::from_num_days_from_ce(days)
    }
}

impl TryFrom<NaiveDate> for Date {
    type Error = DateError;

    fn try_from(value: NaiveDate) -> Result<Self, Self::Error> {
        let d = value.signed_duration_since(*PG_EPOCH);
        let days: i32 = d.num_days().try_into().map_err(|_| DateError::OutOfRange)?;
        Self::from_pg_epoch(days)
    }
}

/// Format an Date in a human form
impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let d: NaiveDate = (*self).into();
        d.format("%Y-%m-%d").fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_date() {
        let pgepoch = Date::from_pg_epoch(0).unwrap();
        let unixepoch = Date::from_unix_epoch(0).unwrap();
        assert_eq!(pgepoch.pg_epoch_days(), 0);
        assert_eq!(pgepoch.unix_epoch_days(), 10957);
        assert_eq!(unixepoch.pg_epoch_days(), -10957);
        assert_eq!(unixepoch.unix_epoch_days(), 0);
        assert_eq!(NaiveDate::from(pgepoch), NaiveDate::from_ymd(2000, 1, 1));
        assert_eq!(
            pgepoch,
            Date::try_from(NaiveDate::from_ymd(2000, 1, 1)).unwrap()
        );
        assert_eq!(
            unixepoch,
            Date::try_from(NaiveDate::from_ymd(1970, 1, 1)).unwrap()
        );
        assert_eq!(
            unixepoch,
            Date::try_from(NaiveDate::from_ymd(1970, 1, 1)).unwrap()
        );
        assert!(pgepoch > unixepoch);
    }
}
