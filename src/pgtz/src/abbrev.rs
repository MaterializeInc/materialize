// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::FixedOffset;
use chrono_tz::Tz;
use uncased::UncasedStr;

use crate::timezone::Timezone;

/// An abbreviation for a time zone.
pub struct TimezoneAbbrev {
    /// The name of the abbreviation.
    pub abbrev: &'static str,
    /// What the abbreviation specifies.
    pub spec: TimezoneAbbrevSpec,
}

impl TimezoneAbbrev {
    /// Returns the [`Timezone`] that the abbreviation specifies.
    pub fn timezone(&self) -> Timezone {
        match &self.spec {
            TimezoneAbbrevSpec::FixedOffset { offset, .. } => Timezone::FixedOffset(*offset),
            TimezoneAbbrevSpec::Tz(tz) => Timezone::Tz(*tz),
        }
    }
}

/// What a [`TimezoneAbbrev`] can specify.
pub enum TimezoneAbbrevSpec {
    /// A fixed offset from UTC.
    FixedOffset {
        /// The offset from UTC.
        offset: FixedOffset,
        /// Whether this is abbreviation represents a Daylight Saving Time
        /// offset.
        is_dst: bool,
    },
    /// A reference to a time zone in the IANA Time Zone Database.
    Tz(Tz),
}

const fn make_fixed_offset(utc_offset_secs: i32) -> FixedOffset {
    // `unwrap()` is not stable in `const` contexts, so manually match.
    match FixedOffset::east_opt(utc_offset_secs) {
        None => panic!("invalid fixed offset"),
        Some(fixed_offset) => fixed_offset,
    }
}

include!(concat!(env!("OUT_DIR"), "/abbrev.gen.rs"));

/// The SQL definition of the contents of the `mz_timezone_abbreviations` view.
pub const MZ_CATALOG_TIMEZONE_ABBREVIATIONS_SQL: &str =
    include_str!(concat!(env!("OUT_DIR"), "/abbrev.gen.sql"));
