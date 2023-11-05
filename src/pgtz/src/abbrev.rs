// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// An abbreviation for a time zone.
pub struct TimezoneAbbrev {
    /// The abbreviation for a fixed offset from UTC.
    pub abbrev: &'static str,
    /// The number of seconds offset from UTC, where positive means east of
    /// Greenwich.
    pub utc_offset_secs: i32,
    /// Whether this is a daylight saving abbreviation.
    pub is_dst: bool,
}

include!(concat!(env!("OUT_DIR"), "/abbrev.gen.rs"));

/// A SQL `VALUES` clause containing all known time zone abbreviations.
pub const TIMEZONE_ABBREV_SQL: &str = include_str!(concat!(env!("OUT_DIR"), "/abbrev.gen.sql"));
