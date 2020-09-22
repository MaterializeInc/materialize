// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility routines for data representation.

use std::time::Duration;

use crate::strconv;

/// Parses a [`Duration`] from a string.
///
/// The accepted syntax for a [`Duration`] is exactly the same syntax accepted
/// by the SQL interval type, i.e., as parsed by [`strconv::parse_interval`],
/// except that negative intervals are rejected as they are not representible in
/// Rust's duration type.
///
// NOTE: This does not belong in the `strconv` module, which is only for
// converting to/from types that are directly used by SQL. This function is for
// Rust code that wants to parse a Rust duration using SQL-ish syntax, e.g.,
// parsing durations passed as CLI arguments.
pub fn parse_duration(s: &str) -> Result<Duration, anyhow::Error> {
    strconv::parse_interval(s)?.duration()
}
