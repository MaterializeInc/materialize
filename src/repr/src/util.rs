// Copyright Materialize, Inc. and contributors. All rights reserved.
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
/// except that negative intervals are rejected as they are not representable in
/// Rust's duration type.
///
// NOTE: This does not belong in the `strconv` module, which is only for
// converting to/from types that are directly used by SQL. This function is for
// Rust code that wants to parse a Rust duration using SQL-ish syntax, e.g.,
// parsing durations passed as CLI arguments.
pub fn parse_duration(s: &str) -> Result<Duration, anyhow::Error> {
    strconv::parse_interval(s)?.duration()
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_parse_duration() {
        let test_cases = vec![
            ("0", Duration::new(0, 0)),
            ("0s", Duration::new(0, 0)),
            ("1ms", Duration::new(0, 1_000_000)),
            ("1s", Duration::new(1, 0)),
            ("1m", Duration::new(60, 0)),
            ("1min", Duration::new(60, 0)),
            ("1h", Duration::new(3600, 0)),
        ];
        for test in test_cases {
            let d = parse_duration(test.0).unwrap();
            assert_eq!(d, test.1, "{}", test.0);
        }
    }

    #[test]
    fn test_parse_duration_error() {
        let test_cases = vec![
            ("-1s", "cannot convert negative interval to duration"),
            ("1month", "cannot convert interval with months to duration"),
        ];
        for test in test_cases {
            match parse_duration(test.0) {
                Ok(_) => panic!("expected error"),
                Err(err) => assert_eq!(test.1, err.to_string()),
            }
        }
    }
}
