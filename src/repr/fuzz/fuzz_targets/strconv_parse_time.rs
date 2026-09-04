// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_time` parses untrusted TIME literal text.
//!
//! Unlike TIMESTAMP, TIME has no precision modifier and its cast does not round,
//! so a `Row` holds every parsed nanosecond while the renderer writes only
//! microseconds. The renderer is therefore the rounding step, and the oracle is
//! that it rounds rather than mangles: re-parsing its output must land within
//! half a microsecond of the value it was handed.

#![no_main]

use chrono::{NaiveTime, Timelike};
use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{format_time, parse_time};

/// The nanosecond count of `t` since midnight, counting chrono's leap second
/// (spelled as a sub-second of a full second at `23:59:59`) as the nanosecond
/// after `23:59:59.999999999`.
///
/// Rendering can move a value across a second, a minute, or an hour boundary by
/// rounding its fraction up, so the round trip has to be measured on one number
/// rather than field by field.
fn nanos_from_midnight(t: NaiveTime) -> i64 {
    i64::from(t.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(t.nanosecond())
}

fuzz_target!(|data: &str| {
    let Ok(t) = parse_time(data) else {
        return;
    };

    // Rendering is unconditional. A sub-microsecond fraction and a leap second
    // are the two input classes that have broken the renderer before, so they
    // have to reach it.
    let mut buf = String::new();
    format_time(&mut buf, t);
    // Every rendering is `HH:MM:SS[.ffffff]`, reaching `:60` only for a leap
    // second, which the parser accepts back. A rejection means the renderer
    // emitted text no client can read back.
    let reparsed = parse_time(&buf).expect("format_time emitted text parse_time rejects");

    // The renderer rounds half away from zero, so a faithful rendering never
    // moves the value by more than half a microsecond.
    //
    // The exception is a round up out of the last second of the day, which has
    // no second to carry into: a `NaiveTime` wraps to midnight rather than
    // reaching PostgreSQL's `24:00:00`, so the fraction saturates at `.999999`
    // and the value can move by just under a full microsecond instead.
    let saturates = t.num_seconds_from_midnight() == 86_399
        && (999_999_500..1_000_000_000).contains(&t.nanosecond());
    let bound = match saturates {
        true => 1_000,
        false => 500,
    };
    let drift = nanos_from_midnight(reparsed) - nanos_from_midnight(t);
    assert!(
        drift.abs() <= bound,
        "time moved {drift}ns across parse/format round trip: {t} -> {buf} -> {reparsed}"
    );
});
