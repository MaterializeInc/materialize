// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_timestamptz` parses untrusted TIMESTAMPTZ
//! literal text. Two consumers read a parsed value back, and each gets its own
//! oracle. See `strconv_parse_timestamp` for the same pair spelled out at
//! length.
//!
//! `mz_pgrepr::Value::decode_text`, behind COPY and text-format bind parameters,
//! stores the parsed nanoseconds as they are, so for it the renderer is itself
//! the rounding step. `CastStringToTimestampTz` instead rounds to the column's
//! precision first. That precision comes from the input rather than being fixed
//! at microseconds, because the cast carries an arbitrary
//! `Option<TimestampPrecision>` taken from the type modifier. Precisions below 6
//! are the only ones that reach `round_to_precision`'s rounding branch at all:
//! at precision 6 the rounding quantum is a single microsecond, so every value
//! is already on a boundary and the branch is dead.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::strconv::{format_timestamptz, parse_timestamptz};

fuzz_target!(|input: (u8, &str)| {
    let (precision, data) = input;
    // `None` (the default) plus every declarable precision, 0 through 6.
    let precision = match precision % 8 {
        0 => None,
        p => Some(TimestampPrecision::try_from(i64::from(p) - 1).expect("0..=6 is in range")),
    };

    let Ok(ts) = parse_timestamptz(data) else {
        return;
    };
    // The unrounded consumer. The renderer writes microseconds, so re-parsing
    // its output has to land on the microsecond-rounded value. The carve-out is
    // the one case the renderer cannot mirror: where rounding up would leave
    // chrono's range it has no error channel and saturates the fraction instead.
    //
    // A leap second needs no carve-out here. chrono stores a parsed `:60` as a
    // sub-second of one second or more, which its `%S` renders back as `60`, and
    // an offset that would move it off `:59` (where the representation is
    // unconstructable) is folded during parsing.
    let mut buf = String::new();
    format_timestamptz(&mut buf, &ts);
    let reparsed =
        parse_timestamptz(&buf).expect("format_timestamptz emitted text parse_timestamptz rejects");
    if let Ok(rounded) = ts.round_to_precision(None) {
        assert_eq!(
            rounded, reparsed,
            "rendering a timestamptz did not round it to microseconds"
        );
    }

    // The cast. Rounding is idempotent, so re-rounding the re-parsed value to
    // the same precision must land back on `ts`. `expect` rather than a
    // carve-out: a renderer that starts emitting text the parser rejects is a
    // bug this oracle should report rather than tolerate.
    let Ok(ts) = ts.round_to_precision(precision) else {
        return;
    };
    let mut buf = String::new();
    format_timestamptz(&mut buf, &ts);
    let reparsed = parse_timestamptz(&buf)
        .expect("format_timestamptz emitted text parse_timestamptz rejects")
        .round_to_precision(precision)
        .expect("re-rounding an already-rounded timestamp cannot overflow");
    assert_eq!(
        ts, reparsed,
        "timestamptz changed across parse/format round trip"
    );
});
