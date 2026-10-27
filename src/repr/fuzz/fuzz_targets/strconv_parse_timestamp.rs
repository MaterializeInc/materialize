// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_timestamp` parses untrusted TIMESTAMP literal
//! text. Two consumers read a parsed value back, and each gets its own oracle.
//!
//! `mz_pgrepr::Value::decode_text`, which backs COPY in TEXT and CSV format and
//! text-format extended-protocol bind parameters, does not round: it stores the
//! parsed nanoseconds as they are, and `Row` keeps all of them. For that
//! consumer the renderer is itself the rounding step, so the oracle is that
//! rendering and re-parsing lands exactly on the microsecond-rounded value.
//!
//! `CastStringToTimestamp` instead rounds to the column's precision before
//! storage. That precision comes from the input rather than being fixed at
//! microseconds, because the cast carries an arbitrary
//! `Option<TimestampPrecision>` taken from the type modifier. Precisions below 6
//! are the only ones that reach `round_to_precision`'s rounding branch at all:
//! at precision 6 the rounding quantum is a single microsecond, so every value
//! is already on a boundary and the branch is dead.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::strconv::{format_timestamp, parse_timestamp};

fuzz_target!(|input: (u8, &str)| {
    let (precision, data) = input;
    // `None` (the default) plus every declarable precision, 0 through 6.
    let precision = match precision % 8 {
        0 => None,
        p => Some(TimestampPrecision::try_from(i64::from(p) - 1).expect("0..=6 is in range")),
    };

    let Ok(ts) = parse_timestamp(data) else {
        return;
    };

    // The unrounded consumer. `format_timestamp` renders microseconds, so
    // re-parsing its output has to land on the microsecond-rounded value: any
    // other result is a value that displays as something it is not.
    //
    // `round_to_precision` reports out of range exactly where rounding up leaves
    // chrono's range, which is the one case the renderer cannot mirror. It has
    // no error channel, so it saturates the fraction at `.999999` instead, and
    // there is nothing for the two to agree on.
    let mut buf = String::new();
    format_timestamp(&mut buf, &ts);
    let reparsed =
        parse_timestamp(&buf).expect("format_timestamp emitted text parse_timestamp rejects");
    if let Ok(rounded) = ts.round_to_precision(None) {
        assert_eq!(
            rounded, reparsed,
            "rendering a timestamp did not round it to microseconds"
        );
    }

    // The cast. A value already rounded to `precision` sits on a microsecond
    // boundary, so the renderer has nothing left to round and the round trip is
    // exact. `expect` rather than a carve-out: a renderer that starts emitting
    // text the parser rejects is a bug this oracle should report, not tolerate.
    let Ok(ts) = ts.round_to_precision(precision) else {
        return;
    };
    let mut buf = String::new();
    format_timestamp(&mut buf, &ts);
    let reparsed = parse_timestamp(&buf)
        .expect("format_timestamp emitted text parse_timestamp rejects")
        .round_to_precision(precision)
        .expect("re-rounding an already-rounded timestamp cannot overflow");
    assert_eq!(
        ts, reparsed,
        "timestamp changed across parse/format round trip"
    );
});
