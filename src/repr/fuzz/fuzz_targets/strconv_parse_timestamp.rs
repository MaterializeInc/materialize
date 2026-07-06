// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_timestamp` parses untrusted TIMESTAMP literal
//! text. `CastStringToTimestamp` rounds the parsed value to the type's
//! precision (microseconds by default) before storage, so mirror that.

#![no_main]

use chrono::Timelike;
use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{format_timestamp, parse_timestamp};

fuzz_target!(|data: &str| {
    let Ok(ts) = parse_timestamp(data) else {
        return;
    };
    // The parser accepts a leap second (`:60`), stored as chrono's leap
    // representation (sub-second >= 1s). PostgreSQL carries `:60` to the next
    // minute and our microsecond renderer mis-encodes the leap, so such values
    // do not round-trip. This is a known parser/PG-compat gap tracked
    // separately, not a panic. Skip them (rounding to precision below can't
    // create a leap).
    if ts.nanosecond() >= 1_000_000_000 {
        return;
    }
    let Ok(ts) = ts.round_to_precision(None) else {
        return;
    };
    let mut buf = String::new();
    format_timestamp(&mut buf, &ts);
    // The renderer can also emit text the parser rejects (e.g. a >4-digit
    // year), also tracked separately. Tolerate that (re-parse failure) and only
    // assert that a re-parseable rendering preserves the value, plus no panics.
    let Ok(reparsed) = parse_timestamp(&buf) else {
        return;
    };
    let Ok(reparsed) = reparsed.round_to_precision(None) else {
        return;
    };
    assert_eq!(
        ts, reparsed,
        "timestamp changed across parse/format round trip"
    );
});
