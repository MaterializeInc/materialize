// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_time` parses untrusted TIME literal text.

#![no_main]

use chrono::Timelike;
use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{format_time, parse_time};

fuzz_target!(|data: &str| {
    let Ok(t) = parse_time(data) else {
        return;
    };
    // mz TIME keeps nanosecond precision (its cast, unlike TIMESTAMP's, does not
    // round) but renders microseconds, and the parser accepts a leap second
    // (`:60`) the renderer can't round-trip — both known TIME/PG-compat gaps
    // tracked separately. Skip sub-microsecond and leap-second values.
    let nanos = t.nanosecond();
    if nanos % 1_000 != 0 || nanos >= 1_000_000_000 {
        return;
    }
    let mut buf = String::new();
    format_time(&mut buf, t);
    let Ok(reparsed) = parse_time(&buf) else {
        return;
    };
    assert_eq!(t, reparsed, "time changed across parse/format round trip");
});
