// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_date` parses untrusted DATE literal text. A
//! re-parseable rendering of a parsed value must yield the same value.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{format_date, parse_date};

fuzz_target!(|data: &str| {
    let Ok(d) = parse_date(data) else {
        return;
    };
    let mut buf = String::new();
    format_date(&mut buf, d);
    // The renderer can emit text the parser rejects — a known cluster of
    // date/time round-trip gaps (a leap second `:60`, which PG carries; a
    // >4-digit year; ...), tracked separately, not panics. Tolerate those and
    // only assert that a *re-parseable* rendering preserves the value (drift),
    // and that nothing panics.
    let Ok(reparsed) = parse_date(&buf) else {
        return;
    };
    assert_eq!(d, reparsed, "date changed across parse/format round trip");
});
