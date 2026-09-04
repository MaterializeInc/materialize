// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_interval` parses untrusted INTERVAL literal
//! text. It drives a complex datetime token state machine (the most intricate
//! parser in strconv). Beyond not panicking, its rendering must re-parse to the
//! same interval.
//!
//! Rendering goes through `strconv::format_interval` rather than `Display`
//! directly, because that is the entry point `::text` output and the pgwire
//! encoders call. The two agree today, `format_interval` being a `write!` of
//! `Display`, so this only keeps the target pointed at the right function if
//! that ever stops being true.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{format_interval, parse_interval};

fuzz_target!(|data: &str| {
    let Ok(iv) = parse_interval(data) else {
        return;
    };
    let mut formatted = String::new();
    format_interval(&mut formatted, iv);
    // Re-parse is total, so a rejection is a renderer bug rather than something
    // to tolerate. The hours field is the only unbounded one, and it is derived
    // as `(micros / 1_000_000).abs() / 3600`, so multiplying it back out during
    // the re-parse cannot exceed `|micros|` and cannot overflow. `i64::MAX`
    // micros renders as `2562047788:00:54.775807` and re-parses exactly.
    let reparsed =
        parse_interval(&formatted).expect("format_interval emitted text parse_interval rejects");
    assert_eq!(
        iv, reparsed,
        "interval changed across parse/format round trip"
    );
});
