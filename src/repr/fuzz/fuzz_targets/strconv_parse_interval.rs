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
//! parser in strconv). Beyond not panicking, its `Display` rendering must
//! re-parse to the same interval.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::parse_interval;

fuzz_target!(|data: &str| {
    let Ok(iv) = parse_interval(data) else {
        return;
    };
    let formatted = iv.to_string();
    let reparsed = parse_interval(&formatted)
        .expect("interval Display must re-parse");
    assert_eq!(iv, reparsed, "interval changed across parse/format round trip");
});
