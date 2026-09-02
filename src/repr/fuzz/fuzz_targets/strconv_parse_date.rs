// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_date` parses untrusted DATE literal text. A
//! rendering of a parsed value must re-parse to the same value, and
//! `format_date`'s `Nestable` verdict must be honest about that rendering.

#![no_main]

use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::{Nestable, element_needs_escaping, format_date, parse_date};

fuzz_target!(|data: &str| {
    let Ok(d) = parse_date(data) else {
        return;
    };
    let mut buf = String::new();
    // `Nestable` is the other half of `format_date`'s contract, and it is
    // consumed: `stringify_datum` and the pgwire text encoder use it to decide
    // whether to escape a date nested in an array, list, map, record or range.
    // A wrong `Yes` is invisible to the round trip below, because the
    // wrongly-unquoted rendering still re-parses, so it needs its own assertion.
    if let Nestable::Yes = format_date(&mut buf, d) {
        assert!(
            !element_needs_escaping(buf.as_bytes()),
            "format_date claimed Nestable::Yes for a rendering that needs escaping: {buf:?}"
        );
    }
    // Re-parse is total over this output space, so a rejection is a renderer bug
    // rather than something to tolerate. `format_date` writes no time component,
    // so the leap-second round-trip gap cannot appear here, and a year of more
    // than four digits parses: `fill_pdt_date` reads a 6-digit leading number
    // followed by a dash as a full year. Sweeping the whole `Date` range leaves
    // four reachable forms, a 4, 5 or 6 digit Common Era year and a 4 digit BC
    // year, and all of them parse.
    let reparsed = parse_date(&buf).expect("format_date emitted text parse_date rejects");
    assert_eq!(d, reparsed, "date changed across parse/format round trip");
});
