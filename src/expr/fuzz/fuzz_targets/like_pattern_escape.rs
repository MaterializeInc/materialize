// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `like_pattern::normalize_pattern` rewrites a `LIKE ... ESCAPE c`
//! pattern (custom escape character) into the default-escape form before
//! compilation. The user controls both the pattern and the escape character, so
//! the custom-escape rewrite — a char-by-char parser with escape state — must
//! not panic on any input, and its output must still compile and match. This is
//! the `ESCAPE`-clause path that `like_pattern_compile` (default escape only)
//! never exercises.
//!
//! With a fully arbitrary escape char and a fully arbitrary pattern, the escape
//! char almost never coincides with a character actually in the pattern, so the
//! two branches that matter stay cold: the custom-escape *consume* branch (where
//! the escape char is followed by another char) and the trailing *unterminated*
//! escape arm (where the escape char is the final character). To light both up
//! we draw the pattern over a tiny alphabet of LIKE metacharacters plus a couple
//! literals, and draw the escape char from that *same* alphabet — so it lands on
//! the pattern's own characters and the escape state machine runs in earnest,
//! including the off-by-one trailing-escape case.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::like_pattern::{self, EscapeBehavior};

/// Shared alphabet for the pattern and the escape char, so the escape char
/// frequently matches characters in the pattern and the consume/unterminated
/// branches fire. Includes the LIKE metacharacters and a few literals.
const ALPHA: &[char] = &['%', '_', '\\', 'a', 'b', 'c'];

fn gen_pattern(u: &mut Unstructured) -> arbitrary::Result<String> {
    let mut p = String::new();
    for _ in 0..u.int_in_range(0usize..=20)? {
        p.push(*u.choose(ALPHA)?);
    }
    Ok(p)
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let case_insensitive = u.arbitrary()?;

    let (pattern, escape_char) = if u.int_in_range(0u8..=3)? == 0 {
        // Some fully-arbitrary inputs keep the raw-Unicode reject coverage.
        (u.arbitrary::<String>()?, u.arbitrary::<char>()?)
    } else {
        (gen_pattern(&mut u)?, *u.choose(ALPHA)?)
    };
    // Text from the same literal-ish alphabet so the compiled matcher can match.
    let mut text = String::new();
    for _ in 0..u.int_in_range(0usize..=20)? {
        text.push(*u.choose(ALPHA)?);
    }

    for behavior in [EscapeBehavior::Char(escape_char), EscapeBehavior::Disabled] {
        let Ok(normalized) = like_pattern::normalize_pattern(&pattern, behavior) else {
            continue;
        };
        // The rewritten pattern is in default-escape form and must compile and
        // match arbitrary text without panicking.
        if let Ok(matcher) = like_pattern::compile(&normalized, case_insensitive) {
            let _ = matcher.is_match(&text);
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
