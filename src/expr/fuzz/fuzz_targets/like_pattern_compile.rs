// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `like_pattern::compile` turns an untrusted SQL `LIKE`/`ILIKE`
//! pattern into a matcher (a string automaton or a compiled regex). A user
//! controls the pattern, so a panic or pathological build here is a real
//! availability bug. We check two things:
//!
//!  1. Compiling an arbitrary pattern and matching arbitrary text never panics.
//!  2. A pattern built by escaping every wildcard/escape character in `text` is
//!     a pure literal, and `LIKE` is anchored, so it must match exactly `text`:
//!     `text` matches, and `text` with a character appended does not.
//!
//! A pattern/text pair of random Unicode rarely contains the wildcard/escape
//! metacharacters or the literal overlap that drives the interesting code, so it
//! plateaus on shallow shapes. Instead we mostly draw both the pattern and the
//! match text from a tiny shared alphabet of literals plus the LIKE
//! metacharacters `%`, `_`, and `\`. Generating *several* `%` interleaved with
//! literals is deliberate, because that is what crosses the routing boundary
//! between the two matcher implementations: `compile` abandons the backtracking
//! string matcher for the linear-time regex engine once two or more `%`
//! subpatterns carry a *non-empty* suffix (so `%a%b`, but not `%%%%`, which
//! collapses into one subpattern, and not `%a%`, whose trailing `%` has an empty
//! suffix), or once the subpattern count exceeds `MAX_SUBPATTERNS`, which the
//! `%`-heavy weighting reaches most often in practice.
//!
//! Drawing the text from the same alphabet means it actually matches the
//! wildcards, exercising the `%`-suffix-search / backtracking and the regex path
//! on real (not always-empty) matches. The alphabet carries multi-byte
//! characters so byte offsets and character counts diverge: the `%` backtracking
//! in `is_match_subpatterns` mixes character counting with byte indexing and
//! walks back over char boundaries by byte, and with a single-byte alphabet that
//! walk degenerates to one iteration and can never expose an off-by-one.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::like_pattern;

/// Literals shared between pattern and text, so wildcards match real characters.
/// `é` (2 bytes) and `漢` (3 bytes) make byte offsets and character counts
/// diverge, so the char-boundary walk in `is_match_subpatterns` runs for more
/// than one iteration.
const LITERALS: &[char] = &['a', 'b', 'c', 'é', '漢'];

/// Builds a LIKE pattern over the literal alphabet plus the `%`/`_`/`\`
/// metacharacters, weighted toward emitting several `%` so patterns land on both
/// sides of the string-matcher/regex routing boundary.
fn gen_pattern(u: &mut Unstructured) -> arbitrary::Result<String> {
    let mut p = String::new();
    let n = u.int_in_range(0usize..=16)?;
    for _ in 0..n {
        match u.int_in_range(0u8..=6)? {
            // Weight `%` heavily (3/7) so multiple-`%` patterns are common.
            0 | 1 | 2 => p.push('%'),
            3 => p.push('_'),
            4 => {
                // Escape sequence: `\` followed by a metachar or literal. A bare
                // trailing `\` (the unterminated-escape arm) is intentionally
                // reachable when this is the last iteration.
                p.push('\\');
                if u.int_in_range(0u8..=3)? != 0 {
                    p.push(*u.choose(&['%', '_', '\\', 'a', 'é'])?);
                }
            }
            _ => p.push(*u.choose(LITERALS)?),
        }
    }
    Ok(p)
}

/// Builds match text over the same literal alphabet.
fn gen_text(u: &mut Unstructured) -> arbitrary::Result<String> {
    let mut t = String::new();
    for _ in 0..u.int_in_range(0usize..=24)? {
        t.push(*u.choose(LITERALS)?);
    }
    Ok(t)
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let case_insensitive = u.arbitrary()?;

    // Pattern/text: usually from the shared alphabet, sometimes raw bytes so the
    // compiler's reject/parse paths over arbitrary Unicode keep their coverage.
    let (pattern, text) = if u.int_in_range(0u8..=3)? == 0 {
        (u.arbitrary::<String>()?, u.arbitrary::<String>()?)
    } else {
        (gen_pattern(&mut u)?, gen_text(&mut u)?)
    };

    // (1) Arbitrary pattern + arbitrary text: compile and match must not panic.
    if let Ok(matcher) = like_pattern::compile(&pattern, case_insensitive) {
        let _ = matcher.is_match(&text);
    }

    // (2) Escape every LIKE metacharacter in `text` to get a literal pattern,
    // which must match exactly its own source text.
    let mut literal = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            literal.push('\\');
        }
        literal.push(c);
    }
    match like_pattern::compile(&literal, case_insensitive) {
        Ok(matcher) => {
            assert!(
                matcher.is_match(&text),
                "literal LIKE pattern {literal:?} must match its source text {text:?}"
            );
            // And nothing else: the pattern is anchored at both ends and every
            // one of its characters consumes exactly one text character, under
            // `ILIKE` too, since simple case folding maps a character to other
            // single characters. So appending anything must break the match. This
            // is the direction that catches over-matching regressions, such as a
            // lost anchor in the regex or an escaped metacharacter that falls
            // back to wildcard semantics.
            let longer = format!("{text}a");
            assert!(
                !matcher.is_match(&longer),
                "literal LIKE pattern {literal:?} must not match {longer:?}"
            );
        }
        // Every escape sequence in `literal` is terminated by construction, so
        // the documented pattern-length limit is the only legitimate rejection.
        Err(e) => assert!(
            literal.len() > 8 << 10,
            "valid literal pattern {literal:?} failed to compile: {e}"
        ),
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
