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
//! the custom-escape rewrite, a char-by-char parser with escape state, must not
//! panic on any input. This is the `ESCAPE`-clause path that
//! `like_pattern_compile` (default escape only) never exercises.
//!
//! `normalize_pattern` has no slicing, no byte indexing and no fallible
//! arithmetic, so a panic-only harness over it can never fail. The bug class it
//! can actually carry is a miscompilation in the escape state machine, a
//! dropped, doubled or mis-paired escape. Two oracles pin that down:
//!
//!  1. Every output of `normalize_pattern` emits the default escape `\` only as
//!     part of a completed two-character escape, so it is well-formed
//!     default-escape form and must compile. The size limit is the only
//!     legitimate rejection. See the exception for the identity passthrough at
//!     the check itself.
//!  2. Escaping every metacharacter *and* the escape character itself with the
//!     custom escape character yields a pure literal pattern. `LIKE` is
//!     anchored, so after normalization it must match exactly its own source
//!     text: `text` matches, and `text` with a character appended does not. A
//!     lost `\` turns an escaped `%`/`_` back into a wildcard and the appended
//!     direction fires. A spurious or mis-paired `\` breaks the match and the
//!     first direction fires.
//!
//! With a fully arbitrary escape char and a fully arbitrary pattern, the escape
//! char almost never coincides with a character actually in the pattern, so the
//! two branches that matter stay cold: the custom-escape *consume* branch (where
//! the escape char is followed by another char) and the trailing *unterminated*
//! escape arm (where the escape char is the final character). To light both up
//! we draw the pattern over a tiny alphabet of LIKE metacharacters plus a couple
//! literals, and draw the escape char from that *same* alphabet, so it lands on
//! the pattern's own characters and the escape state machine runs in earnest,
//! including the off-by-one trailing-escape case.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::EvalError;
use mz_expr::like_pattern::{self, EscapeBehavior};

/// Shared alphabet for the pattern and the match text, so the escape char
/// frequently matches characters in the pattern and the consume/unterminated
/// branches fire. Includes the LIKE metacharacters and a few literals.
const ALPHA: &[char] = &['%', '_', '\\', 'a', 'b', 'c'];

/// Alphabet for the escape char: `ALPHA` minus the default escape `\`.
/// `EscapeBehavior::Char('\\')` is an identity passthrough that never enters the
/// custom-escape state machine, so drawing it here would spend executions on a
/// two-line branch. `\` stays in `ALPHA` because a `\` in the *pattern* is what
/// drives the doubling branch of the rewrite.
const ESCAPE_ALPHA: &[char] = &['%', '_', 'a', 'b', 'c'];

fn gen_over_alpha(u: &mut Unstructured) -> arbitrary::Result<String> {
    let mut s = String::new();
    for _ in 0..u.int_in_range(0usize..=20)? {
        s.push(*u.choose(ALPHA)?);
    }
    Ok(s)
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let case_insensitive = u.arbitrary()?;

    let (pattern, escape_char, text) = if u.int_in_range(0u8..=3)? == 0 {
        // Some fully-arbitrary inputs keep the raw-Unicode reject coverage, and
        // give `is_match` text long enough to expose the super-linear
        // backtracking in the string matcher.
        (
            u.arbitrary::<String>()?,
            u.arbitrary::<char>()?,
            u.arbitrary::<String>()?,
        )
    } else {
        // Text from the same alphabet so the compiled matcher can match.
        (
            gen_over_alpha(&mut u)?,
            *u.choose(ESCAPE_ALPHA)?,
            gen_over_alpha(&mut u)?,
        )
    };

    for behavior in [EscapeBehavior::Char(escape_char), EscapeBehavior::Disabled] {
        let Ok(normalized) = like_pattern::normalize_pattern(&pattern, behavior) else {
            continue;
        };
        // `normalize_pattern` emits `\` only as part of a completed escape pair,
        // so its output is well-formed default-escape form and the documented
        // size limit is the only legitimate rejection. Anything else, notably
        // the `EvalError::Internal` that `build_regex` raises on a regex it
        // failed to build, means the rewrite produced a pattern the compiler
        // cannot read. `EscapeBehavior::Char('\\')` is an identity passthrough,
        // so there a lone trailing `\` from the input pattern reaches `compile`
        // unchanged and rejecting it is correct.
        let passthrough = matches!(behavior, EscapeBehavior::Char('\\'));
        match like_pattern::compile(&normalized, case_insensitive) {
            Ok(matcher) => {
                let _ = matcher.is_match(&text);
            }
            Err(EvalError::LikePatternTooLong) => {}
            Err(EvalError::UnterminatedLikeEscapeSequence) if passthrough => {}
            Err(e) => panic!(
                "normalize_pattern({pattern:?}, {behavior:?}) = {normalized:?} \
                 failed to compile: {e:?}"
            ),
        }
    }

    // Escaping every metacharacter in `text` with `escape_char`, plus
    // `escape_char` itself, yields a pattern that normalizes to a pure literal.
    // The construction holds for every `escape_char`, including when it is
    // itself `%`, `_` or `\`: `escape_char` is only ever emitted as the
    // immediate prefix of the character it escapes, so `normalize_pattern`'s
    // left-to-right pairing lines up exactly with it and no unescaped wildcard
    // survives. It also can never end in a dangling escape, which is why
    // normalization here must succeed.
    let mut literal = String::with_capacity(2 * text.len());
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') || c == escape_char {
            literal.push(escape_char);
        }
        literal.push(c);
    }
    let normalized = like_pattern::normalize_pattern(&literal, EscapeBehavior::Char(escape_char))
        .expect("every escape in `literal` is terminated by construction");
    match like_pattern::compile(&normalized, case_insensitive) {
        Ok(matcher) => {
            assert!(
                matcher.is_match(&text),
                "literal pattern {literal:?} (escape {escape_char:?}) normalized to \
                 {normalized:?} must match its source text {text:?}"
            );
            // And nothing else: the pattern is anchored at both ends and every
            // one of its characters consumes exactly one text character, under
            // `ILIKE` too, since simple case folding maps a character to other
            // single characters. So appending anything must break the match.
            let longer = format!("{text}a");
            assert!(
                !matcher.is_match(&longer),
                "literal pattern {literal:?} (escape {escape_char:?}) normalized to \
                 {normalized:?} must not match {longer:?}"
            );
        }
        Err(EvalError::LikePatternTooLong) => {}
        Err(e) => panic!(
            "literal pattern {literal:?} (escape {escape_char:?}) normalized to \
             {normalized:?} failed to compile: {e:?}"
        ),
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
