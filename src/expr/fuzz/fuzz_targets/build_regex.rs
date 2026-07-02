// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `func::build_regex` compiles an untrusted regular expression
//! (and flags) for the `regexp_*` SQL functions, and the result matches
//! untrusted text. A user controls both, so a panic compiling or matching is a
//! real availability bug. (The `regex` crate is linear-time and size-limited,
//! so an oversized pattern returns an error rather than hanging or OOMing.)
//!
//! A random pattern often compiles, but random *text* rarely matches it, so the
//! match-found paths (capture extraction, `replace_all`, `split` on real splits)
//! stay under-exercised. So most inputs are generated: a structured pattern over
//! the regex feature set (classes, groups, alternation, quantifiers, anchors,
//! escapes) drawn from a small alphabet, plus text drawn from that *same*
//! alphabet so matches actually fire. A quarter of inputs are still raw
//! pattern/text bytes so the compiler's parse/reject paths keep their coverage.
//!
//! Beyond compile+match, we also exercise:
//!  * `replace_all` with a replacement string carrying capture references
//!    (`$1`, `${name}`, `$0`, a literal `$$`), so the regex crate's replacement
//!    interpolation runs against the captures the generated text actually fills.
//!  * occasional near-size-limit patterns built from nested counted quantifiers,
//!    which push the compiler toward its state-count / size limit (where it must
//!    return an error rather than hang or OOM) and toward deep AST nesting.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::func;

/// Shared alphabet for pattern literals and text, so generated text tends to
/// match the generated pattern.
const ALPHA: &[char] = &['a', 'b', 'c', '0', '1', ' '];

/// Return the longest prefix of `s` with at most `max` chars.
fn cap(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn maybe_quantifier(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=5)? {
        0 => out.push('*'),
        1 => out.push('+'),
        2 => out.push('?'),
        3 => out.push_str(&format!("{{{}}}", u.int_in_range(0u32..=5)?)),
        4 => out.push_str(&format!("{{{},}}", u.int_in_range(0u32..=3)?)),
        _ => out.push_str(&format!(
            "{{{},{}}}",
            u.int_in_range(0u32..=2)?,
            u.int_in_range(2u32..=5)?
        )),
    }
    if u.int_in_range(0u8..=2)? == 0 {
        out.push('?'); // lazy
    }
    Ok(())
}

fn gen_regex(
    u: &mut Unstructured,
    depth: u32,
    name_id: &mut u32,
    out: &mut String,
) -> arbitrary::Result<()> {
    if depth == 0 || u.is_empty() || out.len() > 256 {
        match u.int_in_range(0u8..=3)? {
            0 => out.push(*u.choose(ALPHA)?),
            1 => out.push('.'),
            2 => out.push_str(u.choose(&["\\d", "\\w", "\\s", "\\b", "\\.", "\\*", "\\p{L}"])?),
            _ => {
                out.push('[');
                if u.int_in_range(0u8..=2)? == 0 {
                    out.push('^');
                }
                for _ in 0..u.int_in_range(1usize..=3)? {
                    out.push(*u.choose(ALPHA)?);
                }
                if u.int_in_range(0u8..=1)? == 0 {
                    out.push_str("a-c");
                }
                out.push(']');
            }
        }
        return Ok(());
    }
    let d = depth - 1;
    match u.int_in_range(0u8..=5)? {
        0 => gen_regex(u, d, name_id, out)?,
        1 => {
            for _ in 0..u.int_in_range(2usize..=3)? {
                gen_regex(u, d, name_id, out)?;
            }
        }
        2 => {
            gen_regex(u, d, name_id, out)?;
            out.push('|');
            gen_regex(u, d, name_id, out)?;
        }
        3 => {
            out.push('(');
            // Mix non-capturing, plain-capturing, and named-capturing groups so
            // both `$N` and `${name}` replacement refs resolve to real captures.
            // Names must be unique, so derive one from a per-pattern counter.
            match u.int_in_range(0u8..=2)? {
                0 => out.push_str("?:"),
                1 => {
                    out.push_str(&format!("?P<g{}>", *name_id));
                    *name_id += 1;
                }
                _ => {}
            }
            gen_regex(u, d, name_id, out)?;
            out.push(')');
            maybe_quantifier(u, out)?;
        }
        4 => {
            gen_regex(u, d, name_id, out)?;
            maybe_quantifier(u, out)?;
        }
        _ => {
            if u.int_in_range(0u8..=1)? == 0 {
                out.push('^');
            }
            gen_regex(u, d, name_id, out)?;
            if u.int_in_range(0u8..=1)? == 0 {
                out.push('$');
            }
        }
    }
    Ok(())
}

/// Builds a deeply nested chain of counted quantifiers whose multiplied bounds
/// approach the regex crate's compiled-size limit, e.g. `(?:(?:a{40}){40}){40}`.
/// `build_regex` must reject this with an error (PatternTooLarge or the regex
/// crate's CompiledTooBig) rather than hang or OOM.
fn gen_near_limit(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    let layers = u.int_in_range(2usize..=5)?;
    for _ in 0..layers {
        out.push_str("(?:");
    }
    out.push(*u.choose(ALPHA)?);
    for _ in 0..layers {
        out.push_str(&format!("){{{}}}", u.int_in_range(2u32..=60)?));
    }
    Ok(())
}

/// A replacement string mixing several capture-reference forms so the regex
/// crate's interpolation runs against whatever captures the match produced:
/// numbered (`$1`), named-braced (`${g0}`), the whole match (`$0`), a literal
/// `$$`, and an out-of-range index (`$99`, which interpolates to empty).
const REPLACEMENT: &str = "x$1-${g0}-$0-$$-$99y";

fn drive(pattern: &str, flags: &str, text: &str) {
    let pattern = cap(pattern, 4096);
    let text = cap(text, 4096);
    let Ok(regex) = func::build_regex(pattern, flags) else {
        return;
    };
    let _ = regex.is_match(text);
    let _ = regex.find(text);
    let _ = regex.captures(text);
    // Plain deletion plus capture-ref interpolation (exercises the replacement
    // parser and capture-group lookup against real matches).
    let _ = regex.replace_all(text, "");
    let _ = regex.replace_all(text, REPLACEMENT);
    let _ = regex.split(text).count();
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let flags = *u.choose(&["", "i", "c", "ic", "ci"])?;
    // A quarter of the time, raw pattern/text bytes (the compiler's parse paths).
    if u.int_in_range(0u8..=3)? == 0 {
        let pattern: String = u.arbitrary()?;
        let text: String = u.arbitrary()?;
        drive(&pattern, flags, &text);
        return Ok(());
    }
    let mut pattern = String::new();
    // Occasionally emit a near-size-limit nested-quantifier pattern (the
    // compiler must reject it cleanly). Otherwise the structured generator.
    if u.int_in_range(0u8..=7)? == 0 {
        gen_near_limit(&mut u, &mut pattern)?;
    } else {
        let mut name_id = 0u32;
        gen_regex(&mut u, 3, &mut name_id, &mut pattern)?;
    }
    let mut text = String::new();
    for _ in 0..u.int_in_range(0usize..=24)? {
        text.push(*u.choose(ALPHA)?);
    }
    drive(&pattern, flags, &text);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
