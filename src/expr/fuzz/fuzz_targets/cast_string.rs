// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the string->scalar cast functions parse untrusted text into a
//! typed value (`'...'::bigint`, `'...'::uuid`). These are hand-written parsers
//! reached directly from user SQL, so a panic on crafted input is an
//! availability bug. `cast_string_to_uuid` in particular wraps the `uuid`
//! crate, whose parser has panicked on malformed input before. We evaluate each
//! cast on the text and require it to return an `EvalError`, not panic.
//!
//! Purely random text almost never resembles a UUID or a boundary integer, so a
//! raw `&str` plateaus on the early-reject paths and barely reaches the parsers'
//! interesting branches. Instead we mostly *construct* the candidate string:
//!  * UUID-shaped strings: 32 hex digits with dashes placed at the canonical
//!    8/12/16/20 offsets (and off-by-one offsets), optional `{...}` / `urn:uuid:`
//!    wrappers, off-by-one digit counts, and an occasional embedded NUL, to
//!    drive the hyphen-position / length / wrapper handling in the uuid parser.
//!  * Boundary `int64` strings: values around `i64::{MIN,MAX}`, off-by-one past
//!    them (overflow path), leading `+`/`-`/zeros/whitespace, and embedded NUL.
//! A fraction of inputs remain fully arbitrary text so the early-reject paths
//! keep their coverage.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{func, Eval, MirScalarExpr, UnaryFunc};
use mz_repr::{Datum, ReprScalarType, RowArena};

const HEX: &[u8] = b"0123456789abcdefABCDEF";

/// Builds a UUID-shaped candidate string: hex body, dashes at chosen offsets,
/// optional wrapper, occasionally perturbed so it lands just off a valid form.
fn gen_uuid_like(u: &mut Unstructured) -> arbitrary::Result<String> {
    // Number of hex digits: usually 32 (canonical), sometimes off-by-one.
    let n_hex = match u.int_in_range(0u8..=4)? {
        0 => 31usize,
        1 => 33,
        _ => 32,
    };
    let mut hex = String::with_capacity(n_hex);
    for _ in 0..n_hex {
        hex.push(*u.choose(HEX)? as char);
    }

    // Dash placement: canonical 8/12/16/20, off-by-one, none, or scattered.
    let body = match u.int_in_range(0u8..=4)? {
        0 => hex.clone(), // no dashes (uuid::simple form)
        1 => insert_dashes(&hex, &[8, 12, 16, 20]),
        2 => insert_dashes(&hex, &[8, 12, 16, 20]), // canonical, weighted
        3 => insert_dashes(&hex, &[7, 12, 16, 20]), // off-by-one first group
        _ => {
            // Scatter a few dashes at fuzzer-chosen positions.
            let mut positions = Vec::new();
            for _ in 0..u.int_in_range(0usize..=5)? {
                positions.push(u.int_in_range(0usize..=hex.len())?);
            }
            positions.sort_unstable();
            insert_dashes(&hex, &positions)
        }
    };

    // Optional wrapper.
    let mut s = match u.int_in_range(0u8..=3)? {
        0 => format!("{{{body}}}"),
        1 => format!("urn:uuid:{body}"),
        2 => format!("{{urn:uuid:{body}}}"),
        _ => body,
    };

    // Occasionally embed a NUL or stray byte to probe parser robustness.
    if u.int_in_range(0u8..=7)? == 0 {
        let pos = u.int_in_range(0usize..=s.len())?;
        // Find a char boundary at or before `pos`.
        let pos = (0..=pos)
            .rev()
            .find(|&p| s.is_char_boundary(p))
            .unwrap_or(0);
        s.insert(pos, '\0');
    }
    Ok(s)
}

/// Inserts `-` into `hex` at the given (ascending) char offsets.
fn insert_dashes(hex: &str, offsets: &[usize]) -> String {
    let mut out = String::with_capacity(hex.len() + offsets.len());
    for (i, c) in hex.chars().enumerate() {
        if offsets.contains(&i) {
            out.push('-');
        }
        out.push(c);
    }
    out
}

/// Builds a boundary-ish int64 string: values near the i64 limits, just past
/// them, with leading sign/zeros/whitespace, or an embedded NUL.
fn gen_int_like(u: &mut Unstructured) -> arbitrary::Result<String> {
    let base: &str = u.choose(&[
        "0",
        "-0",
        "+0",
        "9223372036854775807",  // i64::MAX
        "9223372036854775808",  // i64::MAX + 1 (overflow)
        "-9223372036854775808", // i64::MIN
        "-9223372036854775809", // i64::MIN - 1 (overflow)
        "99999999999999999999", // way past
        "-1",
        "+1",
    ])?;
    let mut s = String::new();
    // Optional leading whitespace / extra zeros / sign.
    match u.int_in_range(0u8..=4)? {
        0 => s.push_str("  "),
        1 => s.push('\t'),
        2 => s.push('+'),
        3 => s.push_str("000"),
        _ => {}
    }
    s.push_str(base);
    // Optional trailing junk / NUL.
    match u.int_in_range(0u8..=5)? {
        0 => s.push_str("  "),
        1 => s.push('\0'),
        2 => s.push('x'),
        _ => {}
    }
    Ok(s)
}

fn drive(s: &str) {
    let arena = RowArena::new();
    let input = || MirScalarExpr::literal_ok(Datum::String(s), ReprScalarType::String);

    let _ = input()
        .call_unary(UnaryFunc::CastStringToInt64(func::CastStringToInt64))
        .eval(&[], &arena);
    let _ = input()
        .call_unary(UnaryFunc::CastStringToUuid(func::CastStringToUuid))
        .eval(&[], &arena);
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let s = match u.int_in_range(0u8..=4)? {
        // Mostly UUID-shaped (the uuid crate is the higher-risk parser).
        0 | 1 | 2 => gen_uuid_like(&mut u)?,
        3 => gen_int_like(&mut u)?,
        // A fraction fully arbitrary, preserving the early-reject coverage.
        _ => u.arbitrary()?,
    };
    drive(&s);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
