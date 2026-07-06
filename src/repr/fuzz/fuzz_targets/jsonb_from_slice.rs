// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Jsonb::from_slice` decodes untrusted JSON bytes (Kafka/webhook
//! source bodies) into Materialize's JSONB `Row` representation, doing recursive
//! object/array packing with key dedup and numeric coercion. Beyond not
//! panicking, its `Display` rendering must re-parse to the same value.
//!
//! Random bytes almost never form valid JSON, so byte mutation barely reaches
//! past the first token. The recursive packing, numeric coercion, and key dedup
//! (and the Display round-trip oracle, which only runs on a value that parsed)
//! stay shallow. So we consume the byte stream as grammar choices and emit valid
//! JSON, biased toward the bug-prone paths: numbers that stress the
//! arbitrary-precision coercion (huge exponents, long digit runs, `-0`), objects
//! with duplicate keys (the dedup path), nested arrays/objects, and strings with
//! escapes. A quarter of inputs are still the raw bytes, so the parser's reject
//! paths and non-UTF-8 handling stay covered.
//!
//! Two extra shapes probe edges shallow generation misses: a deep single-spine
//! nesting (`[[[…]]]` / `{"a":{"a":…}}`) that walks the recursive collector up
//! toward serde_json's nesting limit (where it must `Err`, not overflow the
//! stack), and an object whose key is literally `$serde_json::private::Number`,
//! serde_json's private arbitrary-precision-number token. Such an object is
//! reinterpreted as a number on the way back in, so we don't run the re-parse
//! oracle on it. We only assert `from_slice` doesn't panic and that `Display`
//! renders fully without panicking.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::adt::jsonb::Jsonb;

/// Valid JSON numbers chosen to stress the arbitrary-precision numeric coercion:
/// big integers, tiny/huge exponents, negative zero, long fractions.
const NUMBERS: &[&str] = &[
    "0",
    "-0",
    "1",
    "-1",
    "42",
    "3.14",
    "-2.5",
    "1e10",
    "1e-10",
    "1e308",
    "1e1000",
    "-1e-1000",
    "1.7976931348623157e308",
    "123456789012345678901234567890",
    "0.00000000000000000001",
    "9999999999999999999999999999999999999999",
];

/// Object keys, including the duplicate-prone short set (so dedup fires) and one
/// that needs escaping.
const KEYS: &[&str] = &["a", "b", "a", "k", "\"q\\\"x\"", "\"\""];

fn push_string(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    out.push('"');
    let n = u.int_in_range(0usize..=5)?;
    for _ in 0..n {
        match u.int_in_range(0u8..=6)? {
            0 => out.push_str("\\\""),
            1 => out.push_str("\\\\"),
            2 => out.push_str("\\n"),
            3 => out.push_str("\\u0041"),
            4 => out.push_str("\\uD83D\\uDE00"), // surrogate pair
            _ => out.push(*u.choose(&['a', 'z', '0', ' ', '!'])?),
        }
    }
    out.push('"');
    Ok(())
}

fn gen_json(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
    let leaf = depth == 0 || u.is_empty();
    let choice = if leaf {
        u.int_in_range(0u8..=4)?
    } else {
        u.int_in_range(0u8..=6)?
    };
    match choice {
        0 => out.push_str("null"),
        1 => out.push_str(if u.int_in_range(0u8..=1)? == 0 {
            "true"
        } else {
            "false"
        }),
        2 => out.push_str(u.choose(NUMBERS)?),
        3 => push_string(u, out)?,
        4 => {
            // A generated number (valid JSON: leading 1-9, optional frac/exp).
            if u.int_in_range(0u8..=1)? == 0 {
                out.push('-');
            }
            out.push(*u.choose(&['1', '2', '3', '4', '5', '6', '7', '8', '9'])?);
            for _ in 0..u.int_in_range(0usize..=12)? {
                out.push(*u.choose(&['0', '1', '5', '9'])?);
            }
            if u.int_in_range(0u8..=1)? == 0 {
                out.push('.');
                for _ in 0..u.int_in_range(1usize..=4)? {
                    out.push(*u.choose(&['0', '1', '9'])?);
                }
            }
        }
        5 => {
            out.push('[');
            let n = u.int_in_range(0usize..=4)?;
            for i in 0..n {
                if i > 0 {
                    out.push(',');
                }
                gen_json(u, depth - 1, out)?;
            }
            out.push(']');
        }
        _ => {
            out.push('{');
            let n = u.int_in_range(0usize..=4)?;
            for i in 0..n {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(u.choose(KEYS)?); // may repeat -> dedup path
                out.push(':');
                gen_json(u, depth - 1, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

/// Emit a deep single-spine nesting (`[[[…leaf…]]]` or `{"a":{"a":…leaf…}}`) to
/// walk the recursive collector down toward serde_json's nesting limit. Beyond
/// the limit `from_slice` must return an `Err` (handled by `check`), never
/// overflow the stack.
fn gen_deep_spine(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
    let use_obj = u.int_in_range(0u8..=1)? == 0;
    for _ in 0..depth {
        if use_obj {
            out.push_str("{\"a\":");
        } else {
            out.push('[');
        }
    }
    gen_json(u, 0, out)?;
    for _ in 0..depth {
        out.push(if use_obj { '}' } else { ']' });
    }
    Ok(())
}

/// Render the object `{"$serde_json::private::Number": <value>}`. serde_json
/// (built with `arbitrary_precision`) treats this exact key as its private
/// number token, so the object is reinterpreted as a number rather than packed
/// as an object. That asymmetry defeats the strict re-parse oracle but must
/// still not panic in `from_slice`/`Display`.
fn gen_private_number(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    out.push_str("{\"$serde_json::private::Number\":");
    // The value serde_json expects here is a string holding the digits, but feed
    // a variety (string number, junk string, real number, nested) to probe how
    // the number sniffer copes.
    match u.int_in_range(0u8..=3)? {
        0 => {
            out.push('"');
            out.push_str(u.choose(NUMBERS)?);
            out.push('"');
        }
        1 => out.push_str("\"not-a-number\""),
        2 => out.push_str(u.choose(NUMBERS)?),
        _ => gen_json(u, 1, out)?,
    }
    out.push('}');
    Ok(())
}

/// Parse, and only require that `Display` renders fully without panicking. Used
/// for shapes whose round trip is inherently lossy (the private number token).
fn check_no_panic(data: &[u8]) {
    if let Ok(j) = Jsonb::from_slice(data) {
        // Force the full Display rendering, which must not panic.
        let _ = j.to_string();
    }
}

fn check(data: &[u8]) {
    let Ok(j) = Jsonb::from_slice(data) else {
        return;
    };
    let formatted = j.to_string();
    // serde_json (which mz-repr builds with `arbitrary_precision`) represents
    // every JSON number internally as a single-key map
    // `{"$serde_json::private::Number": "<digits>"}`. A JSON *object* that uses
    // that exact key is therefore indistinguishable from a number on the way
    // back in: JSONB sorts an object's keys for display, this `$`-prefixed token
    // sorts ahead of ordinary keys, and the value is then reinterpreted as a
    // (possibly invalid) number. That asymmetry is an inherent serde_json
    // limitation, not a JSONB round-trip bug, and `from_slice` itself never
    // panics on it. So skip the re-parse check whenever the rendering leans on
    // serde_json's private number token.
    const SERDE_JSON_NUMBER_TOKEN: &str = "$serde_json::private::Number";
    if formatted.contains(SERDE_JSON_NUMBER_TOKEN) {
        return;
    }
    let reparsed: Jsonb = formatted.parse().expect("jsonb Display must re-parse");
    assert_eq!(j, reparsed, "jsonb changed across parse/format round trip");
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw bytes: keeps the parser's reject paths and
    // non-UTF-8 handling covered.
    if u.int_in_range(0u8..=3)? == 0 {
        check(u.take_rest());
        return Ok(());
    }
    match u.int_in_range(0u8..=9)? {
        // A deep single-spine nesting that probes the recursion limit. Depth is
        // chosen to straddle serde_json's default (~128): well within, right at,
        // and past it (where it must `Err`, not overflow).
        0 => {
            let depth = u.int_in_range(1u32..=200)?;
            let mut s = String::new();
            gen_deep_spine(&mut u, depth, &mut s)?;
            check(s.as_bytes());
        }
        // The private-number-token object: assert no panic + total Display only.
        1 => {
            let mut s = String::new();
            gen_private_number(&mut u, &mut s)?;
            check_no_panic(s.as_bytes());
        }
        // The common case: a wide, moderately deep random JSON value.
        _ => {
            let mut s = String::new();
            gen_json(&mut u, 6, &mut s)?;
            check(s.as_bytes());
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
