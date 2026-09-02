// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_uuid` decodes untrusted `uuid` text (COPY FROM,
//! a text-format `uuid` parameter on the wire). The `uuid` crate itself panics
//! while *constructing a parse error* for some short, brace-wrapped inputs
//! (e.g. `{}`, `{\0}`). It mis-slices the input, so `parse_uuid` pre-screens
//! the string before delegating. This is a regression guard for that fix: no
//! input may panic.
//!
//! Random text essentially never hits the `{`-wrapped / `urn:uuid:` / exactly-32
//! / exactly-36-with-dashes shapes the crate special-cases (and the pre-screen
//! gates on `len >= 32` + ASCII), so byte mutation barely reaches the crash
//! boundary. We instead consume the byte stream as grammar choices and emit
//! inputs biased right at it: brace-wrapped bodies of length 30-34 with embedded
//! NUL / non-hex (the mis-slice trigger), `urn:uuid:` prefixes, exactly-32-hex
//! and 36-char dashed forms with a few corrupted bytes, and surrounding
//! whitespace (`parse_uuid` trims). A quarter of inputs are still the raw string
//! so the pre-screen reject paths and non-ASCII handling stay covered.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;

const HEX: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'A', 'F',
];

/// Push one body character: usually a hex digit, occasionally a corrupting byte
/// (NUL, non-hex ASCII, or a stray dash) to derail the crate's slicing.
fn push_body_char(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=9)? {
        0 => out.push('\0'),
        1 => out.push(*u.choose(&['g', 'z', '-', ' ', '{', '}', ':'])?),
        _ => out.push(*u.choose(HEX)?),
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw string: keeps the pre-screen reject paths
    // and non-ASCII handling covered.
    if u.int_in_range(0u8..=3)? == 0 {
        if let Ok(s) = std::str::from_utf8(u.take_rest()) {
            let _ = mz_repr::strconv::parse_uuid(s);
        }
        return Ok(());
    }

    let mut s = String::new();
    // Optional leading whitespace (`parse_uuid` trims).
    if u.int_in_range(0u8..=4)? == 0 {
        s.push(*u.choose(&[' ', '\t', '\n'])?);
    }

    match u.int_in_range(0u8..=4)? {
        // Brace-wrapped body whose total length straddles the pre-screen's
        // 32-byte floor, the historical mis-slice / panic boundary.
        0 | 1 => {
            s.push('{');
            let n = u.int_in_range(28usize..=32)?;
            for _ in 0..n {
                push_body_char(&mut u, &mut s)?;
            }
            // Sometimes omit the closing brace (unbalanced framing).
            if u.int_in_range(0u8..=2)? != 0 {
                s.push('}');
            }
        }
        // `urn:uuid:` prefixed form.
        2 => {
            s.push_str("urn:uuid:");
            emit_canonical(&mut u, &mut s)?;
        }
        // Exactly-32 hex (simple form), with a few corrupted bytes.
        3 => {
            for _ in 0..32 {
                push_body_char(&mut u, &mut s)?;
            }
        }
        // 36-char dashed (hyphenated) form, with a few corrupted bytes.
        _ => emit_canonical(&mut u, &mut s)?,
    }

    // Optional trailing whitespace.
    if u.int_in_range(0u8..=4)? == 0 {
        s.push(*u.choose(&[' ', '\t', '\n'])?);
    }

    let _ = mz_repr::strconv::parse_uuid(&s);
    Ok(())
}

/// Emit the canonical `8-4-4-4-12` hyphenated form, with `push_body_char` so a
/// few bytes can be corrupted.
fn emit_canonical(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    for (i, &group) in [8usize, 4, 4, 4, 12].iter().enumerate() {
        if i > 0 {
            out.push('-');
        }
        for _ in 0..group {
            push_body_char(u, out)?;
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
