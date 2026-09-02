// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_bytes` decodes untrusted `bytea` text (COPY
//! FROM, a text-format `bytea` parameter on the wire). It dispatches between the
//! hex form (`\x4a6b…`) and the hand-written "traditional" escape parser, which
//! walks bytes looking for `\\` and three-digit octal escapes `\NNN`. This is
//! exactly the byte-at-a-time escape-decoding shape that hid the CSV-decoder
//! bug. Must never panic on any input.
//!
//! Random text rarely lands on the `\x` prefix or a well-formed `\NNN` octal
//! triple, so byte mutation barely reaches either decoder. We instead consume
//! the byte stream as grammar choices and emit `bytea` text biased at the
//! decoders' edges:
//!   * hex form: `\x` followed by hex-digit pairs, deliberately including
//!     odd-length runs (the `OddLength` error), embedded whitespace (the
//!     skip-whitespace arm), and stray non-hex digits;
//!   * traditional form: octal escapes `\NNN` straddling the `\3xx`/`\4xx`
//!     high-nibble boundary (only `\0..\3` lead bytes are valid), short octal
//!     escapes, doubled `\\`, lone/trailing backslashes (the "ends with escape
//!     character" arm), and raw literal bytes.
//! A quarter of inputs are still the raw bytes, so the dispatch and reject paths
//! stay covered.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;

const HEX_DIGITS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C',
    'D', 'E', 'F',
];

/// Octal lead digits, biased to straddle the `\0..\3` (valid) / `\4..\7`
/// (invalid high nibble) boundary that the traditional parser rejects.
const OCTAL_DIGITS: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7'];

/// Emit a hex-form `bytea`: `\x` then a run of hex digits, with the occasional
/// embedded whitespace (skipped) or stray non-hex byte. Odd-length runs hit the
/// `OddLength` error.
fn gen_hex(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    out.push_str("\\x");
    let n = u.int_in_range(0usize..=12)?;
    for _ in 0..n {
        match u.int_in_range(0u8..=9)? {
            0 => out.push(*u.choose(&[' ', '\n', '\t', '\r'])?),
            1 => out.push(*u.choose(&['g', 'x', '_', '-'])?),
            _ => out.push(*u.choose(HEX_DIGITS)?),
        }
    }
    Ok(())
}

/// Emit traditional-form `bytea`: literal bytes interleaved with octal escapes
/// (`\NNN`, including the invalid `\4xx..\7xx` high nibble and short forms),
/// doubled backslashes, and lone/trailing backslashes.
fn gen_traditional(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=10)?;
    for _ in 0..n {
        match u.int_in_range(0u8..=6)? {
            // A full octal escape `\NNN`.
            0 | 1 => {
                out.push('\\');
                for _ in 0..3 {
                    out.push(*u.choose(OCTAL_DIGITS)?);
                }
            }
            // A short/partial octal escape (invalid: too few digits).
            2 => {
                out.push('\\');
                for _ in 0..u.int_in_range(0usize..=2)? {
                    out.push(*u.choose(OCTAL_DIGITS)?);
                }
            }
            // Doubled backslash (a literal `\`).
            3 => out.push_str("\\\\"),
            // A lone backslash followed by a non-octal byte (invalid escape).
            4 => {
                out.push('\\');
                out.push(*u.choose(&['x', '8', '9', 'a', ' ', '\\'])?);
            }
            // Raw literal bytes.
            _ => out.push(*u.choose(&['a', 'Z', '0', ' ', '\n', '~'])?),
        }
    }
    // Sometimes leave a trailing escape char (the "ends with escape" arm).
    if u.int_in_range(0u8..=4)? == 0 {
        out.push('\\');
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw bytes (must be UTF-8 for `parse_bytes`):
    // keeps the dispatch and reject paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        if let Ok(s) = std::str::from_utf8(u.take_rest()) {
            let _ = mz_repr::strconv::parse_bytes(s);
        }
        return Ok(());
    }
    let mut s = String::new();
    if u.int_in_range(0u8..=1)? == 0 {
        gen_hex(&mut u, &mut s)?;
    } else {
        gen_traditional(&mut u, &mut s)?;
    }
    let _ = mz_repr::strconv::parse_bytes(&s);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
