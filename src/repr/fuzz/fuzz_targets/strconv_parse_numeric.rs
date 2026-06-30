// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_numeric` parses untrusted numeric literal text
//! into a fixed-precision decimal. Beyond not panicking, the canonical
//! standard-notation rendering must re-parse to the same value.
//!
//! Random text rarely lands near the interesting decimal edges, the 39-digit
//! precision limit, the exponent over/underflow boundary, or the
//! `NaN`/`Infinity` rejection arms, so byte mutation mostly bounces off the
//! initial `cx.parse`. We instead consume the byte stream as grammar choices and
//! emit numeric literals biased at those edges: long integer/fraction digit runs
//! straddling the precision cap (which triggers `munge_numeric` rounding and the
//! out-of-range arm), exponents near the representable range, optional signs and
//! surrounding whitespace (`parse_numeric` trims), and the special
//! `NaN`/`Infinity`/`-NaN`/signaling spellings that the post-parse validation
//! rejects. The strict round-trip oracle, canonical rendering must re-parse to
//! the same value, makes every parse that survives high signal. A quarter of
//! inputs are still the raw string so the reject paths stay covered.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::strconv::parse_numeric;

/// Special spellings: the infinity/NaN family the post-parse validation rejects
/// (or accepts as overflow-`inf`), plus assorted casing.
const SPECIALS: &[&str] = &[
    "NaN", "nan", "-NaN", "+NaN", "sNaN", "Infinity", "-Infinity", "+Infinity", "inf", "-inf",
    "Inf", "INFINITY",
];

fn push_digits(u: &mut Unstructured, out: &mut String, n: usize) -> arbitrary::Result<()> {
    for _ in 0..n {
        out.push(*u.choose(&['0', '1', '5', '7', '9'])?);
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw string: keeps the reject paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        if let Ok(s) = std::str::from_utf8(u.take_rest()) {
            check(s);
        }
        return Ok(());
    }

    let mut s = String::new();
    // Optional leading whitespace (`parse_numeric` trims).
    if u.int_in_range(0u8..=3)? == 0 {
        s.push(*u.choose(&[' ', '\t', '\n'])?);
    }

    match u.int_in_range(0u8..=5)? {
        // A special infinity/NaN spelling (hits the rejection arms).
        0 => s.push_str(u.choose(SPECIALS)?),
        _ => {
            // Optional sign.
            match u.int_in_range(0u8..=2)? {
                0 => s.push('-'),
                1 => s.push('+'),
                _ => {}
            }
            // Integer part: a digit run straddling the 39-digit precision cap.
            let int_len = u.int_in_range(0usize..=45)?;
            push_digits(&mut u, &mut s, int_len)?;
            // Optional fraction, also able to push total significant digits past
            // the precision limit.
            if u.int_in_range(0u8..=1)? == 0 {
                s.push('.');
                let frac_len = u.int_in_range(0usize..=45)?;
                // Guarantee at least one digit somewhere.
                let frac_len = if int_len == 0 && frac_len == 0 {
                    1
                } else {
                    frac_len
                };
                push_digits(&mut u, &mut s, frac_len)?;
            } else if int_len == 0 {
                s.push('0');
            }
            // Optional exponent near the representable range.
            if u.int_in_range(0u8..=2)? == 0 {
                s.push(*u.choose(&['e', 'E'])?);
                match u.int_in_range(0u8..=2)? {
                    0 => s.push('-'),
                    1 => s.push('+'),
                    _ => {}
                }
                // Magnitudes around the overflow/underflow boundary.
                let exp = u.choose(&["0", "9", "38", "39", "308", "1000", "6144", "999999"])?;
                s.push_str(exp);
            }
        }
    }

    // Optional trailing whitespace.
    if u.int_in_range(0u8..=3)? == 0 {
        s.push(*u.choose(&[' ', '\t', '\n'])?);
    }

    check(&s);
    Ok(())
}

fn check(s: &str) {
    let Ok(n) = parse_numeric(s) else {
        return;
    };
    let formatted = n.0.to_standard_notation_string();
    let reparsed = parse_numeric(&formatted).expect("canonical numeric rendering must re-parse");
    assert_eq!(n, reparsed, "numeric changed across parse/format round trip");
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
