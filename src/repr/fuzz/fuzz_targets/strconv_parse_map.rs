// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_map` parses untrusted SQL map literal text
//! (`'{k=>v,…}'`). Key/value lexing with quoting/escaping and embedded maps.
//! Run both the scalar-value and nested-map-value modes. Must never panic.
//!
//! Random text almost never forms a balanced `{k=>v,…}` map with the `=>`
//! separators in the right places, so byte mutation barely reaches the value
//! lexer. We instead consume the byte stream as grammar choices and emit a valid
//! map literal, with balanced braces, quoted keys/values with `\"`/`\\`
//! escapes, NULL values, and (in nested-value mode) embedded maps, so the
//! key/value split, the quote/escape state machine, and the recursive value
//! parse all run deep. A minority of inputs get structural noise spliced in so
//! the parser's error paths stay covered too.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;

/// Emit a quoted-or-unquoted token (used for both keys and scalar values).
fn push_token(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    if u.int_in_range(0u8..=1)? == 0 {
        let n = u.int_in_range(1usize..=4)?;
        for _ in 0..n {
            out.push(*u.choose(&['a', 'b', '0', '1', '_', '-'])?);
        }
    } else {
        out.push('"');
        let n = u.int_in_range(0usize..=4)?;
        for _ in 0..n {
            match u.int_in_range(0u8..=4)? {
                0 => out.push_str("\\\""),
                1 => out.push_str("\\\\"),
                _ => out.push(*u.choose(&['a', '1', ' ', '{', '}', ',', '='])?),
            }
        }
        out.push('"');
    }
    Ok(())
}

fn gen_map(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
    out.push('{');
    let n = u.int_in_range(0usize..=3)?;
    for i in 0..n {
        if i > 0 {
            out.push(',');
            if u.int_in_range(0u8..=2)? == 0 {
                out.push(' ');
            }
        }
        push_token(u, out)?; // key
        out.push_str("=>");
        if depth > 0 && u.int_in_range(0u8..=2)? == 0 {
            gen_map(u, depth - 1, out)?; // nested map value
        } else if u.int_in_range(0u8..=4)? == 0 {
            out.push_str("NULL");
        } else {
            push_token(u, out)?; // scalar value
        }
    }
    out.push('}');
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut s = String::new();
    gen_map(&mut u, 3, &mut s)?;
    // 1-in-6: splice structural noise to exercise the error paths.
    if u.int_in_range(0u8..=5)? == 0 {
        s.push_str(u.choose(&["{", "}", ",", "=>", "=", "\"", "\\", " ", "NULL"])?);
    }
    for is_value_type_map in [false, true] {
        let _ = mz_repr::strconv::parse_map(&s, is_value_type_map, |e| {
            Ok::<_, std::convert::Infallible>(e.map(|v| v.into_owned()))
        });
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
