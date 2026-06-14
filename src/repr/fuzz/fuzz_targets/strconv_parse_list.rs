// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_list` parses untrusted SQL list literal text.
//! Recursive embedded-element lexing with quoting/escaping. Run both the
//! scalar-element and nested-list-element modes. Must never panic.
//!
//! Random text almost never forms a balanced, properly-quoted `{…}` list, so
//! byte mutation barely reaches past the opening brace. We instead consume the
//! byte stream as grammar choices and emit a valid nested list literal —
//! balanced braces, quoted elements with `\"`/`\\` escapes, NULLs — so the
//! recursive element lexer and its quote/escape state machine run deep, in both
//! the scalar-element and nested-list-element modes. A minority of inputs get
//! structural noise spliced in so the parser's error paths stay covered too.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;

/// Emit one scalar element: an unquoted token, a quoted string (with escapes
/// and structural characters that *must* stay quoted), NULL, or empty.
fn push_elem(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=3)? {
        0 => {
            let n = u.int_in_range(0usize..=4)?;
            for _ in 0..n {
                out.push(*u.choose(&['a', 'b', '0', '1', '_', '-', '.'])?);
            }
        }
        1 => out.push_str("NULL"),
        2 => {
            out.push('"');
            let n = u.int_in_range(0usize..=4)?;
            for _ in 0..n {
                match u.int_in_range(0u8..=4)? {
                    0 => out.push_str("\\\""),
                    1 => out.push_str("\\\\"),
                    _ => out.push(*u.choose(&['a', '1', ' ', '{', '}', ','])?),
                }
            }
            out.push('"');
        }
        _ => {} // empty element
    }
    Ok(())
}

fn gen_list(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
    out.push('{');
    let n = u.int_in_range(0usize..=3)?;
    for i in 0..n {
        if i > 0 {
            out.push(',');
            if u.int_in_range(0u8..=2)? == 0 {
                out.push(' ');
            }
        }
        if depth > 0 && u.int_in_range(0u8..=2)? == 0 {
            gen_list(u, depth - 1, out)?;
        } else {
            push_elem(u, out)?;
        }
    }
    out.push('}');
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut s = String::new();
    gen_list(&mut u, 3, &mut s)?;
    // 1-in-6: splice structural noise to exercise the error paths.
    if u.int_in_range(0u8..=5)? == 0 {
        s.push_str(u.choose(&["{", "}", ",", "\"", "\\", "{{", "}}", " ", "NULL"])?);
    }
    for is_element_type_list in [false, true] {
        let _ = mz_repr::strconv::parse_list(&s, is_element_type_list, String::new, |e| {
            Ok::<_, std::convert::Infallible>(e.into_owned())
        });
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
