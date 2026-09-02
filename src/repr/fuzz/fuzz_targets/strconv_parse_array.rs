// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_array` parses untrusted SQL array literal text
//! (`'{1,2,{3}}'::int[]`, COPY FROM) into elements + dimensions. It is a
//! hand-written, recursive parser over quoting/escaping and nested `{…}`
//! dimensions, a prime panic/OOM/stack-overflow surface. Element values are
//! ignored. We fuzz the structural parser. Must never panic.
//!
//! Random text almost never forms a balanced, properly-quoted `{…}` nesting, so
//! byte mutation barely reaches past the first dimension. We instead consume the
//! byte stream as grammar choices and emit a valid nested array literal, with
//! balanced braces, quoted elements with `\"`/`\\` escapes, and NULLs, so the
//! recursive dimension walk and the quote/escape state machine run deep.
//! A minority of inputs get structural noise spliced in (unbalanced braces,
//! stray quotes/backslashes) so the parser's error paths stay covered too.

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

fn gen_array(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
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
            gen_array(u, depth - 1, out)?;
        } else {
            push_elem(u, out)?;
        }
    }
    out.push('}');
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut s = String::new();
    gen_array(&mut u, 3, &mut s)?;
    // 1-in-6: splice structural noise to exercise the error paths.
    if u.int_in_range(0u8..=5)? == 0 {
        s.push_str(u.choose(&["{", "}", ",", "\"", "\\", "{{", "}}", " ", "NULL"])?);
    }
    let _ = mz_repr::strconv::parse_array(&s, String::new, |e| {
        Ok::<_, std::convert::Infallible>(e.into_owned())
    });
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
