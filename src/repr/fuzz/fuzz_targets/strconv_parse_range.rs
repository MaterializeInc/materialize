// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `strconv::parse_range` parses untrusted SQL range literal text
//! (`'[a,b)'`). Bound extraction with quoting/escaping. Must never panic.
//!
//! Random text rarely produces the `[`/`(` … `,` … `]`/`)` framing (or the
//! `empty` keyword) the parser needs, so byte mutation barely reaches the bound
//! lexer. We instead consume the byte stream as grammar choices and emit a
//! range literal exercising the bound extraction's sharp edges:
//!   * the lower bound is read with a naive `take_while(c != ',')` and the upper
//!     with `take_while(c not in ')]')`, neither of which understands quoting,
//!     so we deliberately embed `,`, `]`, `)` *inside* quoted bounds to drive
//!     that truncation;
//!   * matched and mismatched bracket/paren framing (`[..)`, `(..]`, dropped
//!     closer);
//!   * optional (unbounded) sides, quoted bounds with `\"`/`\\` escapes, and the
//!     `empty` keyword followed by junk (the "Junk after empty" arm).
//! A minority of inputs get extra structural noise spliced in so the parser's
//! error paths stay covered too.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;

/// Emit one range bound: empty (unbounded), an unquoted token, or a quoted
/// string with escapes and structural delimiters (`,`/`]`/`)`) that the naive
/// `take_while` extraction does *not* know to keep inside the quotes.
fn push_bound(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=2)? {
        0 => {} // unbounded
        1 => {
            let n = u.int_in_range(1usize..=4)?;
            for _ in 0..n {
                out.push(*u.choose(&['a', '0', '1', '-', '.', ':'])?);
            }
        }
        _ => {
            out.push('"');
            let n = u.int_in_range(0usize..=6)?;
            for _ in 0..n {
                match u.int_in_range(0u8..=5)? {
                    0 => out.push_str("\\\""),
                    1 => out.push_str("\\\\"),
                    // Bias toward the range delimiters so they land *inside* the
                    // quotes, exercising the naive take_while truncation.
                    2 | 3 => out.push(*u.choose(&[',', ']', ')', '['])?),
                    _ => out.push(*u.choose(&['a', '1', ' ', '('])?),
                }
            }
            out.push('"');
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut s = String::new();
    if u.int_in_range(0u8..=6)? == 0 {
        s.push_str("empty");
        // Bias toward `empty` + trailing junk (the "Junk after empty" arm).
        if u.int_in_range(0u8..=1)? == 0 {
            for _ in 0..u.int_in_range(1usize..=3)? {
                s.push(*u.choose(&[' ', ',', '[', ']', '(', ')', 'x', '"'])?);
            }
        }
    } else {
        // Independent open/close framing so mismatched brackets/parens arise.
        s.push(if u.int_in_range(0u8..=1)? == 0 { '[' } else { '(' });
        push_bound(&mut u, &mut s)?;
        s.push(',');
        push_bound(&mut u, &mut s)?;
        // Sometimes drop the closer entirely (truncated framing).
        if u.int_in_range(0u8..=5)? != 0 {
            s.push(if u.int_in_range(0u8..=1)? == 0 { ']' } else { ')' });
        }
    }
    // 1-in-5: splice extra structural noise to exercise the error paths.
    if u.int_in_range(0u8..=4)? == 0 {
        s.push_str(u.choose(&["[", "]", "(", ")", ",", "\"", "\\", " ", "empty"])?);
    }
    let _ = mz_repr::strconv::parse_range(&s, |e| {
        Ok::<_, std::convert::Infallible>(e.into_owned())
    });
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
