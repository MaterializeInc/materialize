// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the jsonb access operators `->` / `->>` (field/element). A user
//! controls both the jsonb value (parsed from untrusted text) and the key/index,
//! and the operators traverse arbitrary nested structure, so a panic accessing
//! into a crafted document is an availability bug. We parse untrusted JSON, then
//! apply all four single-step accessors and require none to panic:
//!  * `jsonb -> '<key>'`  and `jsonb -> <int>`   (return jsonb)
//!  * `jsonb ->> '<key>'` and `jsonb ->> <int>`  (the `Stringify` variants, which
//!    additionally run `jsonb_stringify` on the accessed element, a distinct
//!    text-rendering path over the same crafted sub-document).
//!
//! Random text almost never parses as JSON, so a raw input would just bail at
//! `from_str` and the access operators would barely run. We instead generate a
//! valid JSON document (objects with keys from a small set, arrays, scalars) and
//! generate the access key/index from that same set, so the accessors hit real
//! fields/elements (the success + traversal paths) as well as missing ones, and
//! the index includes out-of-range and extreme values for the array-bounds path.

#![no_main]

use std::str::FromStr;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{func, Eval, MirScalarExpr};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Datum, ReprScalarType, RowArena};

/// Object keys, kept to a small set so generated access keys hit real fields.
const KEYS: &[&str] = &["a", "b", "c", "x"];

fn gen_json(u: &mut Unstructured, depth: u32, out: &mut String) -> arbitrary::Result<()> {
    let leaf = depth == 0 || u.is_empty();
    match if leaf {
        u.int_in_range(0u8..=3)?
    } else {
        u.int_in_range(0u8..=5)?
    } {
        0 => out.push_str("null"),
        1 => out.push_str(if u.int_in_range(0u8..=1)? == 0 {
            "true"
        } else {
            "false"
        }),
        2 => out.push_str(&i64::from(u.arbitrary::<i32>()?).to_string()),
        3 => {
            out.push('"');
            for _ in 0..u.int_in_range(0usize..=4)? {
                out.push(*u.choose(&['a', 'z', '0', ' '])?);
            }
            out.push('"');
        }
        4 => {
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
                out.push('"');
                out.push_str(u.choose(KEYS)?);
                out.push_str("\":");
                gen_json(u, depth - 1, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let mut json = String::new();
    gen_json(&mut u, 5, &mut json)?;
    let Ok(jsonb) = Jsonb::from_str(&json) else {
        return Ok(());
    };
    let value = jsonb.as_ref().into_datum();
    let arena = RowArena::new();

    // Key: usually a real field name (hit), sometimes a miss / arbitrary string.
    let key_buf;
    let key: &str = if u.int_in_range(0u8..=2)? == 0 {
        let n = u.int_in_range(0usize..=4)?;
        let mut s = String::new();
        for _ in 0..n {
            s.push(u.int_in_range(0x20u8..=0x7e)? as char);
        }
        key_buf = s;
        &key_buf
    } else {
        u.choose(KEYS)?
    };
    // Index: usually small (in/just-past array bounds), sometimes extreme.
    let index: i64 = if u.int_in_range(0u8..=3)? == 0 {
        u.arbitrary::<i64>()?
    } else {
        i64::from(u.int_in_range(-3i32..=8)?)
    };

    let key_expr = || MirScalarExpr::literal_ok(Datum::String(key), ReprScalarType::String);
    let index_expr = || MirScalarExpr::literal_ok(Datum::Int64(index), ReprScalarType::Int64);
    let jsonb_expr = || MirScalarExpr::literal_ok(value, ReprScalarType::Jsonb);

    // jsonb -> '<key>'  (object field access, returns jsonb)
    let _ = jsonb_expr()
        .call_binary(key_expr(), func::JsonbGetString)
        .eval(&[], &arena);
    // jsonb ->> '<key>'  (object field access, returns text via jsonb_stringify)
    let _ = jsonb_expr()
        .call_binary(key_expr(), func::JsonbGetStringStringify)
        .eval(&[], &arena);

    // jsonb -> <index>  (array element access, returns jsonb)
    let _ = jsonb_expr()
        .call_binary(index_expr(), func::JsonbGetInt64)
        .eval(&[], &arena);
    // jsonb ->> <index>  (array element access, returns text via jsonb_stringify)
    let _ = jsonb_expr()
        .call_binary(index_expr(), func::JsonbGetInt64Stringify)
        .eval(&[], &arena);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
