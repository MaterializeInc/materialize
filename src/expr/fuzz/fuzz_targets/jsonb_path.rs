// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the jsonb path-access operators `#>` / `#>>`. A user controls
//! both the jsonb value (parsed from untrusted text) and a `text[]` path, and
//! the operators walk the path into arbitrary nested structure, so a panic on a
//! crafted document/path is an availability bug. We parse untrusted JSON, build
//! a `text[]` path from the fuzzed components, then apply `jsonb #> path`
//! (returns jsonb) and `jsonb #>> path` (returns text), requiring neither to
//! panic. Complements `jsonb_get`, which covers the single-step `->` / `->>`
//! operators.
//!
//! Random text almost never parses as JSON, so a raw input would bail at
//! `from_str` and the path walk would barely run. We instead generate a valid
//! JSON document (objects with keys from a small set, arrays, scalars) and a
//! path whose components are drawn from that same set — so the walk descends
//! through real nested structure (the multi-step traversal the operators exist
//! for), with some missing components and numeric-looking array indices mixed in.
//!
//! Array-index components are not just `"0".."3"`: the list step runs
//! `strconv::parse_int64` on the component and, for a negative index, computes
//! `list.len().wrapping_sub(index)` to count from the end. So we also emit
//! negative (`"-1"`), out-of-range/huge (`"99999999999"`), boundary
//! (`"-9223372036854775808"` = `i64::MIN`, whose `unsigned_abs` is one past
//! `i64::MAX`), and malformed-but-numeric-looking (`"+0"`, `" 1"`, `"1.0"`,
//! `"0x10"`) strings — covering the parse-failure short-circuit and the
//! wrapping-subtraction index arithmetic.

#![no_main]

use std::str::FromStr;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{func, Eval, MirScalarExpr};
use mz_repr::adt::array::{ArrayDimension, InvalidArrayError};
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Datum, ReprScalarType, RowArena};

/// Object keys / path components, kept small so the path hits real fields.
const KEYS: &[&str] = &["a", "b", "c", "x"];

/// Numeric-looking array-index components exercising the list step's
/// `parse_int64` + negative-index `wrapping_sub` arithmetic: in-range,
/// negative (count-from-end), huge/out-of-range, the `i64::MIN` boundary, and
/// malformed-but-numeric-looking strings that `parse_int64` should reject.
const INDICES: &[&str] = &[
    "0",
    "1",
    "2",
    "3",
    "-1",
    "-2",
    "99999999999",
    "-99999999999",
    "9223372036854775807",  // i64::MAX
    "-9223372036854775808", // i64::MIN (unsigned_abs is i64::MAX + 1)
    "+0",
    " 1",
    "1.0",
    "0x10",
    "",
];

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

    // Path components: object keys (hits), array indices ("0".."3"), and an
    // occasional miss — so the walk descends real structure and also dead-ends.
    let n = u.int_in_range(0usize..=5)?;
    let mut path: Vec<&str> = Vec::with_capacity(n);
    for _ in 0..n {
        path.push(match u.int_in_range(0u8..=4)? {
            0 | 1 => u.choose(KEYS)?,
            2 | 3 => *u.choose(INDICES)?,
            _ => "missing",
        });
    }

    let dims = if path.is_empty() {
        Vec::new()
    } else {
        vec![ArrayDimension {
            lower_bound: 1,
            length: path.len(),
        }]
    };
    let path_datum = match arena.try_make_datum::<_, InvalidArrayError>(|packer| {
        packer.try_push_array(&dims, path.iter().map(|s| Datum::String(s)))
    }) {
        Ok(d) => d,
        Err(_) => return Ok(()),
    };
    let path_ty = ReprScalarType::Array(Box::new(ReprScalarType::String));

    // jsonb #> '{a,b,...}'  (returns jsonb)
    let get_path = MirScalarExpr::literal_ok(value, ReprScalarType::Jsonb).call_binary(
        MirScalarExpr::literal_ok(path_datum, path_ty.clone()),
        func::JsonbGetPath,
    );
    let _ = get_path.eval(&[], &arena);

    // jsonb #>> '{a,b,...}'  (returns text)
    let get_path_stringify = MirScalarExpr::literal_ok(value, ReprScalarType::Jsonb).call_binary(
        MirScalarExpr::literal_ok(path_datum, path_ty),
        func::JsonbGetPathStringify,
    );
    let _ = get_path_stringify.eval(&[], &arena);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
