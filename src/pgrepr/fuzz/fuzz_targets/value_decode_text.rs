// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Value::decode_text` decodes a client-supplied bind-parameter
//! value in Postgres *text* format. It dispatches on the type and delegates to
//! the `strconv` parsers (recursively, for array/list/map/record/range), all
//! over untrusted client bytes. Must never panic.
//!
//! A random byte string almost never reaches the interesting recursive
//! decoders: the array/list/map/range grammars need a leading brace/bracket and
//! a comma-separated body of *parseable element literals*, and the scalar
//! parsers (numeric, interval, timestamp, uuid, date) reject almost any random
//! ASCII. Feeding raw bytes therefore leaves the parsers stuck on their first
//! syntax check. Instead we build a `Type` directly (not via `from_oid`, which
//! cannot even produce `List`/`Map`) and synthesize a *well-formed text literal*
//! for it: scalar literals for the leaves, and properly braced, comma-separated,
//! optionally quoted/escaped, optionally `NULL`-bearing, optionally nested
//! bodies for `Array`/`List`/`Map`/`Range` (including the `empty` range and the
//! unsupported-but-parsed `[lo:hi]=` array-dimension prefix). This drives the
//! recursive element dispatch and the per-element scalar parsers all the way to
//! value construction and range/normalization checks. We still spend a quarter
//! of inputs in the "any OID, raw bytes" mode so the not-implemented branches
//! and the syntax-error paths stay covered.
//!
//! Excluded from the main workspace because libFuzzer requires nightly Rust.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgrepr::{Type, Value};

/// A well-formed text literal for a scalar leaf type, paired with that type.
/// The literal is in the *unnested* representation (no extra quoting). The
/// container builders re-quote/escape it as needed.
fn gen_leaf(u: &mut Unstructured) -> arbitrary::Result<(Type, String)> {
    Ok(match u.int_in_range(0u8..=14)? {
        0 => (
            Type::Bool,
            (*u.choose(&["true", "false", "t", "f", "yes", "no", "on", "off", "1", "0"])?)
                .to_string(),
        ),
        // Integers: in- and out-of-range so the parse-int overflow path is hit.
        1 => (Type::Int2, gen_int_literal(u)?),
        2 => (Type::Int4, gen_int_literal(u)?),
        3 => (Type::Int8, gen_int_literal(u)?),
        4 => (Type::UInt2, gen_int_literal(u)?),
        5 => (Type::UInt4, gen_int_literal(u)?),
        6 => (Type::UInt8, gen_int_literal(u)?),
        7 => (Type::Oid, gen_int_literal(u)?),
        // Floats, including the special-token branches.
        8 => (
            Type::Float8,
            (*u.choose(&[
                "0", "-0", "1.5", "-2.25", "3e10", "1.2e-3", "inf", "-inf", "Infinity", "NaN", ".5",
                "1e400",
            ])?)
            .to_string(),
        ),
        9 => (Type::Float4, gen_int_literal(u)?),
        // Numeric: feed digit strings, exponents, and out-of-band magnitudes.
        10 => (Type::Numeric { constraints: None }, gen_numeric_literal(u)?),
        // Interval: a grab bag of the unit/ISO/SQL-standard forms.
        11 => (
            Type::Interval { constraints: None },
            (*u.choose(&[
                "1 day",
                "01:02:03",
                "-1 year 2 mons",
                "1-2",
                "P1Y2M3DT4H5M6S",
                "1 day 2:03:04.567",
                "@ 5 hours ago",
                "100000000 years",
                "1.5 days",
            ])?)
            .to_string(),
        ),
        // Date / time / timestamp(tz): valid and edge-of-range forms.
        12 => (
            Type::Date,
            (*u.choose(&[
                "2000-01-01",
                "0001-01-01 BC",
                "294276-12-31",
                "infinity",
                "-infinity",
                "1999-02-29",
                "2024-02-29",
            ])?)
            .to_string(),
        ),
        13 => (
            Type::Timestamp { precision: None },
            (*u.choose(&[
                "2000-01-01 00:00:00",
                "1999-12-31 23:59:59.999999",
                "294277-01-01 00:00:00",
                "0001-01-01 00:00:00 BC",
                "infinity",
                "2024-02-29 12:34:56+05:30",
            ])?)
            .to_string(),
        ),
        // Uuid: canonical, braced, and hyphen-free spellings.
        _ => (
            Type::Uuid,
            (*u.choose(&[
                "00000000-0000-0000-0000-000000000000",
                "ffffffffffffffffffffffffffffffff",
                "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
                "A0EEBC999C0B4EF8BB6D6BB9BD380A11",
            ])?)
            .to_string(),
        ),
    })
}

/// An integer literal: small in-range values, boundary values, signs, leading
/// zeros, whitespace, and clearly-overflowing magnitudes.
fn gen_int_literal(u: &mut Unstructured) -> arbitrary::Result<String> {
    Ok(match u.int_in_range(0u8..=6)? {
        0 => u.int_in_range(-9i64..=9)?.to_string(),
        1 => u.arbitrary::<i16>()?.to_string(),
        2 => u.arbitrary::<i32>()?.to_string(),
        3 => u.arbitrary::<i64>()?.to_string(),
        4 => format!(" {} ", u.arbitrary::<i32>()?),
        5 => format!("+{}", u.int_in_range(0u64..=u64::MAX)?),
        // Way past i64/i128: forces the overflow error path.
        _ => "999999999999999999999999999999".to_string(),
    })
}

/// A numeric literal: plain digits, fractions, exponents, sign, and magnitudes
/// well beyond the 39-digit / base-10000-word limits of the numeric decoder.
fn gen_numeric_literal(u: &mut Unstructured) -> arbitrary::Result<String> {
    Ok(match u.int_in_range(0u8..=7)? {
        0 => "0".to_string(),
        1 => u.arbitrary::<i64>()?.to_string(),
        2 => format!("{}.{}", u.arbitrary::<u32>()?, u.arbitrary::<u16>()?),
        3 => format!("{}e{}", u.int_in_range(1i32..=9)?, u.int_in_range(-40i32..=40)?),
        4 => "NaN".to_string(),
        5 => "-Infinity".to_string(),
        // Long digit run (more than the 39 significant digits numeric keeps).
        6 => "1".repeat(usize::from(u.int_in_range(40u8..=80)?)),
        _ => format!("1e{}", u.int_in_range(100i32..=10000)?),
    })
}

/// Escape an element body for embedding inside an array/list literal: optionally
/// wrap in double quotes (escaping `"` and `\`) or backslash-escape the
/// structural characters. Returns the body unchanged a third of the time so the
/// unquoted lexer path is exercised too.
fn escape_for_container(u: &mut Unstructured, body: &str) -> arbitrary::Result<String> {
    Ok(match u.int_in_range(0u8..=2)? {
        0 => body.to_string(),
        1 => {
            let mut out = String::with_capacity(body.len() + 2);
            out.push('"');
            for c in body.chars() {
                if c == '"' || c == '\\' {
                    out.push('\\');
                }
                out.push(c);
            }
            out.push('"');
            out
        }
        _ => {
            let mut out = String::with_capacity(body.len());
            for c in body.chars() {
                if matches!(c, '{' | '}' | ',' | '\\' | '"' | ' ') {
                    out.push('\\');
                }
                out.push(c);
            }
            out
        }
    })
}

/// Recursively build a `(Type, literal)` pair, occasionally wrapping a value in
/// an `Array`, `List`, `Map`, or `Range` container with a well-formed body.
/// `depth` bounds the nesting so we always terminate.
fn gen_value(u: &mut Unstructured, depth: u8) -> arbitrary::Result<(Type, String)> {
    // At max depth, or randomly, emit a scalar leaf.
    if depth == 0 || u.int_in_range(0u8..=2)? == 0 {
        return gen_leaf(u);
    }

    Ok(match u.int_in_range(0u8..=3)? {
        // Array: `{e1,e2,...}`, possibly multi-dimensional, possibly with NULLs,
        // possibly prefixed with the (unsupported, but parsed) dimension syntax.
        0 => {
            let (elem_ty, _) = gen_value(u, depth - 1)?;
            let n = u.int_in_range(0usize..=4)?;
            let mut body = String::new();
            // Occasionally emit the `[lo:hi]=` dimension prefix, which the parser
            // recognizes and then rejects as unsupported.
            if u.int_in_range(0u8..=7)? == 0 {
                body.push_str(&format!("[{}:{}]=", u.int_in_range(-2i32..=2)?, n));
            }
            // Optionally wrap in extra braces for a multi-dimensional shape.
            let extra_dims = u.int_in_range(0u8..=2)?;
            for _ in 0..extra_dims {
                body.push('{');
            }
            body.push('{');
            for i in 0..n {
                if i > 0 {
                    body.push(',');
                }
                if u.int_in_range(0u8..=6)? == 0 {
                    body.push_str(*u.choose(&["NULL", "null", "NuLl"])?);
                } else {
                    let (_, elem) = elem_literal(u, &elem_ty, depth - 1)?;
                    body.push_str(&escape_for_container(u, &elem)?);
                }
            }
            body.push('}');
            for _ in 0..extra_dims {
                body.push('}');
            }
            (Type::Array(Box::new(elem_ty)), body)
        }
        // List: `{e1,e2,...}`. Nested lists allowed via embedded braces.
        1 => {
            let (elem_ty, _) = gen_value(u, depth - 1)?;
            let nested_list = matches!(elem_ty, Type::List(_));
            let n = u.int_in_range(0usize..=4)?;
            let mut body = String::from("{");
            for i in 0..n {
                if i > 0 {
                    body.push(',');
                }
                if u.int_in_range(0u8..=6)? == 0 {
                    body.push_str("NULL");
                } else {
                    let (_, elem) = elem_literal(u, &elem_ty, depth - 1)?;
                    // A nested list element keeps its braces. Other elements are
                    // quoted/escaped.
                    if nested_list {
                        body.push_str(&elem);
                    } else {
                        body.push_str(&escape_for_container(u, &elem)?);
                    }
                }
            }
            body.push('}');
            (Type::List(Box::new(elem_ty)), body)
        }
        // Map: `{k1=>v1,k2=>v2,...}` with text keys.
        2 => {
            let (val_ty, _) = gen_value(u, depth - 1)?;
            let nested_map = matches!(val_ty, Type::Map { .. });
            let n = u.int_in_range(0usize..=4)?;
            let mut body = String::from("{");
            for i in 0..n {
                if i > 0 {
                    body.push(',');
                }
                let key = *u.choose(&["a", "b", "key one", "k\"q", "", "=>"])?;
                body.push_str(&escape_for_container(u, key)?);
                body.push_str("=>");
                if u.int_in_range(0u8..=6)? == 0 {
                    body.push_str("NULL");
                } else {
                    let (_, val) = elem_literal(u, &val_ty, depth - 1)?;
                    if nested_map {
                        body.push_str(&val);
                    } else {
                        body.push_str(&escape_for_container(u, &val)?);
                    }
                }
            }
            body.push('}');
            (Type::Map { value_type: Box::new(val_ty) }, body)
        }
        // Range: `empty`, `[lo,hi)`, `(,hi]`, `[lo,)`, etc. Range elements must
        // be a totally-ordered scalar, so restrict to one of the supported
        // domains.
        _ => {
            let elem_ty = match u.int_in_range(0u8..=4)? {
                0 => Type::Int4,
                1 => Type::Int8,
                2 => Type::Numeric { constraints: None },
                3 => Type::Date,
                _ => Type::Timestamp { precision: None },
            };
            if u.int_in_range(0u8..=5)? == 0 {
                return Ok((Type::Range { element_type: Box::new(elem_ty) }, "empty".to_string()));
            }
            let lo_inc = u.arbitrary::<bool>()?;
            let hi_inc = u.arbitrary::<bool>()?;
            let lo = if u.int_in_range(0u8..=2)? == 0 {
                String::new()
            } else {
                gen_range_bound(u, &elem_ty)?
            };
            let hi = if u.int_in_range(0u8..=2)? == 0 {
                String::new()
            } else {
                gen_range_bound(u, &elem_ty)?
            };
            let body = format!(
                "{}{},{}{}",
                if lo_inc { '[' } else { '(' },
                lo,
                hi,
                if hi_inc { ']' } else { ')' },
            );
            (Type::Range { element_type: Box::new(elem_ty) }, body)
        }
    })
}

/// Generate a literal for a *specific* element type (so container element types
/// stay consistent), bottoming out via the generic builder for containers.
fn elem_literal(u: &mut Unstructured, ty: &Type, depth: u8) -> arbitrary::Result<(Type, String)> {
    match ty {
        Type::Bool => Ok((ty.clone(), (*u.choose(&["t", "f", "true", "false"])?).to_string())),
        Type::Int2 | Type::Int4 | Type::Int8 | Type::UInt2 | Type::UInt4 | Type::UInt8
        | Type::Oid | Type::Float4 => Ok((ty.clone(), gen_int_literal(u)?)),
        Type::Float8 => Ok((
            ty.clone(),
            (*u.choose(&["1.5", "-2.25", "inf", "NaN", "0"])?).to_string(),
        )),
        Type::Numeric { .. } => Ok((ty.clone(), gen_numeric_literal(u)?)),
        Type::Date => Ok((
            ty.clone(),
            (*u.choose(&["2000-01-01", "1999-12-31", "infinity"])?).to_string(),
        )),
        Type::Timestamp { .. } => Ok((
            ty.clone(),
            (*u.choose(&["2000-01-01 00:00:00", "1999-12-31 23:59:59"])?).to_string(),
        )),
        Type::Uuid => Ok((
            ty.clone(),
            "00000000-0000-0000-0000-000000000000".to_string(),
        )),
        Type::Interval { .. } => {
            Ok((ty.clone(), (*u.choose(&["1 day", "01:02:03", "1-2"])?).to_string()))
        }
        // Containers and everything else: delegate to the recursive builder,
        // which will pick its own (possibly different) shape but keep it valid.
        _ => gen_value(u, depth),
    }
}

/// A scalar range-bound literal matching the range's element type.
fn gen_range_bound(u: &mut Unstructured, ty: &Type) -> arbitrary::Result<String> {
    Ok(match ty {
        Type::Date => (*u.choose(&["2000-01-01", "1999-12-31", "2024-06-06"])?).to_string(),
        Type::Timestamp { .. } => {
            (*u.choose(&["2000-01-01 00:00:00", "2024-06-06 12:00:00"])?).to_string()
        }
        Type::Numeric { .. } => gen_numeric_literal(u)?,
        // Int4 / Int8.
        _ => gen_int_literal(u)?,
    })
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw mode: any OID + raw remaining bytes.
    // This keeps the not-implemented branches (json, record, timetz,
    // int2vector) and the scalar syntax-error paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        let oid = u32::from(u.arbitrary::<u16>()?);
        let rest = u.take_rest();
        if let Ok(ty) = Type::from_oid(oid) {
            let _ = Value::decode_text(&ty, rest);
        }
        return Ok(());
    }

    let (ty, body) = gen_value(&mut u, 3)?;
    let _ = Value::decode_text(&ty, body.as_bytes());
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
