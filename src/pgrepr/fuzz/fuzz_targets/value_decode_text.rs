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
//! the `strconv` parsers (recursively, for array/list/map/range), all over
//! untrusted client bytes. Must never panic.
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
//! value construction and range/normalization checks.
//!
//! The type is built first and the literal is then built *for* that type at
//! every nesting level, so a nested container carries an element that is
//! well-typed for it rather than one re-rolled from an unrelated type. That is
//! what reaches `parse_list`'s nested-list mode, `parse_map`'s nested-map mode,
//! and array-of-range element construction. A small share of elements is
//! deliberately ill-typed so the element-level error paths stay covered too.
//!
//! We still spend a quarter of inputs in the "raw bytes" mode so the
//! not-implemented branches (json, timetz, int2vector) and the scalar
//! syntax-error paths stay covered. The OID there is drawn from the set
//! `Type::from_oid` actually resolves: it recognizes only 64 of the 65,536 `u16`
//! OIDs, so an arbitrary OID would throw away 99.9% of that budget on an
//! `UnknownOid` early return.
//!
//! NOTE: `Type::Record`'s arm stays unreachable. `from_oid` has no `RECORD` arm,
//! and `decode_text` rejects anonymous composite types outright, so there is
//! nothing behind it to exercise.
//!
//! Excluded from the main workspace because libFuzzer requires nightly Rust.

#![no_main]

use std::sync::LazyLock;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgrepr::{Type, Value};

/// Every OID that `Type::from_oid` resolves. Discovered by scanning the `u16`
/// space, which is exhaustive: `from_oid` delegates to
/// `postgres_types::Type::from_oid`, which knows no OID above that and none of
/// Materialize's custom OIDs.
static KNOWN_OIDS: LazyLock<Vec<u32>> = LazyLock::new(|| {
    (0..=u32::from(u16::MAX))
        .filter(|oid| Type::from_oid(*oid).is_ok())
        .collect()
});

/// A scalar leaf type, i.e. one of the non-container `decode_text` arms that has
/// a hand-written text parser behind it.
fn gen_leaf_type(u: &mut Unstructured) -> arbitrary::Result<Type> {
    Ok(match u.int_in_range(0u8..=18)? {
        0 => Type::Bool,
        1 => Type::Int2,
        2 => Type::Int4,
        3 => Type::Int8,
        4 => Type::UInt2,
        5 => Type::UInt4,
        6 => Type::UInt8,
        7 => Type::Oid,
        8 => Type::Float4,
        9 => Type::Float8,
        10 => gen_numeric_type(u)?,
        11 => Type::Interval { constraints: None },
        12 => Type::Date,
        13 => Type::Timestamp { precision: None },
        14 => Type::Uuid,
        15 => Type::Name,
        16 => Type::MzTimestamp,
        17 => Type::MzAclItem,
        _ => Type::AclItem,
    })
}

/// A `numeric`, sometimes carrying precision/scale constraints so that
/// `rescale_numeric` runs rather than passing the value straight through.
///
/// `Type` exposes no constructor for `NumericConstraints`, so the constraints
/// have to come from a packed typmod.
fn gen_numeric_type(u: &mut Unstructured) -> arbitrary::Result<Type> {
    let typmod = match u.int_in_range(0u8..=3)? {
        // A plausible `numeric(p, s)`: the scale converts and `rescale` runs.
        1 | 2 => {
            let precision = u.int_in_range(0i32..=39)?;
            let scale = u.int_in_range(0i32..=39)?;
            ((precision << 16) | (scale & 0x7ff)) + 4
        }
        // Any typmod: a scale outside `0..=39` (the encoding admits negative
        // scales) instead fails the `NumericMaxScale` conversion.
        3 => u.arbitrary::<i32>()?,
        // Unconstrained.
        _ => -1,
    };
    let oid = Type::Numeric { constraints: None }.oid();
    Ok(Type::from_oid_and_typmod(oid, typmod).unwrap_or(Type::Numeric { constraints: None }))
}

/// A `Type` the generator can write a literal for, nesting at most `depth`
/// containers deep.
fn gen_type(u: &mut Unstructured, depth: u8) -> arbitrary::Result<Type> {
    // At max depth, or randomly, stop at a scalar leaf.
    if depth == 0 || u.int_in_range(0u8..=2)? == 0 {
        return gen_leaf_type(u);
    }

    Ok(match u.int_in_range(0u8..=3)? {
        0 => Type::Array(Box::new(gen_type(u, depth - 1)?)),
        1 => Type::List(Box::new(gen_type(u, depth - 1)?)),
        2 => Type::Map {
            value_type: Box::new(gen_type(u, depth - 1)?),
        },
        // A range element must be a totally-ordered scalar, so restrict it to
        // one of the supported domains.
        _ => Type::Range {
            element_type: Box::new(match u.int_in_range(0u8..=4)? {
                0 => Type::Int4,
                1 => Type::Int8,
                2 => gen_numeric_type(u)?,
                3 => Type::Date,
                _ => Type::Timestamp { precision: None },
            }),
        },
    })
}

/// A well-formed text literal *for* `ty`, in the unnested representation (no
/// extra quoting). The container builders re-quote/escape it as needed.
fn leaf_literal(u: &mut Unstructured, ty: &Type) -> arbitrary::Result<String> {
    Ok(match ty {
        Type::Bool => (*u.choose(&[
            "true", "false", "t", "f", "yes", "no", "on", "off", "1", "0",
        ])?)
        .to_string(),
        // Integers: in- and out-of-range so the parse-int overflow path is hit.
        Type::Int2
        | Type::Int4
        | Type::Int8
        | Type::UInt2
        | Type::UInt4
        | Type::UInt8
        | Type::Oid => gen_int_literal(u)?,
        // Floats, including the special tokens and the overflow/underflow
        // boundaries `parse_float` detects after the fact. The f32-only bounds
        // matter because `Float4` is the sole caller of `f32::from_str`.
        Type::Float4 | Type::Float8 => (*u.choose(&[
            "0", "-0", "1.5", "-2.25", "3e10", "1.2e-3", "inf", "-inf", "Infinity", "NaN", ".5",
            "1e400", "3.4e39", "1e-46",
        ])?)
        .to_string(),
        // Numeric: digit strings, exponents, and out-of-band magnitudes.
        Type::Numeric { .. } => gen_numeric_literal(u)?,
        // Interval: a grab bag of the unit/ISO/SQL-standard forms.
        Type::Interval { .. } => (*u.choose(&[
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
        // Date / timestamp: valid and edge-of-range forms.
        Type::Date => (*u.choose(&[
            "2000-01-01",
            "0001-01-01 BC",
            "294276-12-31",
            "infinity",
            "-infinity",
            "1999-02-29",
            "2024-02-29",
        ])?)
        .to_string(),
        Type::Timestamp { .. } => (*u.choose(&[
            "2000-01-01 00:00:00",
            "1999-12-31 23:59:59.999999",
            "294277-01-01 00:00:00",
            "0001-01-01 00:00:00 BC",
            "infinity",
            "2024-02-29 12:34:56+05:30",
        ])?)
        .to_string(),
        // Uuid: canonical, braced, and hyphen-free spellings.
        Type::Uuid => (*u.choose(&[
            "00000000-0000-0000-0000-000000000000",
            "ffffffffffffffffffffffffffffffff",
            "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}",
            "A0EEBC999C0B4EF8BB6D6BB9BD380A11",
        ])?)
        .to_string(),
        // Name truncates at 64 bytes without splitting a multibyte character,
        // so feed runs whose characters straddle that boundary.
        Type::Name => match u.int_in_range(0u8..=5)? {
            0 => String::new(),
            1 => "a".to_string(),
            2 => "a".repeat(64),
            3 => "a".repeat(65),
            // 66 bytes of 2-byte chars: byte 64 is mid-character.
            4 => "é".repeat(33),
            // 68 bytes of 4-byte chars: byte 64 ends a character, byte 65 does
            // not.
            _ => "😀".repeat(17),
        },
        // mz_timestamp parses as a u64 of milliseconds and otherwise falls back
        // to the timestamptz parser, whose result must fit a u64.
        Type::MzTimestamp => (*u.choose(&[
            "0",
            "18446744073709551615",
            "18446744073709551616",
            "-1",
            " 42 ",
            "2000-01-01 00:00:00",
            "1969-12-31 23:59:59+00",
        ])?)
        .to_string(),
        // mz_aclitem is `grantee=privileges/grantor` over `RoleId`s.
        Type::MzAclItem => (*u.choose(&[
            "u1=UC/u2",
            "=arwd/s1",
            "s1=RBNP/g1",
            "u1=/u2",
            "u1=X/u2",
            "=/",
            "u1",
            "u=U/u1",
            "p=U/p",
            "u18446744073709551616=U/u1",
            "é1=U/u2",
            "u1=U/u2/u3",
        ])?)
        .to_string(),
        // aclitem is the same shape over role OIDs.
        Type::AclItem => (*u.choose(&[
            "1=UC/2",
            "=arwd/0",
            "1=/2",
            "1=Z/2",
            "=/",
            "1",
            "99999999999=U/2",
            "-1=U/2",
        ])?)
        .to_string(),
        // Types the generator does not build. Any short token is a valid
        // literal for the text-like ones and a syntax probe for the rest.
        _ => (*u.choose(&["", "abc", "0", " x "])?).to_string(),
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
        3 => format!(
            "{}e{}",
            u.int_in_range(1i32..=9)?,
            u.int_in_range(-40i32..=40)?
        ),
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

/// A literal for `ty`: a scalar literal for a leaf, and a container body built
/// for the container's own element type so that nested elements stay well-typed.
///
/// Recursion is bounded by `ty`, which the caller built with a depth limit.
fn elem_literal(u: &mut Unstructured, ty: &Type) -> arbitrary::Result<String> {
    // Occasionally an element literal for an unrelated type, so the
    // element-level parse-error paths stay covered. Tested against the high
    // bound: `int_in_range` yields its low bound once the input is exhausted,
    // and this branch must stay rare even then. Keep it well below the
    // per-container element count, or most container bodies end up ill-typed.
    if u.int_in_range(0u8..=63)? == 63 {
        let ty = gen_leaf_type(u)?;
        return leaf_literal(u, &ty);
    }

    match ty {
        Type::Array(elem_ty) => array_body(u, elem_ty),
        Type::List(elem_ty) => list_body(u, elem_ty),
        Type::Map { value_type } => map_body(u, value_type),
        Type::Range { element_type } => range_body(u, element_type),
        _ => leaf_literal(u, ty),
    }
}

/// An array body: `{e1,e2,...}`, possibly multi-dimensional, possibly with
/// NULLs, possibly prefixed with the (unsupported, but parsed) dimension syntax.
fn array_body(u: &mut Unstructured, elem_ty: &Type) -> arbitrary::Result<String> {
    let n = u.int_in_range(0usize..=4)?;
    let mut body = String::new();
    // Occasionally emit the `[lo:hi]=` dimension prefix, which the parser
    // recognizes and then rejects as unsupported. Tested against the high bound
    // so that an exhausted input does not turn every array into a body that
    // `parse_array` rejects at byte 0.
    if u.int_in_range(0u8..=7)? == 7 {
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
            let elem = elem_literal(u, elem_ty)?;
            body.push_str(&escape_for_container(u, &elem)?);
        }
    }
    body.push('}');
    for _ in 0..extra_dims {
        body.push('}');
    }
    Ok(body)
}

/// A list body: `{e1,e2,...}`. A nested list keeps its braces bare, which is
/// what `parse_list`'s nested-list mode expects.
fn list_body(u: &mut Unstructured, elem_ty: &Type) -> arbitrary::Result<String> {
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
            let elem = elem_literal(u, elem_ty)?;
            if nested_list {
                body.push_str(&elem);
            } else {
                body.push_str(&escape_for_container(u, &elem)?);
            }
        }
    }
    body.push('}');
    Ok(body)
}

/// A map body: `{k1=>v1,k2=>v2,...}` with text keys. A nested map keeps its
/// braces bare, which is what `parse_map`'s nested-map mode expects.
fn map_body(u: &mut Unstructured, val_ty: &Type) -> arbitrary::Result<String> {
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
            let val = elem_literal(u, val_ty)?;
            if nested_map {
                body.push_str(&val);
            } else {
                body.push_str(&escape_for_container(u, &val)?);
            }
        }
    }
    body.push('}');
    Ok(body)
}

/// A range body: `empty`, `[lo,hi)`, `(,hi]`, `[lo,)`, etc.
fn range_body(u: &mut Unstructured, elem_ty: &Type) -> arbitrary::Result<String> {
    if u.int_in_range(0u8..=5)? == 0 {
        return Ok("empty".to_string());
    }
    let lo_inc = u.arbitrary::<bool>()?;
    let hi_inc = u.arbitrary::<bool>()?;
    let lo = if u.int_in_range(0u8..=2)? == 0 {
        String::new()
    } else {
        gen_range_bound(u, elem_ty)?
    };
    let hi = if u.int_in_range(0u8..=2)? == 0 {
        String::new()
    } else {
        gen_range_bound(u, elem_ty)?
    };
    Ok(format!(
        "{}{},{}{}",
        if lo_inc { '[' } else { '(' },
        lo,
        hi,
        if hi_inc { ']' } else { ')' },
    ))
}

/// A scalar range-bound literal matching the range's element type. Narrower than
/// [`leaf_literal`] on purpose: the bounds have to be comparable for the
/// normalization check to do anything.
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
    // A quarter of the time, the raw mode: a known OID + raw remaining bytes.
    // This keeps the not-implemented branches and the scalar syntax-error paths
    // covered, including the types the typed mode never builds.
    if u.int_in_range(0u8..=3)? == 0 {
        let oid = *u.choose(KNOWN_OIDS.as_slice())?;
        let rest = u.take_rest();
        // `KNOWN_OIDS` holds only OIDs that resolve, so this always matches.
        if let Ok(ty) = Type::from_oid(oid) {
            let _ = Value::decode_text(&ty, rest);
        }
        return Ok(());
    }

    let ty = gen_type(&mut u, 3)?;
    let body = elem_literal(&mut u, &ty)?;
    let _ = Value::decode_text(&ty, body.as_bytes());
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
