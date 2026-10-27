// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Value::decode_binary` decodes a client-supplied bind-parameter
//! value in Postgres *binary* format. The per-type decoders are hand-written
//! big-endian byte parsers (numeric base-10000, mz_acl_item slice reads,
//! interval, unsigned ints, jsonb). This is directly client-controlled input,
//! so any panic is an availability bug. Must never panic.
//!
//! A random byte string almost never satisfies these strict decoders: an exact
//! length check, a version byte, a sign word drawn from a five-value set, a
//! role-id variant tag. So feeding raw bytes leaves the decoders barely
//! exercised. Instead we pick a type and *encode a valid binary value for it*
//! (numeric header + digits, the 16-byte interval triple, the 26-byte
//! mz_aclitem, a jsonb version byte plus real JSON, in-range
//! date/time/timestamp), so the decoder runs all the way to the
//! value-construction and range-check logic. We then occasionally corrupt one
//! byte of that encoding, so the decoders' *content* validation runs and not
//! just their length checks, or truncate it for the length-validation and
//! short-read paths. A quarter of the time we instead fall back to the "any OID,
//! raw bytes" mode so the not-implemented branches and crafted-header paths stay
//! covered.
//!
//! A few arms also reach past the "happy" shape on purpose. The numeric header
//! sometimes carries an out-of-band weight/dscale, or digit words outside
//! `0..=9999`, which Materialize accepts (unlike PostgreSQL's `numeric_recv`,
//! which rejects them) and folds into the accumulator, so extreme values land on
//! the trailing precision guard. The bytea/text bodies are occasionally multi-KB
//! so the decoders' allocation/validation paths run on a large value rather than
//! a handful of bytes.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_pgrepr::{Type, Value};

/// Append a run of `0..=max` printable-ASCII bytes (valid UTF-8 for the string
/// decoders).
fn push_ascii(u: &mut Unstructured, b: &mut Vec<u8>, max: usize) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=max)?;
    for _ in 0..n {
        b.push(u.int_in_range(0x20u8..=0x7e)?);
    }
    Ok(())
}

/// Append a binary `RoleId`: a variant tag byte (`s`/`g`/`u`/`p`) + u64 LE id.
fn push_role_id(u: &mut Unstructured, b: &mut Vec<u8>) -> arbitrary::Result<()> {
    b.push(*u.choose(&[b's', b'g', b'u', b'p'])?);
    b.extend_from_slice(&u.arbitrary::<u64>()?.to_le_bytes());
    Ok(())
}

/// OID of `numeric`. `NumericConstraints` is not exported from `mz_pgrepr`, so
/// going through the OID is the only way to build a `Type::Numeric` that carries
/// constraints.
const NUMERIC_OID: u32 = 1700;

/// A `numeric` type, usually unconstrained but sometimes carrying a
/// `numeric(precision, scale)` typmod.
///
/// Constraints are the only `Type` payload `decode_binary` acts on: they feed
/// `rescale_numeric`, which rescales the fully client-controlled decoded value.
/// Production takes that path for every `$1` bound against a `numeric(p, s)`
/// column, since pgwire derives the parameter type from the planned scalar type.
fn gen_numeric_type(u: &mut Unstructured) -> arbitrary::Result<Type> {
    if u.int_in_range(0u8..=2)? != 0 {
        return Ok(Type::Numeric { constraints: None });
    }
    let typmod = if u.int_in_range(0u8..=3)? == 0 {
        u.arbitrary::<i32>()?
    } else {
        // A well-formed `numeric(precision, scale)` typmod, packed the way
        // `NumericConstraints::into_typmod` does it.
        ((u.int_in_range(0i32..=39)? << 16) | u.int_in_range(0i32..=39)?) + 4
    };
    // `NumericConstraints::from_typmod` accepts every `i32`, but fall back
    // instead of unwrapping so the harness itself cannot panic.
    Ok(Type::from_oid_and_typmod(NUMERIC_OID, typmod)
        .unwrap_or(Type::Numeric { constraints: None }))
}

/// Append a JSON scalar literal.
///
/// Covers the shapes `JsonbPacker`'s custom visitors exist for: numbers past
/// `f64` and past numeric's 39-digit precision (`NumberParser` funnels every
/// number through `strconv::parse_numeric`), `\u` escapes and lone surrogates,
/// and the `$serde_json::private::Number` magic key with which serde_json spells
/// an arbitrary-precision number, so that a one-key map of it is parsed as a
/// *number* rather than as a map.
fn push_json_scalar(u: &mut Unstructured, out: &mut String) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=7)? {
        0 => out.push_str("null"),
        1 => out.push_str(*u.choose(&["true", "false"])?),
        2 => out.push_str(&u.arbitrary::<i64>()?.to_string()),
        3 => out.push_str(&format!(
            "{}.{}",
            u.arbitrary::<i32>()?,
            u.arbitrary::<u32>()?
        )),
        4 => out.push_str(*u.choose(&[
            "1e309",
            "-1e309",
            "1e-400",
            "1e100000",
            "-0",
            "0.00000000000000000000000000000000000000001",
            "111111111111111111111111111111111111111111111",
        ])?),
        5 => out.push_str(*u.choose(&[
            r#""""#,
            r#""s""#,
            r#"" ""#,
            r#""\ud800""#,
            r#""\udbff\udfff""#,
            r#""\\""#,
            r#""\"""#,
            r#""é""#,
        ])?),
        6 => {
            out.push_str(r#"{"$serde_json::private::Number":"#);
            // Only a numeric string packs. The others reach `NumberParser`'s
            // error paths (unparseable number, non-string payload).
            out.push_str(*u.choose(&[
                r#""1""#,
                r#""NaN""#,
                r#""Infinity""#,
                r#""1e100000""#,
                r#""abc""#,
                r#""""#,
                "1",
                "null",
            ])?);
            out.push('}');
        }
        _ => out.push_str(&u.arbitrary::<f64>()?.to_string()),
    }
    Ok(())
}

/// Append a JSON document. `depth` bounds the nesting so we always terminate.
fn push_json(u: &mut Unstructured, out: &mut String, depth: u8) -> arbitrary::Result<()> {
    if depth == 0 || u.int_in_range(0u8..=2)? == 0 {
        return push_json_scalar(u, out);
    }
    let n = u.int_in_range(0usize..=4)?;
    if u.arbitrary::<bool>()? {
        out.push('[');
        for i in 0..n {
            if i > 0 {
                out.push(',');
            }
            push_json(u, out, depth - 1)?;
        }
        out.push(']');
    } else {
        out.push('{');
        for i in 0..n {
            if i > 0 {
                out.push(',');
            }
            // Keys come from a small pool so duplicates (which the packer must
            // dedup) come up often, and so the magic key also appears alongside
            // other keys, where it is a plain map key rather than a number. An
            // escaped key cannot be borrowed out of the input, which takes
            // `KeyClassifier`'s owned branch instead of its borrowed one.
            out.push_str(*u.choose(&[
                r#""a""#,
                r#""b""#,
                r#""""#,
                r#""$serde_json::private::Number""#,
                r#""\u0041""#,
            ])?);
            out.push(':');
            push_json(u, out, depth - 1)?;
        }
        out.push('}');
    }
    Ok(())
}

/// Pick a type that has a binary decoder and encode a valid value for it.
fn gen_typed_value(u: &mut Unstructured) -> arbitrary::Result<(Type, Vec<u8>)> {
    let mut b = Vec::new();
    let ty = match u.int_in_range(0u8..=25)? {
        0 => {
            b.push(u.int_in_range(0u8..=1)?);
            Type::Bool
        }
        1 => {
            // Usually a short body, but occasionally a multi-KB one so the
            // bytea decoder's allocation/copy path runs on a large value.
            let n = if u.int_in_range(0u8..=15)? == 0 {
                u.int_in_range(1024usize..=8192)?
            } else {
                u.int_in_range(0usize..=16)?
            };
            for _ in 0..n {
                b.push(u.arbitrary::<u8>()?);
            }
            Type::Bytea
        }
        2 => {
            b.push(u.arbitrary::<u8>()?);
            Type::Char
        }
        // Date: i32 BE days since 2000-01-01. from_pg_epoch range-checks.
        3 => {
            b.extend_from_slice(&u.arbitrary::<i32>()?.to_be_bytes());
            Type::Date
        }
        4 => {
            b.extend_from_slice(&u.arbitrary::<f32>()?.to_be_bytes());
            Type::Float4
        }
        5 => {
            b.extend_from_slice(&u.arbitrary::<f64>()?.to_be_bytes());
            Type::Float8
        }
        6 => {
            b.extend_from_slice(&u.arbitrary::<i16>()?.to_be_bytes());
            Type::Int2
        }
        7 => {
            b.extend_from_slice(&u.arbitrary::<i32>()?.to_be_bytes());
            Type::Int4
        }
        8 => {
            b.extend_from_slice(&u.arbitrary::<i64>()?.to_be_bytes());
            Type::Int8
        }
        9 => {
            b.extend_from_slice(&u.arbitrary::<u16>()?.to_be_bytes());
            Type::UInt2
        }
        10 => {
            b.extend_from_slice(&u.arbitrary::<u32>()?.to_be_bytes());
            Type::UInt4
        }
        11 => {
            b.extend_from_slice(&u.arbitrary::<u64>()?.to_be_bytes());
            Type::UInt8
        }
        // Interval: i64 micros + i32 days + i32 months, all BE (16 bytes).
        12 => {
            b.extend_from_slice(&u.arbitrary::<i64>()?.to_be_bytes());
            b.extend_from_slice(&u.arbitrary::<i32>()?.to_be_bytes());
            b.extend_from_slice(&u.arbitrary::<i32>()?.to_be_bytes());
            Type::Interval { constraints: None }
        }
        // Jsonb: a version byte (1) followed by real JSON text.
        13 => {
            b.push(1);
            let mut json = String::new();
            push_json(u, &mut json, 3)?;
            b.extend_from_slice(json.as_bytes());
            Type::Jsonb
        }
        14 => {
            push_ascii(u, &mut b, 64)?;
            Type::Name
        }
        // Numeric: i16 ndigits, i16 weight, u16 sign, u16 dscale, then ndigits
        // base-10000 words (each 0..=9999).
        15 => {
            // All five sign words `Numeric::from_sql` accepts. `NaN` and
            // `±Infinity` return early, skipping the dscale/scale validation and
            // the trailing `to_width` plus context-status guard that the finite
            // signs go through, and a real encoding gives them no digit words.
            let sign = *u.choose(&[0x0000u16, 0x4000, 0xC000, 0xD000, 0xF000])?;
            let ndigits: i16 = if sign == 0x0000 || sign == 0x4000 {
                u.int_in_range(0i16..=4)?
            } else {
                0
            };
            // Mostly a well-formed in-range header so the decoder reaches value
            // construction. Occasionally an out-of-band weight/dscale so the
            // scale/precision math runs on extreme exponents.
            let weight = if u.int_in_range(0u8..=7)? == 0 {
                u.arbitrary::<i16>()?
            } else {
                u.int_in_range(-4i16..=4)?
            };
            let dscale = if u.int_in_range(0u8..=7)? == 0 {
                u.arbitrary::<u16>()?
            } else {
                u.int_in_range(0u16..=10)?
            };
            b.extend_from_slice(&ndigits.to_be_bytes());
            b.extend_from_slice(&weight.to_be_bytes());
            b.extend_from_slice(&sign.to_be_bytes());
            b.extend_from_slice(&dscale.to_be_bytes());
            // Each base-10000 digit word should be 0..=9999. Occasionally emit
            // one outside that range. `from_sql` reads the words as `u16` and
            // folds them straight into the accumulator without a bound check, so
            // this is not a validation path but a way to push the accumulator
            // toward the trailing precision guard.
            let oob_words = u.int_in_range(0u8..=7)? == 0;
            for _ in 0..ndigits {
                let word = if oob_words {
                    u.arbitrary::<u16>()?
                } else {
                    u.int_in_range(0u16..=9999)?
                };
                b.extend_from_slice(&word.to_be_bytes());
            }
            gen_numeric_type(u)?
        }
        16 => {
            b.extend_from_slice(&u.arbitrary::<u32>()?.to_be_bytes());
            Type::Oid
        }
        17 => {
            // Occasionally a multi-KB UTF-8 body so the text decoder's
            // validation/copy runs on a large value, not just a short one.
            let max = if u.int_in_range(0u8..=15)? == 0 {
                8192
            } else {
                16
            };
            push_ascii(u, &mut b, max)?;
            Type::Text
        }
        18 => {
            push_ascii(u, &mut b, 16)?;
            Type::BpChar { length: None }
        }
        19 => {
            push_ascii(u, &mut b, 16)?;
            Type::VarChar { max_length: None }
        }
        // Time: i64 BE micros since midnight, in range.
        20 => {
            b.extend_from_slice(&u.int_in_range(0i64..=86_399_999_999)?.to_be_bytes());
            Type::Time { precision: None }
        }
        // Timestamp(tz): i64 BE micros since 2000-01-01. Keep moderate so the
        // CheckedTimestamp range check is reached on the accept path.
        21 => {
            let micros = u.int_in_range(-6_000_000_000_000_000i64..=6_000_000_000_000_000)?;
            b.extend_from_slice(&micros.to_be_bytes());
            Type::Timestamp { precision: None }
        }
        22 => {
            let micros = u.int_in_range(-6_000_000_000_000_000i64..=6_000_000_000_000_000)?;
            b.extend_from_slice(&micros.to_be_bytes());
            Type::TimestampTz { precision: None }
        }
        23 => {
            let uuid: [u8; 16] = u.arbitrary()?;
            b.extend_from_slice(&uuid);
            Type::Uuid
        }
        // mz_timestamp decodes a text u64.
        24 => {
            b.extend_from_slice(u.arbitrary::<u64>()?.to_string().as_bytes());
            Type::MzTimestamp
        }
        // mz_aclitem: grantee role id, grantor role id, u64 LE acl mode.
        _ => {
            push_role_id(u, &mut b)?;
            push_role_id(u, &mut b)?;
            b.extend_from_slice(&u.arbitrary::<u64>()?.to_le_bytes());
            Type::MzAclItem
        }
    };
    Ok((ty, b))
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, the raw mode: any OID + raw remaining bytes.
    // This keeps the not-implemented branches and the crafted-header / wrong
    // length error paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        let oid = u32::from(u.arbitrary::<u16>()?);
        // A typmod other than -1 also covers `from_oid_and_typmod`'s
        // `InvalidTypmod` rejection, which no other mode reaches.
        let typmod = if u.int_in_range(0u8..=3)? == 0 {
            u.arbitrary::<i32>()?
        } else {
            -1
        };
        let rest = u.take_rest();
        if let Ok(ty) = Type::from_oid_and_typmod(oid, typmod) {
            let _ = Value::decode_binary(&ty, rest);
        }
        return Ok(());
    }

    let (ty, mut body) = gen_typed_value(&mut u)?;
    // Occasionally corrupt a byte so the decoders' *content* validation runs and
    // not only their length checks: an unrecognized role-id variant tag, a NUL or
    // invalid UTF-8 in a `name`, a non-numeric `mz_timestamp`, an unknown numeric
    // sign, a wrong jsonb version byte. Truncation alone only ever produces
    // bodies that are short, never bodies that are wrong. For the
    // Materialize-specific types this is the only source of bad content at all,
    // since `Type::from_oid` cannot construct them, so raw mode never sees them.
    if !body.is_empty() && u.int_in_range(0u8..=7)? == 0 {
        let i = u.int_in_range(0usize..=body.len() - 1)?;
        body[i] = u.arbitrary::<u8>()?;
    }
    // Occasionally truncate to hit the exact-length / short-read checks.
    if !body.is_empty() && u.int_in_range(0u8..=7)? == 0 {
        let keep = u.int_in_range(0usize..=body.len())?;
        body.truncate(keep);
    }
    let _ = Value::decode_binary(&ty, &body);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
