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
//! A random byte string almost never satisfies these strict decoders — an exact
//! length check, a version byte, base-10000 digit bounds, a role-id variant tag
//! — so feeding raw bytes leaves the decoders barely exercised. Instead we pick
//! a type and *encode a valid binary value for it* (numeric header + digits, the
//! 16-byte interval triple, the 26-byte mz_aclitem, a jsonb version byte plus
//! real JSON, in-range date/time/timestamp), so the decoder runs all the way to
//! the value-construction and range-check logic. We still occasionally truncate
//! the valid encoding (to hit the length-validation / short-read paths) and, a
//! quarter of the time, fall back to the original "any OID, raw bytes" mode so
//! the not-implemented branches and crafted-header paths stay covered.
//!
//! A few arms also reach past the "happy" shape on purpose: the numeric header
//! sometimes carries an out-of-band weight/dscale or digit words outside
//! `0..=9999` (negative or `>9999`) to exercise the base-10000 digit-bound and
//! scale math, and the bytea/text bodies are occasionally multi-KB so the
//! decoders' allocation/validation paths run on a large value rather than a
//! handful of bytes.

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
        // Date: i32 BE days since 2000-01-01; from_pg_epoch range-checks.
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
            let json: &[u8] = match u.int_in_range(0u8..=7)? {
                0 => b"null",
                1 => b"true",
                2 => b"123",
                3 => b"-4.5",
                4 => b"\"s\"",
                5 => b"[1,2,3]",
                6 => b"{\"a\":1}",
                _ => b"[]",
            };
            b.extend_from_slice(json);
            Type::Jsonb
        }
        14 => {
            push_ascii(u, &mut b, 64)?;
            Type::Name
        }
        // Numeric: i16 ndigits, i16 weight, u16 sign, u16 dscale, then ndigits
        // base-10000 words (each 0..=9999).
        15 => {
            let nan = u.int_in_range(0u8..=5)? == 0;
            let (ndigits, sign): (i16, u16) = if nan {
                (0, 0xC000)
            } else if u.int_in_range(0u8..=1)? == 0 {
                (u.int_in_range(0i16..=4)?, 0x0000)
            } else {
                (u.int_in_range(0i16..=4)?, 0x4000)
            };
            // Mostly a well-formed in-range header so the decoder reaches value
            // construction; occasionally an out-of-band weight/dscale so the
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
            // Each base-10000 digit word should be 0..=9999; occasionally emit
            // an out-of-band word (>9999, or the full i16 range incl. negative)
            // to exercise the digit-bound validation path.
            let oob_words = u.int_in_range(0u8..=7)? == 0;
            for _ in 0..ndigits {
                let word = if oob_words {
                    u.arbitrary::<i16>()?
                } else {
                    u.int_in_range(0i16..=9999)?
                };
                b.extend_from_slice(&word.to_be_bytes());
            }
            Type::Numeric { constraints: None }
        }
        16 => {
            b.extend_from_slice(&u.arbitrary::<u32>()?.to_be_bytes());
            Type::Oid
        }
        17 => {
            // Occasionally a multi-KB UTF-8 body so the text decoder's
            // validation/copy runs on a large value, not just a short one.
            let max = if u.int_in_range(0u8..=15)? == 0 { 8192 } else { 16 };
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
        // Timestamp(tz): i64 BE micros since 2000-01-01; keep moderate so the
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
    // A quarter of the time, the original mode: any OID + raw remaining bytes.
    // This keeps the not-implemented branches and the crafted-header / wrong
    // length error paths covered.
    if u.int_in_range(0u8..=3)? == 0 {
        let oid = u32::from(u.arbitrary::<u16>()?);
        let rest = u.take_rest();
        if let Ok(ty) = Type::from_oid(oid) {
            let _ = Value::decode_binary(&ty, rest);
        }
        return Ok(());
    }

    let (ty, mut body) = gen_typed_value(&mut u)?;
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
