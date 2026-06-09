// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_interchange::avro::Decoder` decoding an untrusted Avro
//! message body against a *fuzzer-generated* reader schema instead of one fixed
//! schema. The decoder's behaviour is schema-directed, so a fixed schema only
//! ever walks one set of `AvroFlatDecoder` paths. The bug-prone paths are the
//! schema-dependent ones that a single schema barely touches: `decimal`
//! (unscaled two's-complement
//! bytes -> `Numeric`, where precision/scale and byte length interact), `fixed`
//! (size-N byte runs), `enum` (symbol index -> string, an out-of-range index is
//! attacker-controlled), the logical date/timestamp conversions, and deeply
//! nested records/arrays/maps and unions.
//!
//! Critically, we don't just generate the schema and feed *random bytes* as the
//! body: a strict binary decoder rejects almost all random bytes at the first
//! field, so a random body never reaches the deep decode logic. Instead we
//! generate a structured type (`Ty`), serialize it to the reader-schema JSON,
//! and *Avro-binary-encode a random value against that same type* — so the body
//! is valid by construction and the decoder walks all the way through. Coverage
//! guidance then learns which byte streams produce which shapes. We don't lose
//! the error-path coverage a random body gave, though: a quarter of the inputs
//! feed the raw remaining bytes, and others truncate or single-byte-corrupt the
//! valid encoding. Either way, an accepted schema must never panic.
//!
//! Rather than freeze the schema-dependent knobs at one value each, we vary the
//! ones that drive distinct decode arithmetic: `decimal` precision (1..=39, the
//! `NUMERIC_DATUM_MAX_PRECISION` boundary), scale (0..=precision, where
//! `parse_decimal` rejects scale > precision and `twos_complement_be_to_numeric`
//! interprets it), and the backing `fixed` size — so the two's-complement byte
//! run and the precision/scale interaction are no longer pinned. We also emit a
//! `json` logical field (a `string` tagged `connect.name:io.debezium.data.Json`)
//! whose body is real JSON text, reaching the `AvroFlatDecoder::json` ->
//! `JsonbPacker` path that a plain string never touches; and multi-variant
//! *essential* unions like `["int","string"]` — accepted only as a record field
//! (each non-null variant expands to its own nullable column) and rejected
//! elsewhere — exercising `get_union_columns`' field-invention/expansion logic
//! that the `["null", T]` nullability pattern alone never reaches.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_interchange::avro::Decoder;

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
    })
}

/// A generated Avro type. We keep it structured (rather than going straight to
/// JSON) so that the *body* encoder can walk the exact same type — an array
/// needs one item type in the schema but N encoded values in the body, so the
/// schema and the body can't share a single recursive pass.
enum Ty {
    Bool,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    /// `decimal` logical type over `bytes`: (precision, scale). The unscaled
    /// value is length-prefixed, so the byte-run length is unbounded.
    DecimalBytes(u32, u32),
    /// `decimal` logical type over a `fixed`: (unique-name counter, fixed size,
    /// precision, scale). The wire body is exactly `size` bytes.
    DecimalFixed(u32, u32, u32, u32),
    Date,
    TimestampMillis,
    TimestampMicros,
    Uuid,
    /// `string` tagged with `connect.name = io.debezium.data.Json`, which the
    /// avro schema parser maps to `SchemaPiece::Json` (-> SQL `Jsonb`). The wire
    /// body is a length-prefixed run of valid JSON text.
    Json,
    /// Plain `fixed` byte run: (unique-name counter, size).
    Fixed(u32, u32),
    /// `enum` with symbols A/B/C (unique-name counter).
    Enum(u32),
    /// `record` (unique-name counter, fields). Each field is itself a `Ty`,
    /// except that a field may be an `EssentialUnion`, which is only valid in
    /// this position.
    Record(u32, Vec<Ty>),
    Array(Box<Ty>),
    Map(Box<Ty>),
    /// `["null", T]` — the nullability-pattern union, valid anywhere a single
    /// column is expected (record field, array item, map value).
    Nullable(Box<Ty>),
    /// A multi-variant *essential* union of non-null variants, optionally with a
    /// leading `null`: e.g. `["int","string"]` or `["null","int","string"]`.
    /// `validate_schema_2` rejects this everywhere except as a record field,
    /// where `get_union_columns` expands it to one nullable column per non-null
    /// variant. Stored as (has_null, variants). Generated only as a record field.
    EssentialUnion(bool, Vec<Ty>),
}

/// Generate a `decimal`'s precision/scale. `parse_decimal` requires
/// `0 <= scale <= precision` and the SQL validator caps precision at
/// `NUMERIC_DATUM_MAX_PRECISION` (39); pick within those bounds so the schema is
/// accepted and we vary the decode arithmetic across the whole legal range.
fn gen_decimal_params(u: &mut Unstructured) -> arbitrary::Result<(u32, u32)> {
    let precision = u.int_in_range(1u32..=39)?;
    let scale = u.int_in_range(0u32..=precision)?;
    Ok((precision, scale))
}

/// Generate one syntactically valid Avro type. `counter` keeps named types
/// (record/enum/fixed) unique within the schema — duplicate names make Avro
/// schema parsing fail, wasting the whole input. This never returns an
/// `EssentialUnion` (only valid as a record field — see `gen_field`).
fn gen_ty(u: &mut Unstructured, counter: &mut u32, depth: u32) -> arbitrary::Result<Ty> {
    // Without remaining input (or at max depth) fall back to a primitive so
    // generation always terminates with a valid type.
    let choice = if depth == 0 || u.is_empty() {
        u.int_in_range(0u8..=9)?
    } else {
        u.int_in_range(0u8..=13)?
    };
    Ok(match choice {
        0 => match u.int_in_range(0u8..=6)? {
            0 => Ty::Bool,
            1 => Ty::Int,
            2 => Ty::Long,
            3 => Ty::Float,
            4 => Ty::Double,
            5 => Ty::String,
            _ => Ty::Bytes,
        },
        1 => {
            let (p, s) = gen_decimal_params(u)?;
            Ty::DecimalBytes(p, s)
        }
        2 => {
            *counter += 1;
            let (p, s) = gen_decimal_params(u)?;
            // `fixed` requires a positive size; allow runs both shorter and
            // longer than the canonical 16/24 to vary the two's-complement path.
            let size = u.int_in_range(1u32..=40)?;
            Ty::DecimalFixed(*counter, size, p, s)
        }
        3 => Ty::Date,
        4 => Ty::TimestampMillis,
        5 => Ty::TimestampMicros,
        6 => Ty::Uuid,
        7 => {
            *counter += 1;
            let size = u.int_in_range(0u32..=24)?;
            Ty::Fixed(*counter, size)
        }
        8 => {
            *counter += 1;
            Ty::Enum(*counter)
        }
        9 => Ty::Json,
        10 => {
            *counter += 1;
            let name = *counter;
            let n = u.int_in_range(0u8..=3)?;
            let mut fields = Vec::with_capacity(n.into());
            for _ in 0..n {
                fields.push(gen_field(u, counter, depth - 1)?);
            }
            Ty::Record(name, fields)
        }
        11 => Ty::Array(Box::new(gen_ty(u, counter, depth - 1)?)),
        12 => Ty::Map(Box::new(gen_ty(u, counter, depth - 1)?)),
        _ => Ty::Nullable(Box::new(gen_ty(u, counter, depth - 1)?)),
    })
}

/// Generate a single record field. Usually a plain `gen_ty`, but occasionally a
/// multi-variant essential union — valid only here. Variants are drawn from a
/// set of *distinct* Avro type kinds (Avro rejects a union with two branches of
/// the same unnamed type), each a single-column `validate_schema_2` type.
fn gen_field(u: &mut Unstructured, counter: &mut u32, depth: u32) -> arbitrary::Result<Ty> {
    if depth == 0 || u.is_empty() || u.int_in_range(0u8..=4)? != 0 {
        return gen_ty(u, counter, depth);
    }
    // Pool of distinct, single-column variant kinds (no duplicate Avro kind).
    let pool: &[fn() -> Ty] = &[
        || Ty::Bool,
        || Ty::Int,
        || Ty::Long,
        || Ty::Float,
        || Ty::Double,
        || Ty::String,
        || Ty::Bytes,
    ];
    let nvariants = u.int_in_range(2usize..=pool.len())?;
    // Choose `nvariants` distinct indices into the pool, preserving order.
    let mut chosen = Vec::with_capacity(nvariants);
    let mut idx = 0usize;
    let mut remaining = nvariants;
    while remaining > 0 && idx < pool.len() {
        let left = pool.len() - idx;
        // Pick this index iff we still need as many as are left, or by coin flip.
        if remaining == left || u.int_in_range(0u8..=1)? == 1 {
            chosen.push(pool[idx]());
            remaining -= 1;
        }
        idx += 1;
    }
    let has_null = u.int_in_range(0u8..=1)? == 1;
    Ok(Ty::EssentialUnion(has_null, chosen))
}

/// Serialize a `Ty` to its reader-schema JSON fragment.
fn ty_to_json(ty: &Ty, out: &mut String) {
    match ty {
        Ty::Bool => out.push_str("\"boolean\""),
        Ty::Int => out.push_str("\"int\""),
        Ty::Long => out.push_str("\"long\""),
        Ty::Float => out.push_str("\"float\""),
        Ty::Double => out.push_str("\"double\""),
        Ty::String => out.push_str("\"string\""),
        Ty::Bytes => out.push_str("\"bytes\""),
        Ty::DecimalBytes(p, s) => out.push_str(&format!(
            "{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{p},\"scale\":{s}}}"
        )),
        Ty::DecimalFixed(n, size, p, s) => out.push_str(&format!(
            "{{\"type\":\"fixed\",\"name\":\"D{n}\",\"size\":{size},\"logicalType\":\"decimal\",\"precision\":{p},\"scale\":{s}}}"
        )),
        Ty::Date => out.push_str("{\"type\":\"int\",\"logicalType\":\"date\"}"),
        Ty::TimestampMillis => {
            out.push_str("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        }
        Ty::TimestampMicros => {
            out.push_str("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        }
        Ty::Uuid => out.push_str("{\"type\":\"string\",\"logicalType\":\"uuid\"}"),
        // The avro parser maps a `string` with this connect.name to
        // `SchemaPiece::Json`.
        Ty::Json => out.push_str(
            "{\"type\":\"string\",\"connect.name\":\"io.debezium.data.Json\"}",
        ),
        Ty::Fixed(n, size) => {
            out.push_str(&format!("{{\"type\":\"fixed\",\"name\":\"F{n}\",\"size\":{size}}}"))
        }
        Ty::Enum(n) => out.push_str(&format!(
            "{{\"type\":\"enum\",\"name\":\"E{n}\",\"symbols\":[\"A\",\"B\",\"C\"]}}"
        )),
        Ty::Record(n, fields) => {
            out.push_str(&format!("{{\"type\":\"record\",\"name\":\"R{n}\",\"fields\":["));
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{{\"name\":\"g{i}\",\"type\":"));
                ty_to_json(f, out);
                out.push('}');
            }
            out.push_str("]}");
        }
        Ty::Array(item) => {
            out.push_str("{\"type\":\"array\",\"items\":");
            ty_to_json(item, out);
            out.push('}');
        }
        Ty::Map(values) => {
            out.push_str("{\"type\":\"map\",\"values\":");
            ty_to_json(values, out);
            out.push('}');
        }
        Ty::Nullable(inner) => {
            out.push_str("[\"null\",");
            ty_to_json(inner, out);
            out.push(']');
        }
        Ty::EssentialUnion(has_null, variants) => {
            out.push('[');
            if *has_null {
                out.push_str("\"null\"");
            }
            for (i, v) in variants.iter().enumerate() {
                if *has_null || i > 0 {
                    out.push(',');
                }
                ty_to_json(v, out);
            }
            out.push(']');
        }
    }
}

/// Avro encodes int/long as zig-zag varints.
fn encode_long(n: i64, out: &mut Vec<u8>) {
    let mut z = ((n << 1) ^ (n >> 63)) as u64;
    loop {
        if z & !0x7f == 0 {
            out.push(z as u8);
            return;
        }
        out.push(((z & 0x7f) | 0x80) as u8);
        z >>= 7;
    }
}

/// Encode a length-prefixed run of printable ASCII (valid UTF-8 so `string`
/// decode succeeds and we reach the deep paths).
fn encode_str(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=8)?;
    let mut s = Vec::with_capacity(n);
    for _ in 0..n {
        s.push(u.int_in_range(0x20u8..=0x7e)?);
    }
    encode_long(s.len() as i64, out);
    out.extend_from_slice(&s);
    Ok(())
}

/// Encode a length-prefixed run of arbitrary bytes.
fn encode_bytes(u: &mut Unstructured, max: usize, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=max)?;
    encode_long(n as i64, out);
    for _ in 0..n {
        out.push(u.arbitrary::<u8>()?);
    }
    Ok(())
}

/// Encode a length-prefixed run of valid JSON text — the wire form of a `json`
/// logical field, which the decoder feeds to `serde_json::from_slice`. A small
/// menu of well-formed JSON values covers the `JsonbPacker` shapes (scalars,
/// nested object/array); a fraction of the time we emit deliberately invalid
/// text to keep the `BadJson` error path covered.
fn encode_json(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let s: String = match u.int_in_range(0u8..=8)? {
        0 => "null".into(),
        1 => "true".into(),
        2 => format!("{}", u.arbitrary::<i64>()?),
        3 => format!("{}.5", u.arbitrary::<i32>()?),
        4 => {
            let n = u.int_in_range(0usize..=6)?;
            let mut t = String::from("\"");
            for _ in 0..n {
                t.push(char::from(u.int_in_range(0x20u8..=0x7e)?).max('a'));
            }
            t.push('"');
            t
        }
        5 => "[]".into(),
        6 => "{}".into(),
        7 => format!("[{},{},null]", u.arbitrary::<i32>()?, u.arbitrary::<bool>()?),
        // Not valid JSON: exercise the decoder's BadJson error path.
        _ => "{".into(),
    };
    encode_long(s.len() as i64, out);
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

/// Avro-binary-encode one random value of type `ty` into `out`.
fn encode_value(u: &mut Unstructured, ty: &Ty, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    match ty {
        Ty::Bool => out.push(u.int_in_range(0u8..=1)?),
        Ty::Int | Ty::Date => encode_long(i64::from(u.arbitrary::<i32>()?), out),
        Ty::Long | Ty::TimestampMillis | Ty::TimestampMicros => {
            encode_long(u.arbitrary::<i64>()?, out)
        }
        Ty::Float => out.extend_from_slice(&u.arbitrary::<f32>()?.to_le_bytes()),
        Ty::Double => out.extend_from_slice(&u.arbitrary::<f64>()?.to_le_bytes()),
        Ty::String => encode_str(u, out)?,
        // Small unscaled values stay inside precision; len 0 reaches the
        // empty-bytes decimal path (a fixed bug). Larger runs hit the
        // precision-overflow path.
        Ty::Bytes => encode_bytes(u, 12, out)?,
        // Length-prefixed unscaled two's-complement run; up to 20 bytes spans
        // both in- and out-of-precision values for any precision/scale.
        Ty::DecimalBytes(_, _) => encode_bytes(u, 20, out)?,
        // The fixed-backed decimal reads exactly `size` bytes (no length prefix).
        Ty::DecimalFixed(_, size, _, _) => {
            for _ in 0..*size {
                out.push(u.arbitrary::<u8>()?);
            }
        }
        Ty::Json => encode_json(u, out)?,
        Ty::Uuid => {
            let b: [u8; 16] = u.arbitrary()?;
            let mut s = String::with_capacity(36);
            for (i, byte) in b.iter().enumerate() {
                if matches!(i, 4 | 6 | 8 | 10) {
                    s.push('-');
                }
                s.push_str(&format!("{byte:02x}"));
            }
            encode_long(s.len() as i64, out);
            out.extend_from_slice(s.as_bytes());
        }
        Ty::Fixed(_, size) => {
            for _ in 0..*size {
                out.push(u.arbitrary::<u8>()?);
            }
        }
        // Symbols are A/B/C (indices 0..=2); index 3 is an out-of-range index
        // the decoder must reject without panicking.
        Ty::Enum(_) => encode_long(u.int_in_range(0i64..=3)?, out),
        Ty::Record(_, fields) => {
            for f in fields {
                encode_value(u, f, out)?;
            }
        }
        Ty::Array(item) => {
            let n = u.int_in_range(0i64..=3)?;
            if n > 0 {
                encode_long(n, out);
                for _ in 0..n {
                    encode_value(u, item, out)?;
                }
            }
            encode_long(0, out); // end-of-blocks marker
        }
        Ty::Map(values) => {
            let n = u.int_in_range(0i64..=3)?;
            if n > 0 {
                encode_long(n, out);
                for _ in 0..n {
                    encode_str(u, out)?; // key
                    encode_value(u, values, out)?;
                }
            }
            encode_long(0, out);
        }
        // union ["null", T]: branch 0 = null (no bytes), branch 1 = T.
        Ty::Nullable(inner) => {
            if u.int_in_range(0u8..=3)? == 0 {
                encode_long(0, out);
            } else {
                encode_long(1, out);
                encode_value(u, inner, out)?;
            }
        }
        // Essential union: branches are written in declaration order. With a
        // leading `null`, branch 0 = null and branch `i+1` = variant `i`;
        // without, branch `i` = variant `i`. Pick one branch and encode it.
        Ty::EssentialUnion(has_null, variants) => {
            let nbranches = variants.len() + usize::from(*has_null);
            let branch = u.int_in_range(0usize..=nbranches - 1)?;
            encode_long(branch as i64, out);
            if *has_null {
                if branch > 0 {
                    encode_value(u, &variants[branch - 1], out)?;
                }
            } else {
                encode_value(u, &variants[branch], out)?;
            }
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // Top-level reader schema: a record whose fields span everything
    // `validate_schema_2` accepts.
    let mut counter = 0u32;
    let nfields = u.int_in_range(1u8..=8)?;
    let mut fields = Vec::with_capacity(nfields.into());
    for _ in 0..nfields {
        // Record fields, so essential unions (`gen_field`) are reachable here.
        fields.push(gen_field(&mut u, &mut counter, 3)?);
    }
    let row = Ty::Record(0, fields);

    let mut schema = String::new();
    ty_to_json(&row, &mut schema);

    // No CSR client and confluent_wire_format = false, so decode is
    // self-contained (no network) over the generated reader schema.
    let Ok(mut decoder) = Decoder::new(&schema, &[], None, "fuzz".into(), false) else {
        return Ok(());
    };

    // Body: usually a valid encoding (so the decoder runs deep), but a quarter
    // of the time the raw remaining bytes (the original random-body behavior),
    // and otherwise a valid encoding occasionally truncated or single-byte
    // corrupted. The non-valid forms keep the decoder's error paths — short
    // read, bad length/union tag, inconsistent content — covered.
    let body = if u.int_in_range(0u8..=3)? == 0 {
        u.take_rest().to_vec()
    } else {
        let mut b = Vec::new();
        encode_value(&mut u, &row, &mut b)?;
        match u.int_in_range(0u8..=9)? {
            0 if !b.is_empty() => {
                let keep = u.int_in_range(0usize..=b.len())?;
                b.truncate(keep);
            }
            1 if !b.is_empty() => {
                let i = u.int_in_range(0usize..=b.len() - 1)?;
                b[i] ^= u.arbitrary::<u8>()?;
            }
            _ => {}
        }
        b
    };

    let mut bytes = body.as_slice();
    let _ = rt().block_on(decoder.decode(&mut bytes));
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
