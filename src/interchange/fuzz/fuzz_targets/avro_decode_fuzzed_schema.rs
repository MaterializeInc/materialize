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
//! and *Avro-binary-encode a random value against that same type*, so the body
//! is valid by construction and the decoder walks all the way through. Coverage
//! guidance then learns which byte streams produce which shapes. We don't lose
//! the error-path coverage a random body gave, though: a quarter of the inputs
//! feed the raw remaining bytes, and others truncate or single-byte-corrupt the
//! valid encoding. Either way, an accepted schema must never panic.
//!
//! Beyond "never panic", we add one error oracle. Most bodies *should* be
//! rejected (random bytes, truncations, an out-of-range `enum` index, an
//! over-precision `decimal`, invalid `json`), so a decode `Err` is usually the
//! correct outcome and is discarded. But when the body is a *clean* encoding of
//! a type tree that is guaranteed decodable for any value the encoder emits
//! (plain scalars, `fixed`, and structural composites over them, see
//! `decode_infallible`), the decoder is round-tripping bytes it *must* accept,
//! so there a decode error is a real bug and we assert success. A panic-only
//! oracle never notices a "valid input wrongly rejected" regression (cf.
//! #37087's deferred union-promotion error).
//!
//! A fraction of inputs instead drive a round-trip *correctness* oracle
//! (`run_roundtrip`): a decode that succeeds but yields the *wrong* datum slips
//! past both the panic and the decode-success oracles. So we build a record of
//! plain scalars (bool/int/long/string/bytes and their `["null", T]` form) whose
//! decoded `Datum` is exactly determined, encode it, decode it, and assert the
//! decoded `Row` equals the values we put in.
//!
//! Rather than freeze the schema-dependent knobs at one value each, we vary the
//! ones that drive distinct decode arithmetic: `decimal` precision (1..=39, the
//! `NUMERIC_DATUM_MAX_PRECISION` boundary), scale (0..=precision, where
//! `parse_decimal` rejects scale > precision and `twos_complement_be_to_numeric`
//! interprets it), and the backing `fixed` size, so the two's-complement byte
//! run and the precision/scale interaction are not pinned. The decimal
//! *value* bytes are likewise biased (see `push_twos_complement` /
//! `gen_decimal_len`) toward the patterns that stress that arithmetic rather
//! than only uniform-random runs: the empty run (== 0), all-`0x00`/`0xFF` sign
//! extension, the `0x80`/`0x7F` magnitude extremes, and lengths bracketing the
//! narrow/wide split (17) and the wide/overflow regime. We also emit a
//! `json` logical field (a `string` tagged `connect.name:io.debezium.data.Json`)
//! whose body is real JSON text, reaching the `AvroFlatDecoder::json` ->
//! `JsonbPacker` path that a plain string never touches. And multi-variant
//! *essential* unions like `["int","string"]`, accepted only as a record field
//! (each non-null variant expands to its own nullable column) and rejected
//! elsewhere, exercising `get_union_columns`' field-invention/expansion logic
//! that the `["null", T]` nullability pattern alone never reaches.

#![no_main]

use std::sync::OnceLock;

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_interchange::avro::{Decoder, WriterSchemaProvider};
use mz_repr::{Datum, Row};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
    })
}

/// A generated Avro type. We keep it structured (rather than going straight to
/// JSON) so that the *body* encoder can walk the exact same type. An array
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
    /// `["null", T]`, the nullability-pattern union, valid anywhere a single
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
/// `NUMERIC_DATUM_MAX_PRECISION` (39). Pick within those bounds so the schema is
/// accepted and we vary the decode arithmetic across the whole legal range.
fn gen_decimal_params(u: &mut Unstructured) -> arbitrary::Result<(u32, u32)> {
    let precision = u.int_in_range(1u32..=39)?;
    let scale = u.int_in_range(0u32..=precision)?;
    Ok((precision, scale))
}

/// Generate one syntactically valid Avro type. `counter` keeps named types
/// (record/enum/fixed) unique within the schema, since duplicate names make Avro
/// schema parsing fail, wasting the whole input. This never returns an
/// `EssentialUnion` (only valid as a record field, see `gen_field`).
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
            // `fixed` requires a positive size. Allow runs both shorter and
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
/// multi-variant essential union, valid only here. Variants are drawn from a
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

/// Encode a length-prefixed run of valid JSON text, the wire form of a `json`
/// logical field, which the decoder feeds to `serde_json::from_slice`. A small
/// menu of well-formed JSON values covers the `JsonbPacker` shapes (scalars,
/// nested object/array). A fraction of the time we emit deliberately invalid
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

/// Push `len` bytes of a two's-complement `decimal` value, biased toward the
/// sign-extension and boundary patterns that exercise the most intricate decode
/// arithmetic in `numeric::twos_complement_be_to_numeric`: the narrow/wide split
/// (`len <= 17`), the `negate_twos_complement_le` path (any negative value), and
/// the wide-representation precision-overflow handling. Most of the time it
/// still emits a uniform-random run so coverage guidance keeps exploring.
fn push_twos_complement(u: &mut Unstructured, len: usize, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let fill = match u.int_in_range(0u8..=9)? {
        0 => 0x00u8, // zero / positive sign-extension
        1 => 0xFF,   // -1 / negative sign-extension
        2 => 0x80,   // sign bit set: most-negative leading byte
        3 => 0x7F,   // largest-magnitude positive leading byte
        // Uniform-random run the majority of the time.
        _ => {
            for _ in 0..len {
                out.push(u.arbitrary::<u8>()?);
            }
            return Ok(());
        }
    };
    // A constant run, optionally with one differing leading byte to drop a
    // boundary value into an otherwise sign-extended field.
    let lead = if len > 0 && u.int_in_range(0u8..=1)? == 1 {
        Some(u.arbitrary::<u8>()?)
    } else {
        None
    };
    for i in 0..len {
        out.push(if i == 0 { lead.unwrap_or(fill) } else { fill });
    }
    Ok(())
}

/// Pick the length of a `decimal`-over-`bytes` run, biased to the lengths that
/// bracket the narrow/wide split (17) and the wide/overflow regime, plus the
/// degenerate empty and single-byte cases.
fn gen_decimal_len(u: &mut Unstructured) -> arbitrary::Result<usize> {
    Ok(match u.int_in_range(0u8..=9)? {
        0 => 0,
        1 => 1,
        2 => 16,
        3 => 17,
        4 => 18,
        5 => 40,
        _ => u.int_in_range(0usize..=40)?,
    })
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
        Ty::Bytes => encode_bytes(u, 12, out)?,
        // Length-prefixed unscaled two's-complement run. The length (incl. the
        // empty and narrow/wide-boundary cases) and the byte pattern are both
        // biased toward the values that stress the decimal decode arithmetic. A
        // zero-length run is the empty-decimal == 0 case (regression for the
        // numeric.rs out-of-bounds panic).
        Ty::DecimalBytes(_, _) => {
            let len = gen_decimal_len(u)?;
            encode_long(len as i64, out);
            push_twos_complement(u, len, out)?;
        }
        // The fixed-backed decimal reads exactly `size` bytes (no length prefix),
        // so only the byte pattern varies.
        Ty::DecimalFixed(_, size, _, _) => {
            push_twos_complement(u, usize::try_from(*size).expect("fixed size fits usize"), out)?;
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
        // Symbols are A/B/C (indices 0..=2). Index 3 is an out-of-range index
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
        // leading `null`, branch 0 = null and branch `i+1` = variant `i`.
        // Without, branch `i` = variant `i`. Pick one branch and encode it.
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

/// Whether *every* value `encode_value` can emit for `ty` is guaranteed to
/// decode without error, i.e. the type carries no value-level validation the
/// encoder can (deliberately) violate. When this holds for an uncorrupted body,
/// a decode failure is a real bug, not an expected error path, so the caller
/// can assert success instead of falling back to the panic-only oracle.
///
/// This intentionally EXCLUDES the types the encoder can drive out of their
/// target domain, each of which the decoder is *supposed* to reject:
///   * `enum`: the encoder emits an out-of-range symbol index (3).
///   * `json`: the encoder sometimes emits invalid JSON (the `BadJson` path).
///   * `decimal` (bytes / fixed): an unscaled run can exceed the precision.
///   * `date` / `timestamp-*`: an arbitrary `int` / `long` can fall outside the
///     representable chrono range.
///   * `uuid`: kept out for safety even though the encoder writes canonical
///     hex (the decoder still UTF-8- and `Uuid::parse_str`-validates it).
///
/// Everything left always round-trips for any value the encoder produces:
///   * plain scalars: `bool` (0/1), `int`/`long` (canonical varints),
///     `float`/`double` (raw IEEE bytes, NaN included), `string` (the encoder
///     writes printable ASCII, which is valid UTF-8), `bytes`, and `fixed`
///     (raw byte runs, no validation).
///   * structural composites: records, arrays, maps (the decoder dedups keys
///     via a `BTreeMap`, so even colliding ASCII keys are fine), and the
///     `["null", T]` / essential unions, provided their children are decodable.
fn decode_infallible(ty: &Ty) -> bool {
    match ty {
        Ty::Bool
        | Ty::Int
        | Ty::Long
        | Ty::Float
        | Ty::Double
        | Ty::String
        | Ty::Bytes
        | Ty::Fixed(_, _) => true,
        Ty::DecimalBytes(_, _)
        | Ty::DecimalFixed(_, _, _, _)
        | Ty::Date
        | Ty::TimestampMillis
        | Ty::TimestampMicros
        | Ty::Uuid
        | Ty::Json
        | Ty::Enum(_) => false,
        Ty::Record(_, fields) => fields.iter().all(decode_infallible),
        Ty::Array(item) | Ty::Map(item) | Ty::Nullable(item) => decode_infallible(item),
        Ty::EssentialUnion(_, variants) => variants.iter().all(decode_infallible),
    }
}

/// A plain-scalar column for the round-trip correctness oracle. Restricted to
/// the types whose decoded `Datum` is exactly determined by the encoded value,
/// with no float NaN, no logical-type conversion, no multi-column union
/// expansion.
#[derive(Clone)]
enum RtVal {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Str(String),
    Bytes(Vec<u8>),
}

impl RtVal {
    /// The `Datum` the Avro decoder must produce for this value (see the
    /// `AvroFlatDecoder` scalar handlers: `int`->`Int32`, `long`->`Int64`,
    /// `boolean`->`True`/`False`, `string`->`String`, `bytes`->`Bytes`, and a
    /// selected-null `["null", T]` branch -> a single `Datum::Null`).
    fn datum(&self) -> Datum<'_> {
        match self {
            RtVal::Null => Datum::Null,
            RtVal::Bool(true) => Datum::True,
            RtVal::Bool(false) => Datum::False,
            RtVal::Int(v) => Datum::Int32(*v),
            RtVal::Long(v) => Datum::Int64(*v),
            RtVal::Str(s) => Datum::String(s),
            RtVal::Bytes(b) => Datum::Bytes(b),
        }
    }

    /// Avro-binary-encode this value as the body of its column. `nullable`
    /// columns are `["null", T]`: branch 0 = null, branch 1 = the value.
    fn encode(&self, nullable: bool, out: &mut Vec<u8>) {
        if nullable {
            encode_long(if matches!(self, RtVal::Null) { 0 } else { 1 }, out);
        }
        match self {
            RtVal::Null => {} // null branch already written
            RtVal::Bool(b) => out.push(u8::from(*b)),
            RtVal::Int(v) => encode_long(i64::from(*v), out),
            RtVal::Long(v) => encode_long(*v, out),
            RtVal::Str(s) => {
                encode_long(s.len() as i64, out);
                out.extend_from_slice(s.as_bytes());
            }
            RtVal::Bytes(b) => {
                encode_long(b.len() as i64, out);
                out.extend_from_slice(b);
            }
        }
    }
}

/// Round-trip *correctness* oracle. The panic / decode-success oracles only see
/// crashes and wrongly-rejected input. A decode that succeeds but yields the
/// *wrong* datum slips past both. So: build a record of plain scalars whose
/// decoded `Datum` is fully determined, encode it, decode it, and assert the
/// decoded `Row` is exactly the values we put in. Restricted to
/// bool/int/long/string/bytes (and their `["null", T]` form), trivial, exact
/// `Datum` mappings with no float NaN, logical-type, or union-column-expansion
/// ambiguity, so any mismatch is a real decoder bug, never a harness artifact.
fn run_roundtrip(u: &mut Unstructured) -> arbitrary::Result<()> {
    let ncols = u.int_in_range(1usize..=8)?;
    let mut cols: Vec<(RtVal, bool)> = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let nullable = u.int_in_range(0u8..=1)? == 1;
        // A nullable column is null a quarter of the time.
        let val = if nullable && u.int_in_range(0u8..=3)? == 0 {
            RtVal::Null
        } else {
            match u.int_in_range(0u8..=4)? {
                0 => RtVal::Bool(u.int_in_range(0u8..=1)? == 1),
                1 => RtVal::Int(u.arbitrary::<i32>()?),
                2 => RtVal::Long(u.arbitrary::<i64>()?),
                3 => {
                    let n = u.int_in_range(0usize..=8)?;
                    let mut s = String::with_capacity(n);
                    for _ in 0..n {
                        s.push(char::from(u.int_in_range(0x20u8..=0x7e)?));
                    }
                    RtVal::Str(s)
                }
                _ => {
                    let n = u.int_in_range(0usize..=8)?;
                    let mut b = Vec::with_capacity(n);
                    for _ in 0..n {
                        b.push(u.arbitrary::<u8>()?);
                    }
                    RtVal::Bytes(b)
                }
            }
        };
        cols.push((val, nullable));
    }

    // Reader schema: a record of the chosen columns.
    let mut schema = String::from("{\"type\":\"record\",\"name\":\"RT\",\"fields\":[");
    for (i, (val, nullable)) in cols.iter().enumerate() {
        if i > 0 {
            schema.push(',');
        }
        let t = match val {
            // For a null cell the column type only needs to be *some* scalar.
            // The decoded datum is `Null` regardless. Use `int` as a stand-in.
            RtVal::Null | RtVal::Int(_) => "\"int\"",
            RtVal::Bool(_) => "\"boolean\"",
            RtVal::Long(_) => "\"long\"",
            RtVal::Str(_) => "\"string\"",
            RtVal::Bytes(_) => "\"bytes\"",
        };
        if *nullable {
            schema.push_str(&format!("{{\"name\":\"c{i}\",\"type\":[\"null\",{t}]}}"));
        } else {
            schema.push_str(&format!("{{\"name\":\"c{i}\",\"type\":{t}}}"));
        }
    }
    schema.push_str("]}");

    let Ok(mut decoder) = Decoder::new(&schema, &[], WriterSchemaProvider::None, "fuzz".into()) else {
        // A record of plain scalars always validates. If not, nothing to check.
        return Ok(());
    };

    let mut body = Vec::new();
    for (val, nullable) in &cols {
        val.encode(*nullable, &mut body);
    }

    let mut expected = Row::default();
    {
        let mut packer = expected.packer();
        for (val, _) in &cols {
            packer.push(val.datum());
        }
    }

    let mut bytes = body.as_slice();
    match rt().block_on(decoder.decode(&mut bytes)) {
        Ok(Ok(decoded)) => assert_eq!(
            decoded, expected,
            "Avro decode produced the wrong row for a plain-scalar record\nschema = {schema}",
        ),
        Ok(Err(e)) => panic!("plain-scalar record failed to decode (schema = {schema}): {e}"),
        Err(e) => panic!("plain-scalar record hit a transient decode error (schema = {schema}): {e}"),
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A fraction of inputs go to the round-trip correctness oracle (decoded row
    // must equal the encoded values). The rest drive the schema-directed
    // panic / decode-success target below.
    if u.int_in_range(0u8..=3)? == 0 {
        return run_roundtrip(&mut u);
    }

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
    let Ok(mut decoder) = Decoder::new(&schema, &[], WriterSchemaProvider::None, "fuzz".into()) else {
        return Ok(());
    };

    // Body: usually a valid encoding (so the decoder runs deep), but a quarter
    // of the time the raw remaining bytes, and otherwise a valid encoding
    // occasionally truncated or single-byte corrupted. The non-valid forms keep
    // the decoder's error paths covered: short read, bad length/union tag,
    // inconsistent content.
    // `assert_success` is set only when the body is a *clean* (uncorrupted)
    // encoding of a type tree every node of which is guaranteed to decode for
    // any value the encoder can produce (see `decode_infallible`). In that one
    // case the decoder is round-tripping bytes it must accept, so a decode
    // *error* is a real bug. The panic-only oracle that the random-bytes,
    // truncated, corrupted, and value-validated (`enum`/`decimal`/`json`/…)
    // arms rely on would silently miss it, the same class of regression as the
    // deferred union-promotion error in #37087.
    let mut assert_success = false;
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
            // Uncorrupted body: if its type is guaranteed decodable, the decode
            // below must succeed.
            _ => assert_success = decode_infallible(&row),
        }
        b
    };

    let mut bytes = body.as_slice();
    let result = rt().block_on(decoder.decode(&mut bytes));
    if assert_success {
        let _ = result.expect(
            "decoder rejected a clean, in-range Avro body whose every field is guaranteed \
             decodable; such a round-trip must succeed",
        );
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
