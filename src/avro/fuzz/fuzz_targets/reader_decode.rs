// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: decoding arbitrary bytes as an Avro object-container file
//! (header + blocks) must not crash. Avro bytes arrive from Kafka and
//! externally-managed schema registries, so any panic/SEGV reachable from
//! the wire format is a real availability bug.
//!
//! An OCF starts with a 4-byte magic, a metadata map carrying the writer
//! schema, and a 16-byte sync marker, then length-framed data blocks each
//! terminated by that same sync marker. Random bytes fail the magic check on
//! the first four bytes, so a raw-byte body never gets past `Reader::new`. The
//! block iteration, sync-marker matching, and per-object value decode (the bulk
//! of the surface) then stay completely unexercised. So we *build* a valid
//! container: generate a structured writer schema (`Ty`), serialize it into the
//! header's `avro.schema` metadata, and Avro-binary-encode the objects of each
//! block against that schema.
//!
//! The compression codec is chosen per file rather than hardcoded to `null`,
//! which exercises the otherwise-dead decompress dispatch in `read_block_next`:
//!   * `null`: the encoded block bytes pass straight through.
//!   * `deflate` with *correctly* compressed bytes: drives the happy-path
//!     `Codec::decompress` (flate2) round-trip the writer normally produces.
//!   * `deflate` with the *raw* (uncompressed) bytes left in place: the deflate
//!     decoder hits a malformed stream, exercising the decompress-error path.
//!   * an unrecognized codec string: the `UnrecognizedCodec` header-parse
//!     error. (`snappy` is gated behind a Cargo feature that this fuzz crate
//!     does not enable, so it is intentionally not generated.)
//! We also corrupt the declared block framing. `read_block_next` runs both the
//! object-count and the byte-size prefix through the `safe_len` allocation guard
//! before it touches the payload, so a magnitude past `MAX_ALLOCATION_BYTES`
//! never reaches anything downstream of that guard. The lies therefore straddle
//! it deliberately:
//!   * a count or size past the budget, which `safe_len` must reject rather than
//!     attempt a giant allocation.
//!   * a count over the objects the block actually encodes but inside the
//!     budget, which `safe_len` accepts. `bound_block_object_count` must then
//!     reject any count the payload could not hold, and one it accepts must
//!     terminate against the bytes present instead of spinning.
//!   * a size over the bytes the block actually carries but inside the budget,
//!     so `fill_buf` runs and takes the short-read path.
//! And we still hit the structural error paths, occasionally a corrupted trailing
//! sync marker (the mismatch path) or a whole-file truncation mid-block (another
//! short read). Decoding must never panic any of these ways.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_avro::{Codec, Reader};

/// The ceiling `util::safe_len` applies to every length read off the wire, from
/// the private `mz_avro::util::MAX_ALLOCATION_BYTES`. Only used to pick the
/// magnitude of a corrupted block prefix, so drift merely shifts which side of
/// the guard a generated prefix lands on.
const MAX_ALLOCATION_BYTES: i64 = 512 * 1024 * 1024;

/// A generated Avro type. Structured (not straight-to-JSON) so the object
/// encoder can walk the same type. An array is one item type in the schema but
/// N item values in a block.
enum Ty {
    Null,
    Bool,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    Fixed(u32, u32),
    Enum(u32),
    Record(u32, Vec<Ty>),
    Array(Box<Ty>),
    Map(Box<Ty>),
    Nullable(Box<Ty>),
}

fn gen_ty(u: &mut Unstructured, counter: &mut u32, depth: u32) -> arbitrary::Result<Ty> {
    let choice = if depth == 0 || u.is_empty() {
        u.int_in_range(0u8..=9)?
    } else {
        u.int_in_range(0u8..=13)?
    };
    Ok(match choice {
        0 => Ty::Null,
        1 => Ty::Bool,
        2 => Ty::Int,
        3 => Ty::Long,
        4 => Ty::Float,
        5 => Ty::Double,
        6 => Ty::String,
        7 => Ty::Bytes,
        8 => {
            *counter += 1;
            // Size 0 is rejected outright ("Fixed values require a positive size
            // attribute"), which would fail the header parse and discard the
            // input before a single block decodes. `avro_schema_parse` covers
            // that rejection deliberately.
            let size = u.int_in_range(1u32..=24)?;
            Ty::Fixed(*counter, size)
        }
        9 => {
            *counter += 1;
            Ty::Enum(*counter)
        }
        10 => {
            *counter += 1;
            let name = *counter;
            let n = u.int_in_range(0u8..=3)?;
            let mut fields = Vec::with_capacity(n.into());
            for _ in 0..n {
                fields.push(gen_ty(u, counter, depth - 1)?);
            }
            Ty::Record(name, fields)
        }
        11 => Ty::Array(Box::new(gen_ty(u, counter, depth - 1)?)),
        12 => Ty::Map(Box::new(gen_ty(u, counter, depth - 1)?)),
        _ => {
            // `Ty::Nullable` is the only union this target emits, and the parser
            // rejects both `["null","null"]` and a union nested directly inside a
            // union. Flatten a nested nullable and swap a bare `null` for a type
            // that keeps the union valid, so the header still parses. Those two
            // rejections belong to `avro_schema_parse`. Here they would only
            // discard the input before any block decodes.
            let mut inner = gen_ty(u, counter, depth - 1)?;
            while let Ty::Nullable(nested) = inner {
                inner = *nested;
            }
            if matches!(inner, Ty::Null) {
                inner = Ty::Long;
            }
            Ty::Nullable(Box::new(inner))
        }
    })
}

fn ty_to_json(ty: &Ty, out: &mut String) {
    match ty {
        Ty::Null => out.push_str("\"null\""),
        Ty::Bool => out.push_str("\"boolean\""),
        Ty::Int => out.push_str("\"int\""),
        Ty::Long => out.push_str("\"long\""),
        Ty::Float => out.push_str("\"float\""),
        Ty::Double => out.push_str("\"double\""),
        Ty::String => out.push_str("\"string\""),
        Ty::Bytes => out.push_str("\"bytes\""),
        Ty::Fixed(n, size) => out.push_str(&format!(
            "{{\"type\":\"fixed\",\"name\":\"F{n}\",\"size\":{size}}}"
        )),
        Ty::Enum(n) => out.push_str(&format!(
            "{{\"type\":\"enum\",\"name\":\"E{n}\",\"symbols\":[\"A\",\"B\",\"C\"]}}"
        )),
        Ty::Record(n, fields) => {
            out.push_str(&format!(
                "{{\"type\":\"record\",\"name\":\"R{n}\",\"fields\":["
            ));
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
    }
}

/// Avro encodes int/long (and all block counts and lengths) as zig-zag varints.
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

/// Encode length-prefixed bytes (the wire form of both `string` and `bytes`,
/// and of each Avro map/metadata entry).
fn encode_blob(bytes: &[u8], out: &mut Vec<u8>) {
    encode_long(bytes.len() as i64, out);
    out.extend_from_slice(bytes);
}

fn encode_str(u: &mut Unstructured, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    let n = u.int_in_range(0usize..=8)?;
    let mut s = Vec::with_capacity(n);
    for _ in 0..n {
        s.push(u.int_in_range(0x20u8..=0x7e)?);
    }
    encode_blob(&s, out);
    Ok(())
}

/// Avro-binary-encode one value of type `ty`.
fn encode_value(u: &mut Unstructured, ty: &Ty, out: &mut Vec<u8>) -> arbitrary::Result<()> {
    match ty {
        Ty::Null => {}
        Ty::Bool => out.push(u.int_in_range(0u8..=1)?),
        Ty::Int => encode_long(i64::from(u.arbitrary::<i32>()?), out),
        Ty::Long => encode_long(u.arbitrary::<i64>()?, out),
        Ty::Float => out.extend_from_slice(&u.arbitrary::<f32>()?.to_le_bytes()),
        Ty::Double => out.extend_from_slice(&u.arbitrary::<f64>()?.to_le_bytes()),
        Ty::String => encode_str(u, out)?,
        Ty::Bytes => {
            let n = u.int_in_range(0usize..=12)?;
            let mut b = Vec::with_capacity(n);
            for _ in 0..n {
                b.push(u.arbitrary::<u8>()?);
            }
            encode_blob(&b, out);
        }
        Ty::Fixed(_, size) => {
            for _ in 0..*size {
                out.push(u.arbitrary::<u8>()?);
            }
        }
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
            encode_long(0, out);
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
        Ty::Nullable(inner) => {
            if u.int_in_range(0u8..=3)? == 0 {
                encode_long(0, out); // branch 0 = null
            } else {
                encode_long(1, out); // branch 1 = T
                encode_value(u, inner, out)?;
            }
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    // A quarter of the time, feed the raw remaining bytes: this exercises the
    // magic-byte rejection and the header-parse error paths that a well-formed
    // container always skips.
    if u.int_in_range(0u8..=3)? == 0 {
        let data = u.take_rest();
        if let Ok(reader) = Reader::new(data) {
            for item in reader {
                let _ = item;
            }
        }
        return Ok(());
    }

    // Writer schema: a record (the usual OCF top-level), spanning the value
    // types the general decoder walks.
    let mut counter = 0u32;
    let n = u.int_in_range(1u8..=6)?;
    let mut fields = Vec::with_capacity(n.into());
    for _ in 0..n {
        fields.push(gen_ty(&mut u, &mut counter, 3)?);
    }
    let top = Ty::Record(0, fields);
    let mut schema = String::new();
    ty_to_json(&top, &mut schema);

    // Pick a per-file codec. `CodecMode` says both what string goes in the
    // header and how (or whether) each block's encoded bytes get compressed, so
    // we can drive the happy-path decompress *and* the decompress-error path.
    enum CodecMode {
        Null,
        /// Correctly deflate-compress each block (the writer's normal output).
        Deflate,
        /// Declare `deflate` but leave the bytes uncompressed (malformed stream).
        DeflateGarbage,
        /// An unrecognized codec name → `UnrecognizedCodec` at header parse.
        Unrecognized,
    }
    let codec_mode = match u.int_in_range(0u8..=5)? {
        0 | 1 => CodecMode::Null,
        2 | 3 => CodecMode::Deflate,
        4 => CodecMode::DeflateGarbage,
        _ => CodecMode::Unrecognized,
    };
    let codec_name: &[u8] = match codec_mode {
        CodecMode::Null => b"null",
        CodecMode::Deflate | CodecMode::DeflateGarbage => b"deflate",
        // A name that is never a real codec. `Reader::new` must reject it
        // cleanly rather than panic.
        CodecMode::Unrecognized => b"lz4-but-not-really",
    };

    let mut out = Vec::new();
    out.extend_from_slice(b"Obj\x01"); // OCF magic
    // File metadata: a map<string,bytes> with the writer schema and the chosen
    // codec, as one block of two entries.
    encode_long(2, &mut out);
    encode_blob(b"avro.schema", &mut out);
    encode_blob(schema.as_bytes(), &mut out);
    encode_blob(b"avro.codec", &mut out);
    encode_blob(codec_name, &mut out);
    encode_long(0, &mut out); // end of metadata map
    let sync: [u8; 16] = u.arbitrary()?;
    out.extend_from_slice(&sync);

    // Data blocks: each is object-count, byte-size, the (possibly compressed)
    // encoded objects, then the file's sync marker.
    let nblocks = u.int_in_range(0u8..=2)?;
    for _ in 0..nblocks {
        let nobj = u.int_in_range(1i64..=3)?;
        let mut objs = Vec::new();
        for _ in 0..nobj {
            encode_value(&mut u, &top, &mut objs)?;
        }
        // Compress the block payload to match the declared codec. With
        // `DeflateGarbage` we deliberately leave it uncompressed so the deflate
        // decoder sees a malformed stream.
        match codec_mode {
            CodecMode::Deflate => {
                // `compress` is infallible for in-memory deflate. If it ever
                // errored we'd just skip this input.
                if Codec::Deflate.compress(&mut objs).is_err() {
                    return Ok(());
                }
            }
            CodecMode::Null | CodecMode::DeflateGarbage | CodecMode::Unrecognized => {}
        }

        // The block framing prefixes. Usually honest, but occasionally we lie
        // about the object count or the byte size. `read_block_next` runs both
        // prefixes through `safe_len` before touching the payload, so a
        // magnitude past `MAX_ALLOCATION_BYTES` only ever reaches that guard.
        // The lies below therefore straddle it deliberately: over the budget to
        // exercise the guard, and over the *encoded data* but inside the budget
        // to exercise what the reader does with a framing it accepted.
        let (count_prefix, size_prefix) = match u.int_in_range(0u8..=9)? {
            // Honest framing (the common case).
            0..=6 => (nobj, objs.len() as i64),
            7 => match u.int_in_range(0u8..=3)? {
                // More objects than the block encodes, drawn across the whole
                // range `safe_len` accepts. Two further guards live in here and
                // both must hold without spinning: `bound_block_object_count`
                // rejects a count the payload could not hold, and a count it
                // accepts has to terminate against the bytes actually present.
                0..=2 => (
                    u.int_in_range(nobj + 1..=MAX_ALLOCATION_BYTES)?,
                    objs.len() as i64,
                ),
                // Past the budget: `safe_len` must reject before the block bound
                // is ever consulted.
                _ => (u.int_in_range(1i64 << 40..=i64::MAX)?, objs.len() as i64),
            },
            // Negative byte size (wraps to an enormous `usize`): `safe_len`
            // must reject it rather than attempt a giant allocation.
            8 => (nobj, -u.int_in_range(1i64..=i64::MAX)?),
            // More bytes than the block carries, but a size `safe_len` accepts,
            // so `fill_buf` runs and takes the short-read path. A magnitude over
            // the budget would stop at the guard and never get there.
            _ => (nobj, objs.len() as i64 + u.int_in_range(1i64..=1i64 << 20)?),
        };
        encode_long(count_prefix, &mut out);
        encode_long(size_prefix, &mut out);
        out.extend_from_slice(&objs);
        // Usually the correct sync, occasionally a wrong one to drive the
        // sync-marker mismatch path.
        if u.int_in_range(0u8..=7)? == 0 {
            out.extend_from_slice(&u.arbitrary::<[u8; 16]>()?);
        } else {
            out.extend_from_slice(&sync);
        }
    }

    // Occasionally truncate the whole file mid-block (short-read path).
    if u.int_in_range(0u8..=7)? == 0 {
        let keep = u.int_in_range(0usize..=out.len())?;
        out.truncate(keep);
    }

    let Ok(reader) = Reader::new(&out[..]) else {
        return Ok(());
    };
    for item in reader {
        let _ = item;
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
