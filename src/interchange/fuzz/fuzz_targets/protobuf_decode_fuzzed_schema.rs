// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_interchange::protobuf` over a *fuzzer-generated* descriptor
//! rather than a fixed one. A `FORMAT PROTOBUF` source carries a user-supplied
//! `FileDescriptorSet` (the `USING SCHEMA` blob), so `DecodedDescriptors::from_bytes`
//! and the hand-written `derive_inner_type` validator it runs are themselves an
//! untrusted-input surface, and a fixed descriptor never exercises them with
//! anything but one well-formed schema. The interesting paths are the ones a
//! single descriptor can't reach: recursion detection (a message field
//! referencing its own or a mutually-referencing message must be rejected, not
//! stack-overflow), the full scalar `Kind` mapping, enums, and repeated fields.
//!
//! Critically, we don't feed *random bytes* as the body: prost reads a field
//! tag then skips/short-circuits on most random bytes, so a random body rarely
//! reaches the decode-to-`Row` conversion (scalar `Kind` mapping, enum -> string,
//! repeated -> list, nested message -> record). Instead we keep the schema
//! structured (`MsgDef`/`FieldTy`), build the `FileDescriptorSet` from it, and
//! protobuf-binary-encode a valid body for the top message against that same
//! structure, bounding recursion through cyclic message refs by emitting empty
//! nested messages at the depth limit. Both wire formats are exercised: the
//! Confluent variant gets the 5-byte schema-registry header prepended so its
//! body decodes just as deeply. We keep the error-path coverage a random body
//! gave, though: a quarter of the inputs feed the raw remaining bytes, and
//! others truncate or single-byte-corrupt the valid encoding. Neither
//! validation nor decoding may panic.
//!
//! Beyond the well-formed-schema happy path, we drive the descriptor-validation
//! surface harder. Some schemas grow a `map<string,string>` field (a repeated
//! field of a `map_entry`-tagged message): `derive_column_type` is supposed to
//! *reject* maps (`field.is_map()`), so this exercises that rejection cleanly
//! rather than only ever feeding it map-free input. Field numbers are
//! fuzzer-chosen in a small range, so gaps, out-of-order, and duplicate numbers
//! all occur. Duplicates are an invalid descriptor `DescriptorPool::decode`
//! must reject without panicking. Packable repeated scalars are sometimes
//! emitted in proto3's packed wire form (one length-delimited blob of
//! back-to-back values) instead of tag-per-element, covering the packed decode
//! path. Finally the *root* message the decoder is pointed at is fuzzer-chosen
//! among the generated messages, not pinned to `M0`.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_interchange::protobuf::{DecodedDescriptors, Decoder};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions,
};

/// Name of the synthetic `map_entry` message that backs generated `map` fields.
const MAP_ENTRY: &str = "MapEntry";

/// A generated field type. Kept structured (rather than going straight to a
/// `FieldDescriptorProto`) so the body encoder can walk the exact same shape.
/// A repeated field is one descriptor entry but N encoded values, and a message
/// field needs the referenced message's structure to encode a nested value.
enum FieldTy {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    StringT,
    Bytes,
    Float,
    Double,
    /// Reference to enum `E{0}`.
    Enum(u8),
    /// Reference to message `M{0}`, may equal the containing message (a cycle).
    Message(u8),
    /// `map<string,string>`, a repeated reference to the `map_entry`-tagged
    /// `MapEntry` message. `derive_column_type` rejects this, so the descriptor
    /// it appears in fails validation (exercising the map-rejection path).
    Map,
}

struct FieldDef {
    number: i32,
    ty: FieldTy,
    repeated: bool,
}

struct MsgDef {
    fields: Vec<FieldDef>,
}

/// Generate the message/enum structure. `enum_nvals[j]` is the number of values
/// in enum `E{j}`.
fn gen_schema(u: &mut Unstructured) -> arbitrary::Result<(Vec<MsgDef>, Vec<u8>)> {
    let num_msgs = u.int_in_range(1u8..=4)?;
    let num_enums = u.int_in_range(0u8..=2)?;

    let mut enum_nvals = Vec::with_capacity(num_enums.into());
    for _ in 0..num_enums {
        enum_nvals.push(u.int_in_range(1u8..=3)?);
    }

    let mut msgs = Vec::with_capacity(num_msgs.into());
    for _ in 0..num_msgs {
        let nfields = u.int_in_range(0u8..=6)?;
        let mut fields = Vec::with_capacity(nfields.into());
        for fi in 0..nfields {
            let repeated = bool::arbitrary(u)?;
            let ty = match u.int_in_range(0u8..=17)? {
                0 => FieldTy::Int32,
                1 => FieldTy::Int64,
                2 => FieldTy::Uint32,
                3 => FieldTy::Uint64,
                4 => FieldTy::Sint32,
                5 => FieldTy::Sint64,
                6 => FieldTy::Fixed32,
                7 => FieldTy::Fixed64,
                8 => FieldTy::Sfixed32,
                9 => FieldTy::Sfixed64,
                10 => FieldTy::Bool,
                11 => FieldTy::StringT,
                12 => FieldTy::Bytes,
                13 => FieldTy::Float,
                14 => FieldTy::Double,
                15 if num_enums > 0 => FieldTy::Enum(u.int_in_range(0u8..=num_enums - 1)?),
                16 => FieldTy::Map,
                _ => FieldTy::Message(u.int_in_range(0u8..=num_msgs - 1)?),
            };
            // Field numbers are usually the sequential `fi + 1`, but sometimes a
            // fuzzer-chosen small number, producing gaps, reordering, and
            // duplicate numbers (the last being an invalid descriptor that
            // `DescriptorPool::decode` must reject without panicking).
            let number = if u.int_in_range(0u8..=3)? == 0 {
                i32::from(u.int_in_range(1u8..=8)?)
            } else {
                i32::from(fi) + 1
            };
            fields.push(FieldDef {
                number,
                ty,
                repeated,
            });
        }
        msgs.push(MsgDef { fields });
    }

    Ok((msgs, enum_nvals))
}

fn field_proto(f: &FieldDef) -> FieldDescriptorProto {
    let (ty, type_name) = match &f.ty {
        FieldTy::Int32 => (Type::Int32, None),
        FieldTy::Int64 => (Type::Int64, None),
        FieldTy::Uint32 => (Type::Uint32, None),
        FieldTy::Uint64 => (Type::Uint64, None),
        FieldTy::Sint32 => (Type::Sint32, None),
        FieldTy::Sint64 => (Type::Sint64, None),
        FieldTy::Fixed32 => (Type::Fixed32, None),
        FieldTy::Fixed64 => (Type::Fixed64, None),
        FieldTy::Sfixed32 => (Type::Sfixed32, None),
        FieldTy::Sfixed64 => (Type::Sfixed64, None),
        FieldTy::Bool => (Type::Bool, None),
        FieldTy::StringT => (Type::String, None),
        FieldTy::Bytes => (Type::Bytes, None),
        FieldTy::Float => (Type::Float, None),
        FieldTy::Double => (Type::Double, None),
        FieldTy::Enum(j) => (Type::Enum, Some(format!(".fuzz.E{j}"))),
        FieldTy::Message(r) => (Type::Message, Some(format!(".fuzz.M{r}"))),
        // A map field is a repeated reference to the map_entry message.
        FieldTy::Map => (Type::Message, Some(format!(".fuzz.{MAP_ENTRY}"))),
    };
    // Map fields must be repeated for `is_map()` to fire.
    let label = if f.repeated || matches!(f.ty, FieldTy::Map) {
        Label::Repeated
    } else {
        Label::Optional
    };
    FieldDescriptorProto {
        name: Some(format!("f{}", f.number - 1)),
        number: Some(f.number),
        label: Some(label as i32),
        r#type: Some(ty as i32),
        type_name,
        ..Default::default()
    }
}

/// Build the `FileDescriptorSet` blob (`USING SCHEMA`) from the structure.
fn build_fds(msgs: &[MsgDef], enum_nvals: &[u8]) -> Vec<u8> {
    let enums = enum_nvals
        .iter()
        .enumerate()
        .map(|(j, &nvals)| EnumDescriptorProto {
            name: Some(format!("E{j}")),
            // proto3 requires the first enum value to be zero.
            value: (0..nvals)
                .map(|v| EnumValueDescriptorProto {
                    name: Some(format!("E{j}_{v}")),
                    number: Some(i32::from(v)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();

    let mut messages: Vec<DescriptorProto> = msgs
        .iter()
        .enumerate()
        .map(|(mi, m)| DescriptorProto {
            name: Some(format!("M{mi}")),
            field: m.fields.iter().map(field_proto).collect(),
            ..Default::default()
        })
        .collect();

    // The synthetic `map_entry` message backing `map<string,string>` fields:
    // key = 1, value = 2, with `MessageOptions.map_entry = true` so prost-reflect
    // reports `is_map_entry()` (and the referencing field reports `is_map()`).
    messages.push(DescriptorProto {
        name: Some(MAP_ENTRY.to_owned()),
        field: vec![
            FieldDescriptorProto {
                name: Some("key".to_owned()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                ..Default::default()
            },
            FieldDescriptorProto {
                name: Some("value".to_owned()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                ..Default::default()
            },
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });

    let file = FileDescriptorProto {
        name: Some("fuzz.proto".to_owned()),
        package: Some("fuzz".to_owned()),
        message_type: messages,
        enum_type: enums,
        syntax: Some("proto3".to_owned()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

/// Encode an unsigned protobuf varint.
fn encode_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

/// Encode a field tag: `(field_number << 3) | wire_type`.
fn encode_tag(number: i32, wire_type: u8, out: &mut Vec<u8>) {
    encode_varint(((number as u64) << 3) | u64::from(wire_type), out);
}

fn encode_len_delimited(number: i32, bytes: &[u8], out: &mut Vec<u8>) {
    encode_tag(number, 2, out);
    encode_varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// Encode one occurrence of a field (tag + value).
fn encode_field(
    msgs: &[MsgDef],
    enum_nvals: &[u8],
    f: &FieldDef,
    depth: u32,
    u: &mut Unstructured,
    out: &mut Vec<u8>,
) -> arbitrary::Result<()> {
    let n = f.number;
    match &f.ty {
        // wire type 0: varint.
        FieldTy::Int32 => {
            encode_tag(n, 0, out);
            encode_varint(i64::from(u.arbitrary::<i32>()?) as u64, out);
        }
        FieldTy::Int64 => {
            encode_tag(n, 0, out);
            encode_varint(u.arbitrary::<i64>()? as u64, out);
        }
        FieldTy::Uint32 => {
            encode_tag(n, 0, out);
            encode_varint(u64::from(u.arbitrary::<u32>()?), out);
        }
        FieldTy::Uint64 => {
            encode_tag(n, 0, out);
            encode_varint(u.arbitrary::<u64>()?, out);
        }
        FieldTy::Sint32 => {
            encode_tag(n, 0, out);
            let v = u.arbitrary::<i32>()?;
            encode_varint((((v << 1) ^ (v >> 31)) as u32).into(), out);
        }
        FieldTy::Sint64 => {
            encode_tag(n, 0, out);
            let v = u.arbitrary::<i64>()?;
            encode_varint(((v << 1) ^ (v >> 63)) as u64, out);
        }
        FieldTy::Bool => {
            encode_tag(n, 0, out);
            encode_varint(u.int_in_range(0u64..=1)?, out);
        }
        FieldTy::Enum(j) => {
            encode_tag(n, 0, out);
            // Valid values are 0..nvals. `nvals` itself is one out-of-range
            // index (proto3 open enums accept it, the decoder maps it).
            let nvals = u64::from(enum_nvals[usize::from(*j)]);
            encode_varint(u.int_in_range(0u64..=nvals)?, out);
        }
        // wire type 1: 64-bit.
        FieldTy::Fixed64 => {
            encode_tag(n, 1, out);
            out.extend_from_slice(&u.arbitrary::<u64>()?.to_le_bytes());
        }
        FieldTy::Sfixed64 => {
            encode_tag(n, 1, out);
            out.extend_from_slice(&u.arbitrary::<i64>()?.to_le_bytes());
        }
        FieldTy::Double => {
            encode_tag(n, 1, out);
            out.extend_from_slice(&u.arbitrary::<f64>()?.to_le_bytes());
        }
        // wire type 5: 32-bit.
        FieldTy::Fixed32 => {
            encode_tag(n, 5, out);
            out.extend_from_slice(&u.arbitrary::<u32>()?.to_le_bytes());
        }
        FieldTy::Sfixed32 => {
            encode_tag(n, 5, out);
            out.extend_from_slice(&u.arbitrary::<i32>()?.to_le_bytes());
        }
        FieldTy::Float => {
            encode_tag(n, 5, out);
            out.extend_from_slice(&u.arbitrary::<f32>()?.to_le_bytes());
        }
        // wire type 2: length-delimited.
        FieldTy::StringT => {
            let len = u.int_in_range(0usize..=8)?;
            let mut s = Vec::with_capacity(len);
            for _ in 0..len {
                s.push(u.int_in_range(0x20u8..=0x7e)?);
            }
            encode_len_delimited(n, &s, out);
        }
        FieldTy::Bytes => {
            let len = u.int_in_range(0usize..=8)?;
            let mut b = Vec::with_capacity(len);
            for _ in 0..len {
                b.push(u.arbitrary::<u8>()?);
            }
            encode_len_delimited(n, &b, out);
        }
        FieldTy::Message(r) => {
            // Bound recursion through cyclic refs: at the depth limit emit an
            // empty (all-defaults) nested message.
            let mut nested = Vec::new();
            if depth > 0 {
                encode_message(msgs, enum_nvals, usize::from(*r), depth - 1, u, &mut nested)?;
            }
            encode_len_delimited(n, &nested, out);
        }
        FieldTy::Map => {
            // One map entry: a length-delimited message with key=1, value=2
            // (both string). The descriptor with a map field never decodes (it's
            // rejected at validation), but a valid body keeps generation honest.
            let mut entry = Vec::new();
            let mut k = Vec::new();
            for _ in 0..u.int_in_range(0usize..=4)? {
                k.push(u.int_in_range(0x20u8..=0x7e)?);
            }
            encode_len_delimited(1, &k, &mut entry);
            let mut v = Vec::new();
            for _ in 0..u.int_in_range(0usize..=4)? {
                v.push(u.int_in_range(0x20u8..=0x7e)?);
            }
            encode_len_delimited(2, &v, &mut entry);
            encode_len_delimited(n, &entry, out);
        }
    }
    Ok(())
}

/// Whether a scalar field type can be encoded in proto3's packed wire form
/// (varint / fixed32 / fixed64 scalars). Strings, bytes, messages, and maps
/// cannot be packed.
fn is_packable(ty: &FieldTy) -> bool {
    matches!(
        ty,
        FieldTy::Int32
            | FieldTy::Int64
            | FieldTy::Uint32
            | FieldTy::Uint64
            | FieldTy::Sint32
            | FieldTy::Sint64
            | FieldTy::Bool
            | FieldTy::Enum(_)
            | FieldTy::Fixed32
            | FieldTy::Fixed64
            | FieldTy::Sfixed32
            | FieldTy::Sfixed64
            | FieldTy::Float
            | FieldTy::Double
    )
}

/// Encode just the value (no tag) of a packable scalar field, the element form
/// inside a packed repeated field's length-delimited blob.
fn encode_scalar_value(
    enum_nvals: &[u8],
    f: &FieldDef,
    u: &mut Unstructured,
    out: &mut Vec<u8>,
) -> arbitrary::Result<()> {
    match &f.ty {
        FieldTy::Int32 => encode_varint(i64::from(u.arbitrary::<i32>()?) as u64, out),
        FieldTy::Int64 => encode_varint(u.arbitrary::<i64>()? as u64, out),
        FieldTy::Uint32 => encode_varint(u64::from(u.arbitrary::<u32>()?), out),
        FieldTy::Uint64 => encode_varint(u.arbitrary::<u64>()?, out),
        FieldTy::Sint32 => {
            let v = u.arbitrary::<i32>()?;
            encode_varint((((v << 1) ^ (v >> 31)) as u32).into(), out);
        }
        FieldTy::Sint64 => {
            let v = u.arbitrary::<i64>()?;
            encode_varint(((v << 1) ^ (v >> 63)) as u64, out);
        }
        FieldTy::Bool => encode_varint(u.int_in_range(0u64..=1)?, out),
        FieldTy::Enum(j) => {
            let nvals = u64::from(enum_nvals[usize::from(*j)]);
            encode_varint(u.int_in_range(0u64..=nvals)?, out);
        }
        FieldTy::Fixed64 => out.extend_from_slice(&u.arbitrary::<u64>()?.to_le_bytes()),
        FieldTy::Sfixed64 => out.extend_from_slice(&u.arbitrary::<i64>()?.to_le_bytes()),
        FieldTy::Double => out.extend_from_slice(&u.arbitrary::<f64>()?.to_le_bytes()),
        FieldTy::Fixed32 => out.extend_from_slice(&u.arbitrary::<u32>()?.to_le_bytes()),
        FieldTy::Sfixed32 => out.extend_from_slice(&u.arbitrary::<i32>()?.to_le_bytes()),
        FieldTy::Float => out.extend_from_slice(&u.arbitrary::<f32>()?.to_le_bytes()),
        // Unreachable: callers gate on `is_packable`.
        _ => {}
    }
    Ok(())
}

/// Encode a valid body for message `M{idx}`.
fn encode_message(
    msgs: &[MsgDef],
    enum_nvals: &[u8],
    idx: usize,
    depth: u32,
    u: &mut Unstructured,
    out: &mut Vec<u8>,
) -> arbitrary::Result<()> {
    let Some(msg) = msgs.get(idx) else {
        return Ok(());
    };
    for f in &msg.fields {
        // Repeated fields get 0..=3 occurrences, singular fields 0..=1
        // (proto3 fields are optional). Message fields at the depth limit are
        // omitted entirely to break cycles.
        let max = if matches!(f.ty, FieldTy::Message(_)) && depth == 0 {
            0
        } else if f.repeated {
            3
        } else {
            1
        };
        let count = u.int_in_range(0u8..=max)?;
        // A repeated packable scalar is sometimes emitted in proto3's packed wire
        // form (one length-delimited blob of back-to-back values) instead of a
        // tag per element, to exercise the packed decode path. (`prost` accepts
        // either encoding for a packed field.)
        if f.repeated && count > 0 && is_packable(&f.ty) && u.int_in_range(0u8..=1)? == 0 {
            let mut packed = Vec::new();
            for _ in 0..count {
                encode_scalar_value(enum_nvals, f, u, &mut packed)?;
            }
            encode_len_delimited(f.number, &packed, out);
        } else {
            for _ in 0..count {
                encode_field(msgs, enum_nvals, f, depth, u, out)?;
            }
        }
    }
    Ok(())
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let (msgs, enum_nvals) = gen_schema(&mut u)?;
    let fds = build_fds(&msgs, &enum_nvals);

    // The decoder's root message is fuzzer-chosen among the generated messages,
    // not pinned to M0, so the various field shapes get exercised as the *top*
    // message (where columns are derived directly) and not only when nested.
    let root = usize::from(u.int_in_range(0u8..=u8::try_from(msgs.len() - 1).unwrap())?);
    let root_name = format!("fuzz.M{root}");

    // Body for the root message: usually a valid encoding (so prost decodes
    // through to the Row conversion), but a quarter of the time the raw remaining
    // bytes, and otherwise a valid encoding occasionally truncated or single-byte
    // corrupted. The non-valid forms keep the decoder's error paths covered: bad
    // tag/wire-type, length overrun, unexpected EOF.
    let body = if u.int_in_range(0u8..=3)? == 0 {
        u.take_rest().to_vec()
    } else {
        let mut b = Vec::new();
        encode_message(&msgs, &enum_nvals, root, 3, &mut u, &mut b)?;
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

    for confluent_wire_format in [false, true] {
        let Ok(descriptors) = DecodedDescriptors::from_bytes(&fds, root_name.clone()) else {
            return Ok(());
        };
        let Ok(mut decoder) = Decoder::new(descriptors, confluent_wire_format) else {
            return Ok(());
        };
        // The Confluent variant strips a 5-byte schema-registry header (magic
        // byte 0 + 4-byte schema id) before the body. Prepend one so its body
        // decodes just as deeply as the raw variant.
        let payload = if confluent_wire_format {
            let mut p = vec![0u8; 5];
            p.extend_from_slice(&body);
            p
        } else {
            body.clone()
        };
        let _ = decoder.decode(&payload);
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
