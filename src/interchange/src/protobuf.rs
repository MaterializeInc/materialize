// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use anyhow::{Context, anyhow, bail};
use mz_ore::str::StrExt;
use mz_repr::{ColumnName, Datum, Row, RowPacker, SqlColumnType, SqlScalarType};
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};

/// Maximum Protobuf message nesting depth accepted when deriving a relation
/// type. A message nested deeper than this is rejected rather than recursed
/// into, so that a pathological (non-cyclic) message chain cannot overflow the
/// stack while deriving the source's `RelationDesc`.
const MAX_MESSAGE_NESTING_DEPTH: usize = 128;

/// Maximum Protobuf wire nesting depth accepted when decoding a
/// `FileDescriptorSet`.
///
/// `DescriptorPool::decode` decodes the descriptor set recursively, descending
/// once per level of wire nesting. `DescriptorProto.nested_type` is
/// self-referential and unknown group fields are skipped recursively, so a
/// deeply nested (but cheap to encode) descriptor set drives that recursion
/// arbitrarily deep. The workspace builds Prost with `no-recursion-limit`, so
/// the recursion is unbounded and such input aborts the process with a stack
/// overflow before any of our own validation runs.
///
/// We therefore reject descriptor sets nested deeper than this on the wire,
/// before decoding them. This is distinct from [`MAX_MESSAGE_NESTING_DEPTH`],
/// which bounds how far message *type references* (`m0 -> m1 -> ...` by name)
/// are followed while deriving the relation type. Those references are a flat
/// list on the wire, so a reference chain does not nest here.
const MAX_DESCRIPTOR_WIRE_NESTING_DEPTH: usize = 128;

/// A decoded description of the schema of a Protobuf message.
#[derive(Debug, PartialEq)]
pub struct DecodedDescriptors {
    message_descriptor: MessageDescriptor,
    columns: Vec<(ColumnName, SqlColumnType)>,
    message_name: String,
}

impl DecodedDescriptors {
    /// Builds a `DecodedDescriptors` from an encoded `FileDescriptorSet` and
    /// the fully qualified name of a message inside that file descriptor set.
    pub fn from_bytes(bytes: &[u8], message_name: String) -> Result<Self, anyhow::Error> {
        // Reject pathologically nested input before `DescriptorPool::decode`
        // recurses into it and overflows the stack. See
        // `MAX_DESCRIPTOR_WIRE_NESTING_DEPTH`.
        check_descriptor_set_nesting_depth(bytes)?;
        let fds = DescriptorPool::decode(bytes).context("decoding file descriptor set")?;
        let message_descriptor = fds.get_message_by_name(&message_name).ok_or_else(|| {
            anyhow!(
                "protobuf message {} not found in file descriptor set",
                message_name.quoted(),
            )
        })?;
        let mut seen_messages = BTreeSet::new();
        seen_messages.insert(message_descriptor.name().to_owned());
        let mut columns = vec![];
        for field in message_descriptor.fields() {
            let name = ColumnName::from(field.name());
            let ty = derive_column_type(&mut seen_messages, &field)?;
            columns.push((name, ty))
        }
        Ok(DecodedDescriptors {
            message_descriptor,
            columns,
            message_name,
        })
    }

    /// Describes the columns in the message.
    ///
    /// In other words, the return value describes the shape of the rows that
    /// will be produced by a [`Decoder`] constructed from this
    /// `DecodedDescriptors`.
    pub fn columns(&self) -> &[(ColumnName, SqlColumnType)] {
        &self.columns
    }
}

/// Decodes a particular Protobuf message from its wire format.
#[derive(Debug)]
pub struct Decoder {
    descriptors: DecodedDescriptors,
    row: Row,
    confluent_wire_format: bool,
}

impl Decoder {
    /// Constructs a decoder for a particular Protobuf message.
    pub fn new(
        descriptors: DecodedDescriptors,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(Decoder {
            descriptors,
            row: Row::default(),
            confluent_wire_format,
        })
    }

    /// Decodes the encoded Protobuf message into a [`Row`].
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<Option<Row>, anyhow::Error> {
        if self.confluent_wire_format {
            // We support Protobuf schema evolution by ignoring the schema that
            // the message was written with and attempting to decode into the
            // schema we know about. As long as the new schema has been evolved
            // according to the Protobuf evolution rules [0], this produces
            // sensible and desirable results.
            //
            // There is the possibility that the message has been written with
            // an incompatible schema, but this is relatively unlikely as the
            // schema registry enforces compatible evolution by default. We
            // don't bother to perform our own compatibility checks because the
            // rules are complex and the Protobuf format is self-describing
            // enough that decoding an Protobuf message with an incompatible
            // schema is handled gracefully (e.g., no accidentally massive
            // allocations).
            //
            // [0]: https://developers.google.com/protocol-buffers/docs/overview
            let (_schema_id, adjusted_bytes) = crate::confluent::extract_protobuf_header(bytes)?;
            bytes = adjusted_bytes;
        }
        let message = DynamicMessage::decode(self.descriptors.message_descriptor.clone(), bytes)?;
        let mut packer = self.row.packer();
        pack_message(&mut packer, &message)?;
        Ok(Some(self.row.clone()))
    }
}

fn derive_column_type(
    seen_messages: &mut BTreeSet<String>,
    field: &FieldDescriptor,
) -> Result<SqlColumnType, anyhow::Error> {
    if field.is_map() {
        bail!("Protobuf map fields are not supported");
    }

    let ty = derive_inner_type(seen_messages, field.kind())?;
    if field.is_list() {
        Ok(SqlColumnType {
            nullable: false,
            scalar_type: SqlScalarType::List {
                element_type: Box::new(ty.scalar_type),
                custom_id: None,
            },
        })
    } else {
        Ok(ty)
    }
}

fn derive_inner_type(
    seen_messages: &mut BTreeSet<String>,
    ty: Kind,
) -> Result<SqlColumnType, anyhow::Error> {
    match ty {
        Kind::Bool => Ok(SqlScalarType::Bool.nullable(false)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(SqlScalarType::Int32.nullable(false)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(SqlScalarType::Int64.nullable(false)),
        Kind::Uint32 | Kind::Fixed32 => Ok(SqlScalarType::UInt32.nullable(false)),
        Kind::Uint64 | Kind::Fixed64 => Ok(SqlScalarType::UInt64.nullable(false)),
        Kind::Float => Ok(SqlScalarType::Float32.nullable(false)),
        Kind::Double => Ok(SqlScalarType::Float64.nullable(false)),
        Kind::String => Ok(SqlScalarType::String.nullable(false)),
        Kind::Bytes => Ok(SqlScalarType::Bytes.nullable(false)),
        Kind::Enum(_) => Ok(SqlScalarType::String.nullable(false)),
        Kind::Message(m) => {
            if seen_messages.contains(m.name()) {
                bail!("Recursive types are not supported: {}", m.name());
            }
            // `seen_messages` holds the ancestor messages currently being
            // resolved, so its length is the current nesting depth. Bounding it
            // prevents a stack overflow from a deeply nested (but non-cyclic)
            // message chain, whose descriptor set is a flat, cheap-to-encode
            // list of messages that reference each other by name.
            if seen_messages.len() >= MAX_MESSAGE_NESTING_DEPTH {
                bail!(
                    "Protobuf message nesting depth exceeds limit of {}",
                    MAX_MESSAGE_NESTING_DEPTH
                );
            }
            seen_messages.insert(m.name().to_owned());
            let mut fields = Vec::with_capacity(m.fields().len());
            for field in m.fields() {
                let column_name = ColumnName::from(field.name());
                let column_type = derive_column_type(seen_messages, &field)?;
                fields.push((column_name, column_type))
            }
            seen_messages.remove(m.name());
            let ty = SqlScalarType::Record {
                fields: fields.into(),
                custom_id: None,
            };
            Ok(ty.nullable(true))
        }
    }
}

/// Rejects a `FileDescriptorSet` whose wire encoding nests message or group
/// fields deeper than [`MAX_DESCRIPTOR_WIRE_NESTING_DEPTH`], which would
/// otherwise overflow the stack inside `DescriptorPool::decode`.
///
/// The scan is iterative (it tracks open regions on an explicit stack), so it
/// cannot itself overflow. It descends into every length-delimited field and
/// every group, which is a superset of what Prost recurses into while decoding,
/// so any input that would drive Prost past the limit is rejected here first. A
/// length-delimited field that is not actually a nested message (a string or a
/// packed field) fails to parse as one within a few bytes and is skipped as an
/// opaque leaf, so realistic descriptor sets are not rejected.
fn check_descriptor_set_nesting_depth(bytes: &[u8]) -> Result<(), anyhow::Error> {
    use bytes::Buf;
    use prost::encoding::{WireType, decode_key, decode_varint};

    enum Frame {
        /// A length-delimited region that ends once `buf.remaining()` has
        /// dropped to this value.
        Len(usize),
        /// A group that ends at an `EndGroup` key carrying this tag.
        Group(u32),
    }

    // Skips the remainder of the innermost length-delimited region, treating it
    // as opaque bytes. Returns `false` when there is no enclosing
    // length-delimited region to recover into, in which case scanning must stop.
    let recover = |frames: &mut Vec<Frame>, buf: &mut &[u8]| -> bool {
        while let Some(frame) = frames.pop() {
            if let Frame::Len(end_remaining) = frame {
                let skip = buf.remaining().saturating_sub(end_remaining);
                buf.advance(skip);
                return true;
            }
        }
        false
    };

    let mut buf: &[u8] = bytes;
    let mut frames: Vec<Frame> = Vec::new();

    loop {
        // Close every length-delimited region that ends at the current position.
        while let Some(Frame::Len(end_remaining)) = frames.last() {
            if buf.remaining() <= *end_remaining {
                frames.pop();
            } else {
                break;
            }
        }
        if !buf.has_remaining() {
            // Any frames still open mean truncated input, which
            // `DescriptorPool::decode` will reject. The depth is bounded either
            // way.
            return Ok(());
        }

        let (tag, wire_type) = match decode_key(&mut buf) {
            Ok(key) => key,
            Err(_) => {
                if recover(&mut frames, &mut buf) {
                    continue;
                }
                return Ok(());
            }
        };

        match wire_type {
            WireType::Varint => {
                if decode_varint(&mut buf).is_err() {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
            }
            WireType::SixtyFourBit | WireType::ThirtyTwoBit => {
                let width = if wire_type == WireType::SixtyFourBit {
                    8
                } else {
                    4
                };
                if buf.remaining() < width {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
                buf.advance(width);
            }
            WireType::LengthDelimited => {
                let len = match decode_varint(&mut buf) {
                    Ok(len) => len,
                    Err(_) => {
                        if recover(&mut frames, &mut buf) {
                            continue;
                        }
                        return Ok(());
                    }
                };
                if len > buf.remaining() as u64 {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
                if frames.len() >= MAX_DESCRIPTOR_WIRE_NESTING_DEPTH {
                    bail!(
                        "Protobuf descriptor set nesting depth exceeds limit of {}",
                        MAX_DESCRIPTOR_WIRE_NESTING_DEPTH
                    );
                }
                let end_remaining = buf.remaining() - len as usize;
                frames.push(Frame::Len(end_remaining));
            }
            WireType::StartGroup => {
                if frames.len() >= MAX_DESCRIPTOR_WIRE_NESTING_DEPTH {
                    bail!(
                        "Protobuf descriptor set nesting depth exceeds limit of {}",
                        MAX_DESCRIPTOR_WIRE_NESTING_DEPTH
                    );
                }
                frames.push(Frame::Group(tag));
            }
            WireType::EndGroup => match frames.last() {
                Some(Frame::Group(open_tag)) if *open_tag == tag => {
                    frames.pop();
                }
                _ => {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
            },
        }
    }
}

fn pack_message(packer: &mut RowPacker, message: &DynamicMessage) -> Result<(), anyhow::Error> {
    for field_desc in message.descriptor().fields() {
        if !message.has_field(&field_desc) {
            if field_desc.cardinality() == Cardinality::Required {
                bail!(
                    "protobuf message missing required field {}",
                    field_desc.name()
                );
            }
            if field_desc.kind().as_message().is_some() && !field_desc.is_list() {
                packer.push(Datum::Null);
                continue;
            }
        }
        let value = message.get_field(&field_desc);
        pack_value(packer, &field_desc, &*value)?;
    }
    Ok(())
}

fn pack_value(
    packer: &mut RowPacker,
    field_desc: &FieldDescriptor,
    value: &Value,
) -> Result<(), anyhow::Error> {
    match value {
        Value::Bool(false) => packer.push(Datum::False),
        Value::Bool(true) => packer.push(Datum::True),
        Value::I32(i) => packer.push(Datum::Int32(*i)),
        Value::I64(i) => packer.push(Datum::Int64(*i)),
        Value::U32(i) => packer.push(Datum::UInt32(*i)),
        Value::U64(i) => packer.push(Datum::UInt64(*i)),
        Value::F32(f) => packer.push(Datum::Float32((*f).into())),
        Value::F64(f) => packer.push(Datum::Float64((*f).into())),
        Value::String(s) => packer.push(Datum::String(s)),
        Value::Bytes(s) => packer.push(Datum::Bytes(s)),
        Value::EnumNumber(i) => {
            let kind = field_desc.kind();
            let enum_desc = kind.as_enum().ok_or_else(|| {
                anyhow!(
                    "internal error: decoding protobuf: field {} missing enum descriptor",
                    field_desc.name()
                )
            })?;
            let value = enum_desc.get_value(*i).ok_or_else(|| {
                anyhow!(
                    "error decoding protobuf: unknown enum value {} while decoding field {}",
                    i,
                    field_desc.name()
                )
            })?;
            packer.push(Datum::String(value.name()));
        }
        Value::Message(m) => packer.push_list_with(|packer| pack_message(packer, m))?,
        Value::List(values) => {
            packer.push_list_with(|packer| {
                for value in values {
                    pack_value(packer, field_desc, value)?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        }
        Value::Map(_) => bail!(
            "internal error: unexpected value while decoding protobuf message: {:?}",
            value
        ),
    }
    Ok(())
}

#[cfg(test)]
mod stack_overflow_verification {
    // Regression test: a deep, non-cyclic protobuf message chain
    // (`m0 -> m1 -> ... -> mN`) must not overflow the stack while deriving the
    // relation type in `DecodedDescriptors::from_bytes` (reached during
    // `CREATE SOURCE ... FORMAT PROTOBUF`). `derive_inner_type` bounds the
    // nesting depth, so this returns a graceful error rather than aborting the
    // process with a stack overflow.
    use prost::Message;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    use super::DecodedDescriptors;

    fn deep_chain_fds(n: usize) -> Vec<u8> {
        let message_type = (0..n)
            .map(|i| {
                let field = if i + 1 < n {
                    FieldDescriptorProto {
                        name: Some("f".into()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(format!(".test.m{}", i + 1)),
                        ..Default::default()
                    }
                } else {
                    FieldDescriptorProto {
                        name: Some("f".into()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    }
                };
                DescriptorProto {
                    name: Some(format!("m{}", i)),
                    field: vec![field],
                    ..Default::default()
                }
            })
            .collect();
        let file = FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            message_type,
            syntax: Some("proto2".into()),
            ..Default::default()
        };
        FileDescriptorSet { file: vec![file] }.encode_to_vec()
    }

    #[mz_ore::test]
    fn deep_chain_does_not_overflow() {
        let bytes = deep_chain_fds(50_000);
        // Must return an error (nesting-depth limit), not abort with a stack
        // overflow. Before the fix this recursed once per message and aborted.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_err(),
            "expected a nesting-depth error for a deep message chain"
        );
    }

    /// The `type_name` reference chain above is flat on the wire, so it stresses
    /// the relation-type derivation. A `DescriptorProto.nested_type` chain is
    /// instead recursive on the wire, so it stresses `DescriptorPool::decode`
    /// itself, which decodes the descriptor set recursively without a depth
    /// limit (`no-recursion-limit`). This must be rejected before that decode
    /// overflows the stack.
    fn deep_nested_type_fds(depth: usize) -> Vec<u8> {
        use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint};

        // Build the wire encoding directly. Materializing the nested
        // `DescriptorProto` values would overflow Prost's *encoder* (and the
        // recursive `Drop`) for the same reason we are guarding the decoder.
        //
        // `lens[k]` is the encoded size of the `DescriptorProto` nested `k`
        // levels deep. Level 0 is an empty message (0 bytes); each further level
        // wraps the previous one in a single `nested_type` field (field 3).
        const NESTED_TYPE_FIELD: u32 = 3;
        let key_len = {
            let mut probe = vec![];
            encode_key(NESTED_TYPE_FIELD, WireType::LengthDelimited, &mut probe);
            probe.len()
        };
        let mut lens = vec![0usize; depth + 1];
        for k in 1..=depth {
            lens[k] = key_len + encoded_len_varint(lens[k - 1] as u64) + lens[k - 1];
        }

        let mut dp = Vec::with_capacity(lens[depth]);
        for k in (1..=depth).rev() {
            encode_key(NESTED_TYPE_FIELD, WireType::LengthDelimited, &mut dp);
            encode_varint(lens[k - 1] as u64, &mut dp);
        }

        // Wrap the chain: FileDescriptorProto.message_type (field 4), then
        // FileDescriptorSet.file (field 1).
        let mut fdp = vec![];
        encode_key(4, WireType::LengthDelimited, &mut fdp);
        encode_varint(dp.len() as u64, &mut fdp);
        fdp.extend_from_slice(&dp);

        let mut fds = vec![];
        encode_key(1, WireType::LengthDelimited, &mut fds);
        encode_varint(fdp.len() as u64, &mut fds);
        fds.extend_from_slice(&fdp);
        fds
    }

    #[mz_ore::test]
    fn deep_nested_type_does_not_overflow() {
        let bytes = deep_nested_type_fds(50_000);
        // Must return an error (wire nesting-depth limit), not abort with a
        // stack overflow. Before the fix `DescriptorPool::decode` recursed once
        // per `nested_type` level and aborted the process.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_err(),
            "expected a nesting-depth error for a deeply nested descriptor"
        );
    }
}
