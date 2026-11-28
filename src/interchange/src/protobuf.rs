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
        check_nesting_depth(bytes, MAX_DESCRIPTOR_DEPTH)?;
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

/// The deepest message nesting accepted in an encoded `FileDescriptorSet`.
///
/// The workspace disables prost's recursion limit, so decoding recurses once
/// per level. Debug builds decode about twice this depth on a 2 MiB stack.
/// protoc caps message declarations at 32 levels but not option values, so
/// only option values nested hundreds deep hit this. Stored sources are
/// planned again on restart, so lowering it can reject descriptors that
/// already decode.
const MAX_DESCRIPTOR_DEPTH: usize = 256;

/// Rejects encoded protobuf whose message nesting may exceed `max_depth`.
///
/// Without a schema, a nested message and a byte string look the same on the
/// wire, so every length-delimited field that parses as fields counts as a
/// level, as does every group. That over-estimates the depth any decoder
/// reaches. Malformed input is left for the decoder to report.
fn check_nesting_depth(bytes: &[u8], max_depth: usize) -> Result<(), anyhow::Error> {
    // End offset and open group count of each enclosing message.
    let mut stack = vec![(bytes.len(), 0usize)];
    let mut depth = 1;
    let mut pos = 0;
    while let Some(&(end, _)) = stack.last() {
        if pos >= end {
            let (_, groups) = stack.pop().expect("stack is non-empty");
            depth -= 1 + groups;
            pos = end;
            continue;
        }
        let top = stack.last_mut().expect("stack is non-empty");
        match read_field(&bytes[..end], pos) {
            Some((next, WireField::Scalar)) => pos = next,
            Some((next, WireField::LengthDelimited { end })) => {
                stack.push((end, 0));
                depth += 1;
                pos = next;
            }
            Some((next, WireField::StartGroup)) => {
                top.1 += 1;
                depth += 1;
                pos = next;
            }
            Some((next, WireField::EndGroup)) if top.1 > 0 => {
                top.1 -= 1;
                depth -= 1;
                pos = next;
            }
            // Not a message, so the rest of this field is opaque bytes.
            Some((_, WireField::EndGroup)) | None => pos = end,
        }
        if depth > max_depth {
            bail!("protobuf descriptors nest deeper than {max_depth} levels");
        }
    }
    Ok(())
}

enum WireField {
    Scalar,
    LengthDelimited { end: usize },
    StartGroup,
    EndGroup,
}

/// Reads the field key at `pos` and skips a scalar value, returning the offset
/// of whatever follows.
fn read_field(buf: &[u8], pos: usize) -> Option<(usize, WireField)> {
    let (key, pos) = read_varint(buf, pos)?;
    if key >> 3 == 0 {
        return None;
    }
    let skip = |len: usize| pos.checked_add(len).filter(|&next| next <= buf.len());
    match key & 7 {
        0 => Some((read_varint(buf, pos)?.1, WireField::Scalar)),
        1 => Some((skip(8)?, WireField::Scalar)),
        2 => {
            let (len, start) = read_varint(buf, pos)?;
            let end = start
                .checked_add(usize::try_from(len).ok()?)
                .filter(|&end| end <= buf.len())?;
            Some((start, WireField::LengthDelimited { end }))
        }
        3 => Some((pos, WireField::StartGroup)),
        4 => Some((pos, WireField::EndGroup)),
        5 => Some((skip(4)?, WireField::Scalar)),
        _ => None,
    }
}

fn read_varint(buf: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = *buf.get(pos)?;
        pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, pos));
        }
    }
    None
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
mod tests {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};

    use super::*;

    /// Wraps `inner` in `levels` messages, each holding the next in field 1.
    fn nest(levels: usize, inner: Vec<u8>) -> Vec<u8> {
        (0..levels).fold(inner, |inner, _| {
            let mut outer = vec![0x0a];
            prost_reflect::prost::encoding::encode_varint(
                u64::try_from(inner.len()).expect("fits"),
                &mut outer,
            );
            outer.extend(inner);
            outer
        })
    }

    #[mz_ore::test]
    fn nesting_depth_limit() {
        assert!(check_nesting_depth(&nest(9, vec![]), 10).is_ok());
        assert!(check_nesting_depth(&nest(10, vec![]), 10).is_err());

        // Groups count too: field 1 start group (0x0b) and end group (0x0c).
        let groups = [vec![0x0b; 10], vec![0x0c; 10]].concat();
        assert!(check_nesting_depth(&groups, 10).is_err());
        assert!(check_nesting_depth(&groups, 11).is_ok());

        // Byte strings that don't parse as fields are opaque.
        assert!(check_nesting_depth(&nest(1, vec![0xff; 4]), 2).is_ok());
    }

    #[mz_ore::test]
    fn deeply_nested_descriptors() {
        let descriptors = |levels: usize| {
            let message =
                (0..levels).fold(DescriptorProto::default(), |inner, _| DescriptorProto {
                    name: Some("M".into()),
                    nested_type: vec![inner],
                    ..Default::default()
                });
            FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("p.proto".into()),
                    message_type: vec![message],
                    ..Default::default()
                }],
            }
            .encode_to_vec()
        };

        // Decodes on the default test thread stack just under the limit.
        let bytes = descriptors(MAX_DESCRIPTOR_DEPTH - 8);
        DecodedDescriptors::from_bytes(&bytes, ".M".into()).unwrap();

        let bytes = descriptors(MAX_DESCRIPTOR_DEPTH);
        let err = DecodedDescriptors::from_bytes(&bytes, ".M".into()).unwrap_err();
        assert!(
            err.to_string().contains("nest deeper"),
            "unexpected error: {err}"
        );
    }
}
