// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use anyhow::{anyhow, bail, Context};

use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};

use mz_ore::str::StrExt;
use mz_repr::{ColumnName, ColumnType, Datum, Row, RowPacker, ScalarType};

/// A decoded description of the schema of a Protobuf message.
#[derive(Debug, PartialEq)]
pub struct DecodedDescriptors {
    message_descriptor: MessageDescriptor,
    columns: Vec<(ColumnName, ColumnType)>,
    message_name: String,
}

impl DecodedDescriptors {
    /// Builds a `DecodedDescriptors` from an encoded `FileDescriptorSet` and
    /// the fully qualified name of a message inside that file descriptor set.
    pub fn from_bytes(bytes: &[u8], message_name: String) -> Result<Self, anyhow::Error> {
        let fds = DescriptorPool::decode(bytes).context("decoding file descriptor set")?;
        let message_descriptor = fds.get_message_by_name(&message_name).ok_or_else(|| {
            anyhow!(
                "protobuf message {} not found in file descriptor set",
                message_name.quoted(),
            )
        })?;
        let mut seen_messages = HashSet::new();
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
    pub fn columns(&self) -> &[(ColumnName, ColumnType)] {
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
    pub async fn decode(&mut self, mut bytes: &[u8]) -> Result<Option<Row>, anyhow::Error> {
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
    seen_messages: &mut HashSet<String>,
    field: &FieldDescriptor,
) -> Result<ColumnType, anyhow::Error> {
    if field.is_map() {
        bail!("Protobuf map fields are not supported");
    }

    let ty = derive_inner_type(seen_messages, field.kind())?;
    if field.is_list() {
        Ok(ColumnType {
            nullable: false,
            scalar_type: ScalarType::List {
                element_type: Box::new(ty.scalar_type),
                custom_id: None,
            },
        })
    } else {
        Ok(ty)
    }
}

fn derive_inner_type(
    seen_messages: &mut HashSet<String>,
    ty: Kind,
) -> Result<ColumnType, anyhow::Error> {
    match ty {
        Kind::Bool => Ok(ScalarType::Bool.nullable(false)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(ScalarType::Int32.nullable(false)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(ScalarType::Int64.nullable(false)),
        Kind::Uint32 | Kind::Fixed32 | Kind::Uint64 | Kind::Fixed64 => {
            bail!("Protobuf unsigned integer types are not supported")
        }
        Kind::Float => Ok(ScalarType::Float32.nullable(false)),
        Kind::Double => Ok(ScalarType::Float64.nullable(false)),
        Kind::String => Ok(ScalarType::String.nullable(false)),
        Kind::Bytes => Ok(ScalarType::Bytes.nullable(false)),
        Kind::Enum(_) => Ok(ScalarType::String.nullable(false)),
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
            let ty = ScalarType::Record {
                fields,
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
        Value::U32(_) | Value::U64(_) | Value::Map(_) => bail!(
            "internal error: unexpected value while decoding protobuf message: {:?}",
            value
        ),
    }
    Ok(())
}
