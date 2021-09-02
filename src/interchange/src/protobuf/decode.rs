// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, bail, Context, Result};

use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::{Descriptors, FieldDescriptor, FieldType, MessageDescriptor};
use serde_protobuf::value::Value as ProtoValue;
use serde_value::Value as SerdeValue;

use repr::adt::numeric::Numeric;
use repr::{Datum, DatumList, Row};

use crate::protobuf::proto_message_name;

/// Manages required metadata to read protobuf
#[derive(Debug)]
pub struct Decoder {
    descriptors: Descriptors,
    message_name: String,
    packer: Row,
}

impl Decoder {
    /// Build a decoder from a pre-validated message.
    ///
    /// The message `message_name` must exist in the descriptor set and be
    /// valid.
    pub fn new(descriptors: Descriptors, message_name: &str) -> Decoder {
        // TODO: verify that name exists
        Decoder {
            descriptors,
            message_name: proto_message_name(message_name),
            packer: Row::default(),
        }
    }

    pub fn decode(
        &mut self,
        bytes: &[u8],
        position: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>> {
        let input_stream = protobuf::CodedInputStream::from_bytes(bytes);
        let mut deserializer =
            Deserializer::for_named_message(&self.descriptors, &self.message_name, input_stream)
                .map_err(|e| anyhow!("Creating an input stream to parse protobuf: {}", e))?;
        let deserialized_message =
            SerdeValue::deserialize(&mut deserializer).context("Deserializing into rust object")?;

        let msg_name = &self.message_name;
        let mut packer = &mut self.packer;
        extract_row_into(
            deserialized_message,
            &self.descriptors,
            self.descriptors.message_by_name(&msg_name).ok_or_else(|| {
                anyhow!(
                    "Message should be included in the descriptor set {:?}",
                    msg_name
                )
            })?,
            &mut packer,
        )?;
        if push_metadata {
            packer.push(Datum::from(position));
        }
        Ok(Some(packer.finish_and_reuse()))
    }
}

#[derive(Debug)]
pub struct DecodedDescriptors {
    pub descriptors: Descriptors,
    // Confluent Schema Registry uses the first Message defined in a .proto file
    // if multiple Messages are present. If the user is using protobuf + CSR,
    // we should match this behavior.
    //
    // Link to internal discussion:
    // https://materializeinc.slack.com/archives/C01CFKM1QRF/p1629920709406300
    pub first_message_name: String,
}

fn extract_row_into(
    deserialized_message: SerdeValue,
    descriptors: &Descriptors,
    message_descriptors: &MessageDescriptor,
    packer: &mut Row,
) -> Result<()> {
    let deserialized_message = match deserialized_message {
        SerdeValue::Map(deserialized_message) => deserialized_message,
        _ => bail!("Deserialization failed with an unsupported top level object type"),
    };

    // TODO: This is actually unpacking a row, it should always return json
    for f in message_descriptors.fields().iter() {
        let key = SerdeValue::String(f.name().to_string());
        let value = deserialized_message.get(&key);

        if let Some(value) = value {
            json_from_serde_value(&value, packer, f, descriptors)?;
        } else {
            packer.push(default_datum_from_field(f, descriptors)?);
        }
    }

    Ok(())
}

/// Convert an arbitrary [`SerdeValue`] into a [`Datum`], possibly creating a jsonb value
///
/// Top-level values are converted to equivalent Datums, but in the case of a nested
/// type, all numeric types will be converted to f64s (issue #1476)
fn json_from_serde_value(
    val: &SerdeValue,
    packer: &mut Row,
    f: &FieldDescriptor,
    descriptors: &Descriptors,
) -> Result<()> {
    packer.push(match val {
        SerdeValue::Bool(true) => Datum::True,
        SerdeValue::Bool(false) => Datum::False,
        SerdeValue::I8(i) => Datum::Int32(*i as i32),
        SerdeValue::I16(i) => Datum::Int32(*i as i32),
        SerdeValue::I32(i) => Datum::Int32(*i),
        SerdeValue::I64(i) => Datum::Int64(*i),
        SerdeValue::U8(i) => Datum::Int32(*i as i32),
        SerdeValue::U16(i) => Datum::Int32(*i as i32),
        SerdeValue::U32(u) => Datum::from(Numeric::from(*u)),
        SerdeValue::U64(u) => Datum::from(Numeric::from(*u)),
        SerdeValue::F32(f) => Datum::Float32((*f).into()),
        SerdeValue::F64(f) => Datum::Float64((*f).into()),
        SerdeValue::String(s) => Datum::String(s),
        SerdeValue::Bytes(b) => Datum::Bytes(b),
        SerdeValue::Option(s) => {
            if let Some(s) = s {
                return json_from_serde_value(&s, packer, f, descriptors);
            }

            default_datum_from_field(f, descriptors)?
        }
        SerdeValue::Seq(_) | SerdeValue::Map(_) => {
            return json_nested_from_serde_value(val, packer, f, descriptors);
        }
        SerdeValue::Char(_) | SerdeValue::Unit | SerdeValue::Newtype(_) => bail!(
            "Unsupported type for Datum from serde_value::Value: {:?}",
            val
        ),
    });
    Ok(())
}

fn default_datum_from_field<'a>(
    f: &'a FieldDescriptor,
    descriptors: &'a Descriptors,
) -> Result<Datum<'a>> {
    if let Some(default) = f.default_value() {
        return datum_from_serde_proto(default);
    }

    if f.is_repeated() {
        return Ok(Datum::List(DatumList::empty()));
    }

    match f.field_type(descriptors) {
        FieldType::Bool => Ok(Datum::False),
        FieldType::Int32 | FieldType::SInt32 | FieldType::SFixed32 => Ok(Datum::Int32(0)),
        FieldType::Int64 | FieldType::SInt64 | FieldType::SFixed64 => Ok(Datum::Int64(0)),
        FieldType::Enum(e) => Ok(Datum::String(
            e.value_by_number(0)
                .expect("Error while deserializing protobuf: expected enum to have zero variant")
                .name(),
        )),
        FieldType::Float => Ok(Datum::Float32(OrderedFloat::from(0.0))),
        FieldType::Double => Ok(Datum::Float64(OrderedFloat::from(0.0))),
        FieldType::UInt32 | FieldType::UInt64 | FieldType::Fixed32 | FieldType::Fixed64 => {
            Ok(Datum::from(Numeric::from(0)))
        }
        FieldType::String => Ok(Datum::String("")),
        FieldType::Bytes => Ok(Datum::Bytes(&[])),
        FieldType::Message(_) => Ok(Datum::Null),
        FieldType::Group => bail!("Unions are currently not supported"),
        FieldType::UnresolvedMessage(m) => bail!("Unresolved message {} not supported", m),
        FieldType::UnresolvedEnum(e) => bail!("Unresolved enum {} not supported", e),
    }
}

fn datum_from_serde_proto<'a>(val: &'a ProtoValue) -> Result<Datum<'a>> {
    match val {
        ProtoValue::Bool(true) => Ok(Datum::True),
        ProtoValue::Bool(false) => Ok(Datum::False),
        ProtoValue::I32(i) => Ok(Datum::Int32(*i)),
        ProtoValue::I64(i) => Ok(Datum::Int64(*i)),
        ProtoValue::U32(u) => Ok(Datum::from(Numeric::from(*u))),
        ProtoValue::U64(u) => Ok(Datum::from(Numeric::from(*u))),
        ProtoValue::F32(f) => Ok(Datum::Float32((*f).into())),
        ProtoValue::F64(f) => Ok(Datum::Float64((*f).into())),
        ProtoValue::String(s) => Ok(Datum::String(s)),
        ProtoValue::Bytes(b) => Ok(Datum::Bytes(b)),
        _ => bail!("Unsupported type for Datum from serde_protobuf::Value"),
    }
}

fn json_nested_from_serde_value(
    val: &SerdeValue,
    packer: &mut Row,
    f: &FieldDescriptor,
    descriptors: &Descriptors,
) -> Result<()> {
    packer.push(match val {
        SerdeValue::Bool(true) => Datum::True,
        SerdeValue::Bool(false) => Datum::False,
        SerdeValue::I8(i) => json_number(i)?,
        SerdeValue::I16(i) => json_number(i)?,
        SerdeValue::I32(i) => json_number(i)?,
        SerdeValue::I64(i) => json_number(i)?,
        SerdeValue::U8(i) => json_number(i)?,
        SerdeValue::U16(i) => json_number(i)?,
        SerdeValue::U32(i) => json_number(i)?,
        SerdeValue::U64(i) => json_number(i)?,
        SerdeValue::F32(f) => json_number(f)?,
        SerdeValue::F64(f) => json_number(f)?,
        SerdeValue::String(s) => Datum::String(s),
        SerdeValue::Bytes(_) => {
            bail!("We don't currently support arrays or nested messages with bytes")
        }
        SerdeValue::Seq(s) => {
            return packer.push_list_with(|packer| {
                for value in s {
                    json_nested_from_serde_value(&value, packer, f, descriptors)?;
                }
                Ok(())
            });
        }
        SerdeValue::Option(v) => {
            if let Some(v) = v {
                return json_nested_from_serde_value(&v, packer, f, descriptors);
            }

            default_datum_from_field_nested(f, descriptors)?
        }
        SerdeValue::Map(m) => {
            let mut kvs = m.iter().collect::<Vec<_>>();
            kvs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
            kvs.dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2);
            return packer.push_dict_with(|packer| {
                let nested_message_descriptor = f.field_type(descriptors);
                for (k, v) in kvs {
                    match k {
                        SerdeValue::String(s) => {
                            packer.push(Datum::String(s.as_str()));

                            let nested_message_descriptor = match nested_message_descriptor {
                                FieldType::Message(m) => m,
                                _ => bail!("Nested message is the wrong type"),
                            };

                            json_nested_from_serde_value(
                                &v,
                                packer,
                                nested_message_descriptor
                                    .field_by_name(s)
                                    .expect("Expected this to work"),
                                descriptors,
                            )?;
                        }
                        _ => bail!("Unrecognized value while trying to parse a nested message"),
                    }
                }
                Ok(())
            });
        }
        _ => bail!("Unsupported types from serde_value"),
    });
    Ok(())
}

fn json_number<N: ToPrimitive + std::fmt::Display>(i: &N) -> Result<Datum<'static>> {
    Ok(Datum::Float64(OrderedFloat::from(i.to_f64().ok_or_else(
        || anyhow!("couldn't convert {} into an f64", i),
    )?)))
}

fn default_datum_from_field_nested<'a>(
    f: &'a FieldDescriptor,
    descriptors: &'a Descriptors,
) -> Result<Datum<'a>> {
    if let Some(default) = f.default_value() {
        return datum_from_serde_proto_nested(default);
    }

    if f.is_repeated() {
        return Ok(Datum::List(DatumList::empty()));
    }

    match f.field_type(descriptors) {
        FieldType::Bool => Ok(Datum::False),
        FieldType::Int32
        | FieldType::SInt32
        | FieldType::SFixed32
        | FieldType::Int64
        | FieldType::SInt64
        | FieldType::SFixed64
        | FieldType::UInt32
        | FieldType::UInt64
        | FieldType::Fixed32
        | FieldType::Fixed64
        | FieldType::Float
        | FieldType::Double => Ok(Datum::Float64(OrderedFloat::from(0.0))),
        FieldType::Enum(e) => Ok(Datum::String(
            e.value_by_number(0)
                .expect("Error while deserializing protobuf: expected enum to have zero variant")
                .name(),
        )),
        FieldType::String => Ok(Datum::String("")),
        FieldType::Message(_) => Ok(Datum::Null),
        FieldType::Bytes => bail!("Nested bytes are not supported"),
        FieldType::Group => bail!("Unions are currently not supported"),
        FieldType::UnresolvedMessage(m) => bail!("Unresolved message {} not supported", m),
        FieldType::UnresolvedEnum(e) => bail!("Unresolved enum {} not supported", e),
    }
}

fn datum_from_serde_proto_nested<'a>(val: &'a ProtoValue) -> Result<Datum<'a>> {
    match val {
        ProtoValue::Bool(true) => Ok(Datum::True),
        ProtoValue::Bool(false) => Ok(Datum::False),
        ProtoValue::I32(i) => json_number(i),
        ProtoValue::I64(i) => json_number(i),
        ProtoValue::U32(u) => json_number(u),
        ProtoValue::U64(u) => json_number(u),
        ProtoValue::F32(f) => json_number(f),
        ProtoValue::F64(f) => json_number(f),
        ProtoValue::String(s) => Ok(Datum::String(s)),
        _ => bail!("Unsupported type for Datum from serde_protobuf::Value"),
    }
}
