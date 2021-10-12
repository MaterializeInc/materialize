// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::collections::HashSet;

use anyhow::{anyhow, bail, Context, Result};

use ordered_float::OrderedFloat;
use protobuf::descriptor::FileDescriptorSet;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::{Descriptors, FieldDescriptor, FieldType, MessageDescriptor};
use serde_protobuf::value::Value as ProtoValue;
use serde_value::Value as SerdeValue;

use ore::str::StrExt;
use repr::adt::numeric::Numeric;
use repr::{ColumnType, Datum, DatumList, RelationDesc, RelationType, Row, ScalarType};

use crate::protobuf::proto_message_name;

/// Manages required metadata to read protobuf
#[derive(Debug)]
pub struct RawDescriptors<'a> {
    bytes: &'a [u8],
    message_name: String,
}

/// Note: Descriptors are not `Clone` so we're use generics here to be able to
/// support the two following flows:
///   1. RawDescriptor.decode() -> DecodedDescriptor where the Descriptor must
///      be construced inline so we cannot hold a reference.
///   2. Tests, where we would like to be able to construct from a pre-existing
///      `Descriptor` a `DecodedDescriptor`.  We want to be able to keep the
///      original `Descriptor` around so we can do some sanity checking with
///      the result of `DecodedDescriptor.verify()`.  To do this, we want to
///      pass a borrowed version of `Descriptor`.
#[derive(Debug)]
pub struct DecodedDescriptors<D: Borrow<Descriptors>> {
    descriptors: D,
    message_name: String,
}

impl<'a> RawDescriptors<'a> {
    pub fn new(bytes: &'a [u8], message_name: String) -> Self {
        Self {
            bytes,
            message_name,
        }
    }

    pub fn decode(self) -> Result<DecodedDescriptors<Descriptors>> {
        Ok(DecodedDescriptors {
            descriptors: Descriptors::from_proto(
                &protobuf::Message::parse_from_bytes(&self.bytes)
                    .context("parsing encoded protobuf descriptors failed")?,
            ),
            message_name: self.message_name,
        })
    }
}

impl DecodedDescriptors<Descriptors> {
    pub fn from_fds(fds: &FileDescriptorSet, message_name: String) -> Self {
        Self {
            descriptors: Descriptors::from_proto(fds),
            message_name,
        }
    }
}

impl<D: Borrow<Descriptors>> DecodedDescriptors<D> {
    pub fn from_descriptors(desc: D, message_name: String) -> Self {
        Self {
            descriptors: desc,
            message_name: message_name,
        }
    }

    pub fn descriptors(&self) -> &Descriptors {
        self.descriptors.borrow()
    }

    pub fn validate(&self) -> Result<RelationDesc> {
        let proto_name = proto_message_name(&self.message_name);
        let message = self
            .descriptors()
            .message_by_name(&proto_name)
            .ok_or_else(|| {
                // TODO(benesch): the error message here used to include the names of
                // all messages in the descriptor set, but that one feature required
                // maintaining a fork of serde_protobuf. I sent the patch upstream [0],
                // and we can add the error message improvement back if that patch is
                // accepted.
                // [0]: https://github.com/dflemstr/serde-protobuf/pull/9
                anyhow!(
                    "Message {} not found in file descriptor set",
                    proto_name.quoted()
                )
            })?;
        let mut seen_messages = HashSet::new();
        seen_messages.insert(message.name());
        let column_types = message
            .fields()
            .iter()
            .map(|f| {
                Ok(ColumnType {
                    /// All the fields have to be optional, so mark a field as
                    /// nullable if it doesn't have any defaults
                    nullable: f.default_value().is_none(),
                    scalar_type: super::derive_scalar_type_from_proto_field(
                        &mut seen_messages,
                        &f,
                        self.descriptors.borrow(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let column_names = message.fields().iter().map(|f| Some(f.name().to_string()));
        Ok(RelationDesc::new(
            RelationType::new(column_types),
            column_names,
        ))
    }
}

/// Manages required metadata to read protobuf
#[derive(Debug)]
pub struct Decoder {
    descriptors: DecodedDescriptors<Descriptors>,
    packer: Row,
}

impl Decoder {
    /// Build a decoder from a pre-validated message.
    pub fn new(descriptors: DecodedDescriptors<Descriptors>) -> Self {
        // TODO: verify that name exists
        Decoder {
            descriptors: descriptors,
            packer: Row::default(),
        }
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>> {
        let input_stream = protobuf::CodedInputStream::from_bytes(bytes);
        let mut deserializer = Deserializer::for_named_message(
            &self.descriptors.descriptors,
            &self.descriptors.message_name,
            input_stream,
        )
        .map_err(|e| anyhow!("Creating an input stream to parse protobuf: {}", e))?;
        let deserialized_message =
            SerdeValue::deserialize(&mut deserializer).context("Deserializing into rust object")?;
        let relation_type = self.descriptors.validate()?;

        let msg_name = &self.descriptors.message_name;
        let mut packer = &mut self.packer;
        extract_row_into(
            deserialized_message,
            &self.descriptors.descriptors,
            self.descriptors
                .descriptors
                .message_by_name(msg_name)
                .ok_or_else(|| {
                    anyhow!(
                        "Message should be included in the descriptor set {:?}",
                        msg_name
                    )
                })?,
            &relation_type.typ().column_types,
            &mut packer,
        )?;
        Ok(Some(packer.finish_and_reuse()))
    }
}

fn extract_row_into(
    deserialized_message: SerdeValue,
    descriptors: &Descriptors,
    message_descriptors: &MessageDescriptor,
    column_types: &[ColumnType],
    packer: &mut Row,
) -> Result<()> {
    let deserialized_message = match deserialized_message {
        SerdeValue::Map(deserialized_message) => deserialized_message,
        _ => bail!("Deserialization failed with an unsupported top level object type"),
    };

    // TODO: This is actually unpacking a row, it should always return json
    for (f, column_type) in message_descriptors.fields().iter().zip(column_types) {
        let key = SerdeValue::String(f.name().to_string());
        let value = deserialized_message.get(&key);
        if let Some(value) = value {
            json_from_serde_value(&value, packer, f, descriptors, column_type)?;
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
    column_type: &ColumnType,
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
                return json_from_serde_value(&s, packer, f, descriptors, column_type);
            }

            default_datum_from_field(f, descriptors)?
        }
        SerdeValue::Seq(_) | SerdeValue::Map(_) => {
            return nested_datum_from_serde_value(val, packer, f, descriptors, column_type);
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

fn nested_datum_from_serde_value(
    val: &SerdeValue,
    packer: &mut Row,
    f: &FieldDescriptor,
    descriptors: &Descriptors,
    column_type: &ColumnType,
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
        SerdeValue::Bytes(_) => {
            bail!("We don't currently support arrays or nested messages with bytes")
        }
        SerdeValue::Seq(s) => {
            let inner_column_type = if let ColumnType {
                scalar_type: ScalarType::List { element_type, .. },
                ..
            } = column_type
            {
                ColumnType {
                    scalar_type: *element_type.clone(),
                    nullable: true,
                }
            } else {
                bail!("sequence must be of type list, found {:?}", column_type);
            };
            return packer.push_list_with(|packer| {
                for value in s {
                    nested_datum_from_serde_value(
                        &value,
                        packer,
                        f,
                        descriptors,
                        &inner_column_type,
                    )?;
                }
                Ok(())
            });
        }
        SerdeValue::Option(v) => {
            if let Some(v) = v {
                return nested_datum_from_serde_value(&v, packer, f, descriptors, column_type);
            }

            default_datum_from_field_nested(f, descriptors)?
        }
        SerdeValue::Map(m) => {
            let nested_message_descriptor = f.field_type(descriptors);
            let pack_map = |packer: &mut Row,
                            k: &SerdeValue,
                            v: &SerdeValue,
                            typ: &ColumnType,
                            push_key: bool| {
                match k {
                    SerdeValue::String(s) => {
                        if push_key {
                            packer.push(Datum::String(s.as_str()));
                        }

                        let nested_message_descriptor = match nested_message_descriptor {
                            FieldType::Message(m) => m,
                            _ => bail!("Nested message is the wrong type"),
                        };

                        nested_datum_from_serde_value(
                            &v,
                            packer,
                            nested_message_descriptor
                                .field_by_name(s)
                                .expect("nested message to exist"),
                            descriptors,
                            typ,
                        )?;
                    }
                    _ => bail!("Unrecognized value while trying to parse a nested message"),
                }
                Ok(())
            };

            match &column_type.scalar_type {
                ScalarType::Map { value_type, .. } => {
                    let mut kvs = m.iter().collect::<Vec<_>>();
                    kvs.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));

                    return packer.push_dict_with(|packer| {
                        let typ = ColumnType {
                            scalar_type: *value_type.clone(),
                            nullable: true,
                        };
                        for (k, v) in kvs {
                            pack_map(packer, k, v, &typ, true)?;
                        }
                        Ok(())
                    });
                }
                ScalarType::Record { fields, .. } => {
                    return packer.push_list_with(|packer| {
                        for (n, typ) in fields {
                            let (k, v) = m
                                .get_key_value(&SerdeValue::String(n.to_string()))
                                .expect("key value pair to exist");
                            pack_map(packer, k, v, &typ, false)?;
                        }
                        Ok(())
                    });
                }
                _ => bail!("Unsupported scalar type for map"),
            }
        }
        _ => bail!("Unsupported types from serde_value"),
    });
    Ok(())
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

fn datum_from_serde_proto_nested<'a>(val: &'a ProtoValue) -> Result<Datum<'a>> {
    if let ProtoValue::Bytes(_) = val {
        bail!("Nested bytes are not supported");
    }
    datum_from_serde_proto(val)
}
