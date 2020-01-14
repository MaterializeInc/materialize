// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Protobuf source connector

use std::fs;

use failure::{bail, format_err, ResultExt};
use num_traits::ToPrimitive;
use ordered_float::OrderedFloat;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::{
    Descriptors, FieldDescriptor, FieldLabel, FieldType, MessageDescriptor,
};
use serde_protobuf::value::Value as ProtoValue;
use serde_value::Value as SerdeValue;

use repr::decimal::Significand;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};

use crate::error::Result;

pub mod test_util;

pub fn read_descriptors_from_file(descriptor_file: &str) -> Result<Descriptors> {
    let abs = fs::canonicalize(descriptor_file)?;
    let mut file = fs::File::open(&abs).map_err(|e| {
        format_err!(
            "Opening descriptor set file {} failed: {}",
            abs.display(),
            e
        )
    })?;
    let proto = protobuf::parse_from_reader(&mut file).context("Parsing descriptor set failed")?;
    Ok(Descriptors::from_proto(&proto))
}

fn validate_proto_field(field: &FieldDescriptor, descriptors: &Descriptors) -> Result<ScalarType> {
    Ok(match field.field_label() {
        FieldLabel::Required => bail!("Required field {} not supported", field.name()),
        FieldLabel::Repeated => {
            validate_proto_field_resolved(field, descriptors)?;
            ScalarType::Jsonb
        }
        FieldLabel::Optional => {
            match field.field_type(descriptors) {
                FieldType::Bool => ScalarType::Bool,
                FieldType::Int32 | FieldType::SInt32 | FieldType::SFixed32 => ScalarType::Int32,
                FieldType::Int64 | FieldType::SInt64 | FieldType::SFixed64 => ScalarType::Int64,
                FieldType::Enum(_) => ScalarType::String,
                FieldType::Float => ScalarType::Float32,
                FieldType::Double => ScalarType::Float64,
                FieldType::UInt32 | FieldType::UInt64 | FieldType::Fixed32 | FieldType::Fixed64 => {
                    ScalarType::Decimal(38, 0)
                } // is that right
                FieldType::String => ScalarType::String,
                FieldType::Bytes => ScalarType::Bytes,
                FieldType::Message(m) => {
                    for f in m.fields().iter() {
                        validate_proto_field_resolved(&f, descriptors)?;
                    }
                    ScalarType::Jsonb
                }
                FieldType::Group => bail!("Unions are currently not supported"),
                FieldType::UnresolvedMessage(m) => bail!("Unresolved message {} not supported", m),
                FieldType::UnresolvedEnum(e) => bail!("Unresolved enum {} not supported", e),
            }
        }
    })
}

fn validate_proto_field_resolved(field: &FieldDescriptor, descriptors: &Descriptors) -> Result<()> {
    match field.field_label() {
        FieldLabel::Required => bail!("Required field {} not supported", field.name()),
        FieldLabel::Repeated | FieldLabel::Optional => match field.field_type(descriptors) {
            FieldType::Bool
            | FieldType::Int32
            | FieldType::SInt32
            | FieldType::SFixed32
            | FieldType::Int64
            | FieldType::SInt64
            | FieldType::SFixed64
            | FieldType::UInt32
            | FieldType::Fixed32
            | FieldType::UInt64
            | FieldType::Fixed64
            | FieldType::Float
            | FieldType::Double
            | FieldType::String
            | FieldType::Enum(_) => (),

            FieldType::Message(m) => {
                for f in m.fields().iter() {
                    validate_proto_field_resolved(&f, descriptors)?;
                }
            }
            FieldType::Bytes => {
                bail!("Arrays or nested messages with bytes objects are not currently supported")
            }
            FieldType::Group => bail!("Unions are currently not supported"),
            FieldType::UnresolvedMessage(a) => bail!("Nested message type {} unresolved", a),
            FieldType::UnresolvedEnum(e) => bail!("Unresolved enum type {}", e),
        },
    }

    Ok(())
}

pub fn validate_proto_schema(message_name: &str, descriptor_file: &str) -> Result<RelationDesc> {
    let descriptors = read_descriptors_from_file(descriptor_file)?;
    validate_proto_schema_with_descriptors(message_name, &descriptors)
}

pub fn validate_proto_schema_with_descriptors(
    message_name: &str,
    descriptors: &Descriptors,
) -> Result<RelationDesc> {
    let message = descriptors
        .message_by_name(message_name)
        .expect("Message not found in file descriptor set");
    let column_types = message
        .fields()
        .iter()
        .map(|f| {
            Ok(ColumnType {
                /// All the fields have to be optional, so mark a field as
                /// nullable if it doesn't have any defaults
                nullable: f.default_value().is_none(),
                scalar_type: validate_proto_field(&f, descriptors)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let column_names = message.fields().iter().map(|f| Some(f.name().to_string()));
    Ok(RelationDesc::new(
        RelationType::new(column_types),
        column_names,
    ))
}

/// Manages required metadata to read protobuf
#[derive(Debug)]
pub struct Decoder {
    descriptors: Descriptors,
    message_name: String,
}

impl Decoder {
    /// Build a decoder from a pre-validated message
    ///
    /// The message `message_name` must exist in the descriptor set and be valid
    pub fn new(descriptors: Descriptors, message_name: &str) -> Decoder {
        // TODO: verify that name exists
        Decoder {
            descriptors,
            message_name: message_name.to_string(),
        }
    }

    pub fn from_descriptor_file(descriptor_file_name: &str, message_name: &str) -> Result<Decoder> {
        let descriptors = read_descriptors_from_file(descriptor_file_name)?;
        // TODO: should we validate message exists in descriptor?
        Ok(Decoder::new(descriptors, message_name))
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>> {
        let input_stream = protobuf::CodedInputStream::from_bytes(bytes);
        let mut deserializer =
            Deserializer::for_named_message(&self.descriptors, &self.message_name, input_stream)
                .with_context(|e| format!("Creating a input stream to parse protobuf: {}", e))?;
        let deserialized_message = SerdeValue::deserialize(&mut deserializer)
            .with_context(|e| format!("Deserializing into rust object: {}", e))?;

        let msg_name = &self.message_name;
        extract_row(
            deserialized_message,
            self.descriptors.message_by_name(&msg_name).ok_or_else(|| {
                format_err!(
                    "Message should be included in the descriptor set {:?}",
                    msg_name
                )
            })?,
        )
    }
}

fn extract_row(
    deserialized_message: SerdeValue,
    message_descriptors: &MessageDescriptor,
) -> Result<Option<Row>> {
    let deserialized_message = match deserialized_message {
        SerdeValue::Map(deserialized_message) => deserialized_message,
        _ => bail!("Deserialization failed with an unsupported top level object type"),
    };

    let mut row = RowPacker::new();

    // TODO: This is actually unpacking a row, it should always return json
    for f in message_descriptors.fields().iter() {
        let key = SerdeValue::String(f.name().to_string());
        let value = deserialized_message.get(&key);

        if let Some(SerdeValue::Option(Some(value))) = value {
            row = json_from_serde_value(&value, row)?;
        } else if let Some(SerdeValue::Seq(_inner)) = value {
            // Note(rkhaitan) This control flow feels extremely weird to me
            // but the library gives different types in very different
            // 'packaging / wrapping' of Options so this seemed like the cleanest
            // thing to do
            row = json_from_serde_value(&value.unwrap(), row)?;
        } else if let Some(default) = f.default_value() {
            row.push(datum_from_serde_proto(default)?);
        } else {
            row.push(Datum::Null);
        }
    }

    Ok(Some(row.finish()))
}

fn datum_from_serde_proto<'a>(val: &'a ProtoValue) -> Result<Datum<'a>> {
    match val {
        ProtoValue::Bool(true) => Ok(Datum::True),
        ProtoValue::Bool(false) => Ok(Datum::False),
        ProtoValue::I32(i) => Ok(Datum::Int32(*i)),
        ProtoValue::I64(i) => Ok(Datum::Int64(*i)),
        ProtoValue::U32(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
        ProtoValue::U64(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
        ProtoValue::F32(f) => Ok(Datum::Float32((*f).into())),
        ProtoValue::F64(f) => Ok(Datum::Float64((*f).into())),
        ProtoValue::String(s) => Ok(Datum::String(s)),
        ProtoValue::Bytes(b) => Ok(Datum::Bytes(b)),
        _ => bail!("Unsupported type for Datum from serde_protobuf::Value"),
    }
}

/// Convert an arbitrary [`SerdeValue`] into a [`Datum`], possibly creating a jsonb value
///
/// Top-level values are converted to equivalent Datums, but in the case of a nested
/// type, all numeric types will be converted to f64s (issue #1476)
fn json_from_serde_value(val: &SerdeValue, mut packer: RowPacker) -> Result<RowPacker> {
    packer.push(match val {
        SerdeValue::Bool(true) => Datum::True,
        SerdeValue::Bool(false) => Datum::False,
        SerdeValue::I8(i) => Datum::Int32(*i as i32),
        SerdeValue::I16(i) => Datum::Int32(*i as i32),
        SerdeValue::I32(i) => Datum::Int32(*i),
        SerdeValue::I64(i) => Datum::Int64(*i),
        SerdeValue::U8(i) => Datum::Int32(*i as i32),
        SerdeValue::U16(i) => Datum::Int32(*i as i32),
        SerdeValue::U32(u) => Datum::Decimal(Significand::new(*u as i128)),
        SerdeValue::U64(u) => Datum::Decimal(Significand::new(*u as i128)),
        SerdeValue::F32(f) => Datum::Float32((*f).into()),
        SerdeValue::F64(f) => Datum::Float64((*f).into()),
        SerdeValue::String(s) => Datum::String(s),
        SerdeValue::Bytes(b) => Datum::Bytes(b),
        SerdeValue::Seq(_) | SerdeValue::Map(_) => {
            return json_nested_from_serde_value(val, packer);
        }
        SerdeValue::Char(_) | SerdeValue::Unit | SerdeValue::Option(_) | SerdeValue::Newtype(_) => {
            bail!(
                "Unsupported type for Datum from serde_value::Value: {:?}",
                val
            )
        }
    });
    Ok(packer)
}

fn json_nested_from_serde_value(val: &SerdeValue, mut packer: RowPacker) -> Result<RowPacker> {
    fn json_number<N: ToPrimitive + std::fmt::Display>(i: &N) -> Result<Datum<'static>> {
        Ok(Datum::Float64(OrderedFloat::from(i.to_f64().ok_or_else(
            || format_err!("couldn't convert {} into an f64", i),
        )?)))
    }

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
            return packer.try_push_list_with(|mut packer| {
                for value in s {
                    packer = json_nested_from_serde_value(&value, packer)?;
                }
                Ok(packer)
            });
        }
        SerdeValue::Map(m) => {
            return packer.try_push_dict_with(|mut packer| {
                for (k, v) in m {
                    match (k, v) {
                        (SerdeValue::String(s), SerdeValue::Option(Some(val))) => {
                            packer.push(Datum::String(s.as_str()));
                            packer = json_nested_from_serde_value(&val, packer)?;
                        }
                        (SerdeValue::String(_), SerdeValue::Option(None)) => (),
                        (SerdeValue::String(s), SerdeValue::Seq(_seq)) => {
                            packer.push(Datum::String(s.as_str()));
                            packer = json_nested_from_serde_value(&v, packer)?;
                        }
                        _ => bail!("Unrecognized value while trying to parse a nested message"),
                    }
                }
                Ok(packer)
            });
        }
        _ => bail!("Unsupported types from serde_value"),
    });
    Ok(packer)
}

#[cfg(test)]
mod tests {
    use super::test_util::gen::fuzz::{
        file_descriptor_proto, Color, TestNestedRecord, TestRecord, TestRepeatedNestedRecord,
        TestRepeatedRecord,
    };
    use failure::{bail, Error};
    use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
    use protobuf::{Message, RepeatedField};
    use serde_protobuf::descriptor::{
        Descriptors, FieldDescriptor, FieldLabel, FieldType, InternalFieldType, MessageDescriptor,
    };

    use ordered_float::OrderedFloat;
    use repr::decimal::Significand;
    use repr::{Datum, RelationDesc, ScalarType};

    fn sanity_check_relation(
        relation: &RelationDesc,
        message: &MessageDescriptor,
        descriptors: &Descriptors,
    ) -> Result<(), Error> {
        for (field_descriptor, (column_name, column_type)) in
            message.fields().iter().zip(relation.iter())
        {
            if let Some(column_name) = column_name {
                assert_eq!(field_descriptor.name(), column_name.as_str());
            } else {
                bail!(
                    "Missing name in relation for field {}",
                    field_descriptor.name()
                );
            }

            match (
                field_descriptor.field_type(descriptors),
                field_descriptor.field_label(),
                &column_type.scalar_type,
            ) {
                (FieldType::Bool, FieldLabel::Optional, ScalarType::Bool)
                | (FieldType::Int32, FieldLabel::Optional, ScalarType::Int32)
                | (FieldType::SInt32, FieldLabel::Optional, ScalarType::Int32)
                | (FieldType::SFixed32, FieldLabel::Optional, ScalarType::Int32)
                | (FieldType::Enum(_), FieldLabel::Optional, ScalarType::String)
                | (FieldType::Int64, FieldLabel::Optional, ScalarType::Int64)
                | (FieldType::SInt64, FieldLabel::Optional, ScalarType::Int64)
                | (FieldType::SFixed64, FieldLabel::Optional, ScalarType::Int64)
                | (FieldType::Float, FieldLabel::Optional, ScalarType::Float32)
                | (FieldType::Double, FieldLabel::Optional, ScalarType::Float64)
                | (FieldType::UInt32, FieldLabel::Optional, ScalarType::Decimal(38, 0))
                | (FieldType::Fixed32, FieldLabel::Optional, ScalarType::Decimal(38,0))
                | (FieldType::UInt64, FieldLabel::Optional, ScalarType::Decimal(38, 0))
                | (FieldType::Fixed64, FieldLabel::Optional, ScalarType::Decimal(38,0))
                | (FieldType::String, FieldLabel::Optional, &ScalarType::String)
                | (FieldType::Bytes, FieldLabel::Optional, ScalarType::Bytes)
                | (FieldType::Message(_), FieldLabel::Optional, ScalarType::Jsonb) => (),

                (ft, FieldLabel::Optional, st) => bail!("Incorrect protobuf optional type {:?} mapping to Materialize type {:?}", ft, st),
                (ft, FieldLabel::Repeated, ScalarType::Jsonb) => {
                    match ft {
                        FieldType::UnresolvedMessage(_) | FieldType::UnresolvedEnum(_) | FieldType::Group => {
                            bail!("Unsupported repeated type {:?}", ft)
                        }
                        _ => (),
                    }
                }
                (ft, label, st) => bail!(
                    "Mismatched field types for proto field {:?} proto type {:?} label {:?} relationtype {:?}",
                    field_descriptor.name(), ft, label, st
                ),
            }
        }

        Ok(())
    }

    #[test]
    fn test_proto_schema_parsing() -> Result<(), failure::Error> {
        let mut descriptors = Descriptors::new();
        let mut m1 = MessageDescriptor::new(".test.message1");
        m1.add_field(FieldDescriptor::new(
            "name",
            1,
            FieldLabel::Optional,
            InternalFieldType::String,
            None,
        ));
        m1.add_field(FieldDescriptor::new(
            "age",
            2,
            FieldLabel::Optional,
            InternalFieldType::UInt32,
            None,
        ));
        descriptors.add_message(m1);

        let mut relation =
            super::validate_proto_schema_with_descriptors(".test.message1", &descriptors)
                .expect("Failed to parse descriptor");

        sanity_check_relation(
            &relation,
            descriptors
                .message_by_name(".test.message1")
                .expect("message should be in the descriptor set"),
            &descriptors,
        )?;

        let mut m2 = MessageDescriptor::new(".test.message2");
        m2.add_field(FieldDescriptor::new(
            "ids",
            1,
            FieldLabel::Repeated,
            InternalFieldType::Int32,
            None,
        ));

        m2.add_field(FieldDescriptor::new(
            "nested",
            1,
            FieldLabel::Repeated,
            InternalFieldType::String,
            None,
        ));
        descriptors.add_message(m2);

        relation = super::validate_proto_schema_with_descriptors(".test.message2", &descriptors)
            .expect("Failed to parse descriptor");

        sanity_check_relation(
            &relation,
            descriptors
                .message_by_name(".test.message2")
                .expect("message should be in the descriptor set"),
            &descriptors,
        )?;

        Ok(())
    }

    fn get_decoder(message_name: &str) -> super::Decoder {
        let mut repeated_field = RepeatedField::<FileDescriptorProto>::new();
        let file_descriptor_proto = file_descriptor_proto().clone();
        repeated_field.push(file_descriptor_proto);

        let mut file_descriptor_set: FileDescriptorSet = FileDescriptorSet::new();
        file_descriptor_set.set_file(repeated_field);

        let descriptors = Descriptors::from_proto(&file_descriptor_set);
        // TODO: should we be validating that message_name exists?
        super::Decoder::new(descriptors, message_name)
    }

    #[test]
    fn test_decode() {
        let mut test_record = TestRecord::new();

        test_record.set_int_field(1);
        test_record.set_string_field("one".to_string());
        test_record.set_int64_field(10000);
        test_record.set_bytes_field(b"foo".to_vec());
        test_record.set_color_field(Color::BLUE);
        test_record.set_uint_field(5);
        test_record.set_uint64_field(55);
        test_record.set_float_field(5.456);
        test_record.set_double_field(99.99);

        let bytes = test_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestRecord");
        let row = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let expected = vec![
            Datum::Int32(1),
            Datum::String("one"),
            Datum::Int64(10000),
            Datum::Bytes(&[102, 111, 111]),
            Datum::String("BLUE"),
            Datum::Decimal(Significand::new(5)),
            Datum::Decimal(Significand::new(55)),
            Datum::Float32(OrderedFloat::from(5.456)),
            Datum::Float64(OrderedFloat::from(99.99)),
        ];

        assert_eq!(datums, expected);
    }

    #[test]
    fn test_decode_with_null() {
        let mut test_record = TestRecord::new();

        test_record.set_int_field(1);
        let bytes = test_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestRecord");
        let row = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let expected = vec![
            Datum::Int32(1),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Null,
        ];

        assert_eq!(datums, expected);
    }

    #[test]
    fn test_repeated() {
        let mut test_record = TestRepeatedRecord::new();
        test_record.set_int_field(vec![1, 2, 3]);
        let bytes = test_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestRepeatedRecord");
        let row = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let d = datums[0];
        if let Datum::List(d) = d {
            let datumlist = d.iter().collect::<Vec<Datum>>();
            assert_eq!(
                datumlist,
                vec![
                    Datum::Float64(OrderedFloat::from(1.0)),
                    Datum::Float64(OrderedFloat::from(2.0)),
                    Datum::Float64(OrderedFloat::from(3.0))
                ]
            );
        } else {
            panic!("Expected the first field to be a list of datums!");
        }
    }

    #[test]
    fn test_nested() {
        let mut test_record = TestRecord::new();

        test_record.set_int_field(1);
        test_record.set_string_field("one".to_string());

        let mut test_nested_record = TestNestedRecord::new();
        test_nested_record.set_test_record(test_record);
        let bytes = test_nested_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestNestedRecord");
        let row = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();
        let d = datums[0];
        if let Datum::Dict(d) = d {
            let datumdict = d.iter().collect::<Vec<(&str, Datum)>>();
            assert_eq!(
                datumdict,
                vec![
                    ("int_field", Datum::Float64(OrderedFloat::from(1.0))),
                    ("string_field", Datum::String("one")),
                ]
            );
        } else {
            panic!("Expected the first field to be a dict of datums!");
        }

        let mut test_repeated_record = TestRepeatedRecord::new();
        let mut repeated_strings = RepeatedField::<String>::new();
        repeated_strings.push("start".to_string());
        repeated_strings.push("two".to_string());
        repeated_strings.push("three".to_string());
        test_repeated_record.set_string_field(repeated_strings);
        test_nested_record.set_test_repeated_record(test_repeated_record);

        let bytes = test_nested_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let row2 = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row2.iter().collect::<Vec<_>>();

        let d = datums[1];
        if let Datum::Dict(d) = d {
            let datumdict = d.iter().collect::<Vec<(&str, Datum)>>();

            for (name, datum) in datumdict.iter() {
                if let (&"string_field", Datum::List(d)) = (name, datum) {
                    let datumlist = d.iter().collect::<Vec<Datum>>();
                    assert_eq!(
                        datumlist,
                        vec![
                            Datum::String("start"),
                            Datum::String("two"),
                            Datum::String("three"),
                        ]
                    );
                }
            }
        } else {
            panic!("Expected the second field to be a dict of datums!");
        }
    }

    #[test]
    fn test_arrays_nested() {
        let mut record = TestRepeatedNestedRecord::new();

        let mut test_record = TestRecord::new();
        let mut repeated_test_records = RepeatedField::<TestRecord>::new();

        test_record.set_int_field(1);
        repeated_test_records.push(test_record.clone());
        repeated_test_records.push(test_record);

        record.set_test_record(repeated_test_records);
        let bytes = record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestRepeatedNestedRecord");
        let row = decoder
            .decode(&bytes)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let d = datums[0];
        if let Datum::List(d) = d {
            let datumlist = d.iter().collect::<Vec<Datum>>();

            for datum in datumlist {
                if let Datum::Dict(d) = datum {
                    let datumdict = d.iter().collect::<Vec<(&str, Datum)>>();
                    assert_eq!(
                        datumdict,
                        vec![("int_field", Datum::Float64(OrderedFloat::from(1.0))),]
                    );
                } else {
                    panic!("Expected the inner elements to be dicts of datums");
                }
            }
        } else {
            panic!("Expected the first field to be a list of datums!");
        }
    }
}
