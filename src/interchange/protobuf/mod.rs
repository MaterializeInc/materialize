// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Protobuf source connector

use std::fs;

use failure::{bail, Error};
use ordered_float::OrderedFloat;
use protoc::Protoc;
use serde::de::Deserialize;
use serde_protobuf::de::Deserializer;
use serde_protobuf::descriptor::{
    Descriptors, FieldDescriptor, FieldLabel, FieldType, MessageDescriptor,
};
use serde_protobuf::value;
use serde_value::Value;

use repr::decimal::Significand;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};

pub mod test;

fn read_descriptors_from_file(descriptor_file: &str) -> Descriptors {
    let mut file = fs::File::open(descriptor_file).expect("Opening descriptor set file failed");
    let proto = protobuf::parse_from_reader(&mut file).expect("Parsing descriptor set failed");
    Descriptors::from_proto(&proto)
}

// Takes a path to a .proto spec and attempts to generate a binary file
// containing a set of descriptors for the message (and any nested messages)
// defined in the spec. Only useful for test purposes and currently unused
#[allow(dead_code)]
fn generate_descriptors(proto_path: &str, out: &str) -> Descriptors {
    let protoc = Protoc::from_env_path();
    let descriptor_set_out_args = protoc::DescriptorSetOutArgs {
        out,
        includes: &[],
        input: &[proto_path],
        include_imports: false,
    };

    protoc
        .write_descriptor_set(descriptor_set_out_args)
        .expect("protoc write descriptor set failed");
    read_descriptors_from_file(out)
}

fn validate_proto_field(
    field: &FieldDescriptor,
    descriptors: &Descriptors,
) -> Result<ScalarType, Error> {
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

fn validate_proto_field_resolved(
    field: &FieldDescriptor,
    descriptors: &Descriptors,
) -> Result<(), Error> {
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

pub fn validate_proto_schema(
    message_name: &str,
    descriptor_file: &str,
) -> Result<RelationDesc, Error> {
    let descriptors = read_descriptors_from_file(descriptor_file);
    validate_proto_schema_with_descriptors(message_name, &descriptors)
}

pub fn validate_proto_schema_with_descriptors(
    message_name: &str,
    descriptors: &Descriptors,
) -> Result<RelationDesc, Error> {
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
        .collect::<Result<Vec<_>, Error>>()?;

    let column_names = message.fields().iter().map(|f| Some(f.name().to_string()));
    Ok(RelationDesc::new(
        RelationType::new(column_types),
        column_names,
    ))
}

// Manages required metadata to read protobuf
#[derive(Debug)]
pub struct Decoder {
    descriptors: Descriptors,
    message_name: String,
    packer: RowPacker,
}

impl Decoder {
    pub fn new(descriptors: Descriptors, message_name: &str) -> Decoder {
        // It's assumed that we've already validated that the message exists in
        // the descriptor set and is valid

        Decoder {
            descriptors,
            message_name: message_name.to_string(),
            packer: RowPacker::new(),
        }
    }

    pub fn from_descriptor_file(descriptor_file_name: &str, message_name: &str) -> Decoder {
        let descriptors = read_descriptors_from_file(descriptor_file_name);

        Decoder::new(descriptors, message_name)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<Option<Row>, failure::Error> {
        let input_stream = protobuf::CodedInputStream::from_bytes(bytes);
        let mut deserializer =
            Deserializer::for_named_message(&self.descriptors, &self.message_name, input_stream)
                .expect("Creating a input stream to parse protobuf");
        let deserialized_message =
            Value::deserialize(&mut deserializer).expect("Deserializing into rust object");

        fn value_to_datum(v: &Value) -> Result<Datum<'_>, failure::Error> {
            match v {
                Value::Bool(true) => Ok(Datum::True),
                Value::Bool(false) => Ok(Datum::False),
                Value::I32(i) => Ok(Datum::Int32(*i)),
                Value::I64(i) => Ok(Datum::Int64(*i)),
                Value::U32(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
                Value::U64(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
                Value::F32(f) => Ok(Datum::Float32((*f).into())),
                Value::F64(f) => Ok(Datum::Float64((*f).into())),
                Value::String(s) => Ok(Datum::String(s)),
                Value::Bytes(b) => Ok(Datum::Bytes(b)),
                _ => bail!("Unsupported types from serde_value"),
            }
        };

        fn inner_value_to_datum(v: &Value) -> Result<Datum<'_>, failure::Error> {
            match v {
                Value::Bool(true) => Ok(Datum::True),
                Value::Bool(false) => Ok(Datum::False),
                Value::I32(i) => Ok(Datum::Float64(OrderedFloat::from(*i as f64))),
                Value::I64(i) => Ok(Datum::Float64(OrderedFloat::from(*i as f64))),
                Value::U32(i) => Ok(Datum::Float64(OrderedFloat::from(*i as f64))),
                Value::U64(i) => Ok(Datum::Float64(OrderedFloat::from(*i as f64))),
                Value::F32(f) => Ok(Datum::Float64((*f as f64).into())),
                Value::F64(f) => Ok(Datum::Float64((*f).into())),
                Value::String(s) => Ok(Datum::String(s)),
                Value::Bytes(_) => {
                    bail!("We don't currently support arrays or nested messages with bytes")
                }
                _ => bail!("Unsupported types from serde_value"),
            }
        };

        fn default_to_datum(v: &value::Value) -> Result<Datum<'_>, failure::Error> {
            match v {
                value::Value::Bool(true) => Ok(Datum::True),
                value::Value::Bool(false) => Ok(Datum::False),
                value::Value::I32(i) => Ok(Datum::Int32(*i)),
                value::Value::I64(i) => Ok(Datum::Int64(*i)),
                value::Value::U32(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
                value::Value::U64(u) => Ok(Datum::Decimal(Significand::new(*u as i128))),
                value::Value::F32(f) => Ok(Datum::Float32((*f).into())),
                value::Value::F64(f) => Ok(Datum::Float64((*f).into())),
                value::Value::String(s) => Ok(Datum::String(s)),
                value::Value::Bytes(b) => Ok(Datum::Bytes(b)),
                _ => bail!("Unsupported types from serde_protobuf::value"),
            }
        };

        fn extract_row(
            deserialized_message: Value,
            packer: &mut RowPacker,
            message_descriptors: &MessageDescriptor,
        ) -> Result<Option<Row>, failure::Error> {
            let deserialized_message = match deserialized_message {
                Value::Map(deserialized_message) => deserialized_message,
                _ => bail!("Deserialization failed with an unsupported top level object type"),
            };

            let mut arena = packer.arena();

            let mut datums: Vec<Datum> = vec![];

            for f in message_descriptors.fields().iter() {
                let key = Value::String(f.name().to_string());
                let value = deserialized_message.get(&key);

                if let Some(Value::Option(Some(value))) = value {
                    datums.push(value_to_datum(&value)?);
                } else if let Some(Value::Seq(value)) = value {
                    let mut datum_list = vec![];

                    for v in value {
                        datum_list.push(inner_value_to_datum(&v)?);
                    }

                    datums.push(Datum::List(arena.push_list(datum_list)));
                } else if let Some(default) = f.default_value() {
                    datums.push(default_to_datum(default)?);
                } else {
                    datums.push(Datum::Null);
                }
            }

            Ok(Some(arena.pack(datums)))
        };

        extract_row(
            deserialized_message,
            &mut self.packer,
            &self
                .descriptors
                .message_by_name(&self.message_name)
                .expect("Message should be included in the descriptor set"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::test::test_proto_schemas::{
        file_descriptor_proto, Color, TestRecord, TestRepeatedRecord,
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
            InternalFieldType::Bytes,
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
            assert!(false);
        }
    }
}
