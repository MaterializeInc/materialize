// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf source connector

pub mod decode;

use std::collections::HashSet;

use anyhow::{anyhow, bail, Context, Result};
use serde_protobuf::descriptor::{Descriptors, FieldDescriptor, FieldLabel, FieldType};

use ore::str::StrExt;
use repr::{ColumnName, ColumnType, RelationDesc, RelationType, ScalarType};

fn proto_message_name(message_name: &str) -> String {
    // Prepend a . (following the serde-protobuf naming scheme to list root paths
    // for packaged messages) if the message is part of a package and the user hasn't
    // already specified a root path
    if message_name.is_empty() || !message_name.contains('.') || message_name.starts_with('.') {
        message_name.to_string()
    } else {
        format!(".{}", message_name)
    }
}

fn derive_scalar_type_from_proto_field<'a>(
    seen_messages: &mut HashSet<&'a str>,
    field: &'a FieldDescriptor,
    descriptors: &'a Descriptors,
) -> Result<ScalarType> {
    let field_type = field.field_type(descriptors);
    match field.field_label() {
        FieldLabel::Required => bail!("Required field {} not supported", field.name()),
        FieldLabel::Repeated => {
            if let FieldType::Bytes = field_type {
                bail!("Arrays or nested messages with bytes objects are not currently supported")
            }
        }
        FieldLabel::Optional => (),
    }

    let typ = derive_scalar_type(seen_messages, field, descriptors)?;
    Ok(match field.field_label() {
        FieldLabel::Repeated => ScalarType::List {
            element_type: Box::new(typ),
            custom_oid: None,
        },
        _ => typ,
    })
}

fn derive_scalar_type<'a>(
    seen_messages: &mut HashSet<&'a str>,
    field: &'a FieldDescriptor,
    descriptors: &'a Descriptors,
) -> Result<ScalarType> {
    Ok(match field.field_type(descriptors) {
        FieldType::Bool => ScalarType::Bool,
        FieldType::Int32 | FieldType::SInt32 | FieldType::SFixed32 => ScalarType::Int32,
        FieldType::Int64 | FieldType::SInt64 | FieldType::SFixed64 => ScalarType::Int64,
        FieldType::Enum(_) => ScalarType::String,
        FieldType::Float => ScalarType::Float32,
        FieldType::Double => ScalarType::Float64,
        FieldType::UInt32 => bail!("Protobuf type \"uint32\" is not supported"),
        FieldType::UInt64 => bail!("Protobuf type \"uint64\" is not supported"),
        FieldType::Fixed32 => bail!("Protobuf type \"fixed32\" is not supported"),
        FieldType::Fixed64 => bail!("Protobuf type \"fixed64\" is not supported"),
        FieldType::String => ScalarType::String,
        FieldType::Bytes => ScalarType::Bytes,
        FieldType::Message(m) => {
            if seen_messages.contains(m.name()) {
                bail!("Recursive types are not supported: {}", m.name());
            }
            seen_messages.insert(m.name());
            let mut fields = Vec::with_capacity(m.fields().len());
            for field in m.fields() {
                let column_name = ColumnName::from(field.name());
                let scalar_type =
                    derive_scalar_type_from_proto_field(seen_messages, field, descriptors)?;
                let nullable = match field.field_label() {
                    FieldLabel::Optional => true,
                    FieldLabel::Repeated | FieldLabel::Required => false,
                };
                let column_type = ColumnType {
                    scalar_type,
                    nullable,
                };
                fields.push((column_name, column_type))
            }
            seen_messages.remove(m.name());
            ScalarType::Record {
                fields,
                custom_oid: None,
                custom_name: None,
            }
        }
        FieldType::Group => bail!("Unions are currently not supported"),
        FieldType::UnresolvedMessage(m) => bail!("Unresolved message {} not supported", m),
        FieldType::UnresolvedEnum(e) => bail!("Unresolved enum {} not supported", e),
    })
}

pub fn decode_descriptors(descriptors: &[u8]) -> Result<decode::DecodedDescriptors> {
    let proto: protobuf::descriptor::FileDescriptorSet =
        protobuf::Message::parse_from_bytes(descriptors)
            .context("parsing encoded protobuf descriptors failed")?;

    // Iterate through the FileDescriptors to get the first Message name,
    // which might not be in the first file!
    for file in proto.file.iter() {
        if let Some(message) = file.get_message_type().iter().next() {
            return Ok(decode::DecodedDescriptors {
                descriptors: Descriptors::from_proto(&proto),
                first_message_name: format!(".{}", message.get_name().to_owned()),
            });
        }
    }

    Err(anyhow!("file descriptor set must have a message"))
}

pub fn validate_descriptors(message_name: &str, descriptors: &Descriptors) -> Result<RelationDesc> {
    let proto_name = proto_message_name(message_name);
    let message = descriptors.message_by_name(&proto_name).ok_or_else(|| {
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
                scalar_type: derive_scalar_type_from_proto_field(
                    &mut seen_messages,
                    &f,
                    descriptors,
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

#[cfg(test)]
mod tests {
    use anyhow::{bail, Error};
    use ordered_float::OrderedFloat;
    use protobuf::{Message, RepeatedField};
    use serde_protobuf::descriptor::{
        Descriptors, FieldDescriptor, FieldLabel, FieldType, InternalFieldType, MessageDescriptor,
    };

    use repr::{Datum, DatumList, RelationDesc, ScalarType};

    use gen::fuzz::{
        Color, TestNestedRecord, TestRecord, TestRepeatedNestedRecord, TestRepeatedRecord,
    };

    use super::decode;

    mod gen {
        include!(concat!(env!("OUT_DIR"), "/protobuf/mod.rs"));
    }

    fn sanity_check_relation(
        relation: &RelationDesc,
        message: &MessageDescriptor,
        descriptors: &Descriptors,
    ) -> Result<(), Error> {
        let check_types = |name: &str,
                           field_type: FieldType,
                           field_label: FieldLabel,
                           scalar_type: &ScalarType| {
            match (field_type, scalar_type) {
            (FieldType::Bool, ScalarType::Bool)
            | (FieldType::Int32, ScalarType::Int32)
            | (FieldType::SInt32, ScalarType::Int32)
            | (FieldType::SFixed32, ScalarType::Int32)
            | (FieldType::Enum(_), ScalarType::String)
            | (FieldType::Int64, ScalarType::Int64)
            | (FieldType::SInt64, ScalarType::Int64)
            | (FieldType::SFixed64, ScalarType::Int64)
            | (FieldType::Float, ScalarType::Float32)
            | (FieldType::Double, ScalarType::Float64)
            | (FieldType::UInt32, ScalarType::Numeric {scale: Some(0)})
            | (FieldType::Fixed32, ScalarType::Numeric {scale: Some(0)})
            | (FieldType::UInt64, ScalarType::Numeric {scale: Some(0)})
            | (FieldType::Fixed64, ScalarType::Numeric {scale: Some(0)})
            | (FieldType::String, ScalarType::String)
            | (FieldType::Bytes, ScalarType::Bytes)
            | (FieldType::Message(_), ScalarType::Record { .. }) => return Ok(()),
            | (ft, st) => bail!(
                "Mismatched field types for proto field {:?} proto type {:?} label {:?} scalar type {:?}",
                name, ft, field_label, st
            ),
        }
        };

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

            match field_descriptor.field_label() {
                FieldLabel::Required => {
                    bail!("Unsupported required type {:?}", field_descriptor.name())
                }
                FieldLabel::Optional => check_types(
                    field_descriptor.name(),
                    field_descriptor.field_type(descriptors),
                    FieldLabel::Optional,
                    &column_type.scalar_type,
                )?,
                FieldLabel::Repeated => {
                    let inner_typ =
                        if let ScalarType::List { element_type, .. } = &column_type.scalar_type {
                            *element_type.clone()
                        } else {
                            bail!(
                                "Expected list for FieldLabel::Repeated, got {:?}",
                                &column_type.scalar_type
                            )
                        };
                    check_types(
                        field_descriptor.name(),
                        field_descriptor.field_type(descriptors),
                        FieldLabel::Repeated,
                        &inner_typ,
                    )?
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_proto_schema_parsing() -> Result<(), anyhow::Error> {
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
            InternalFieldType::Int32,
            None,
        ));
        descriptors.add_message(m1);

        let mut relation = super::validate_descriptors(".test.message1", &descriptors)
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

        relation = super::validate_descriptors(".test.message2", &descriptors)
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

    fn get_decoder(message_name: &str) -> decode::Decoder {
        let descriptors = Descriptors::from_proto(&gen::file_descriptor_set());
        let relation = super::validate_descriptors(message_name, &descriptors)
            .expect("Failed to parse descriptor");

        sanity_check_relation(
            &relation,
            descriptors
                .message_by_name(message_name)
                .expect("message should be in the descriptor set"),
            &descriptors,
        )
        .expect("Sanity checking descriptors failed");
        decode::Decoder::new(descriptors, message_name)
    }

    #[test]
    fn test_decode() {
        let mut test_record = TestRecord::new();

        test_record.set_int_field(1);
        test_record.set_string_field("one".to_string());
        test_record.set_int64_field(10000);
        test_record.set_color_field(Color::BLUE);
        test_record.set_float_field(5.456);
        test_record.set_double_field(99.99);

        let bytes = test_record
            .write_to_bytes()
            .expect("test failed to serialize to bytes");

        let mut decoder = get_decoder(".TestRecord");
        let row = decoder
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let expected = vec![
            Datum::Int32(1),
            Datum::String("one"),
            Datum::Int64(10000),
            Datum::String("BLUE"),
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
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let expected = vec![
            Datum::Int32(1),
            Datum::String(""),
            Datum::Int64(0),
            Datum::String("RED"),
            Datum::Float32(OrderedFloat::from(0.0)),
            Datum::Float64(OrderedFloat::from(0.0)),
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
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let d = datums[0];
        if let Datum::List(d) = d {
            let mut datumlist = d.iter().collect::<Vec<Datum>>();
            datumlist.sort();
            assert_eq!(
                datumlist,
                vec![Datum::Int32(1), Datum::Int32(2), Datum::Int32(3)]
            );
        } else {
            panic!("Expected the first field to be a list of datums!");
        }

        for i in 1..datums.len() {
            let d = datums[i];
            assert_eq!(d, Datum::List(DatumList::empty()));
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
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();
        let d = datums[0];
        if let Datum::List(d) = d {
            let mut datumlist = d.iter().collect::<Vec<Datum>>();
            datumlist.sort();
            assert_eq!(
                datumlist,
                vec![
                    Datum::Int32(1),
                    Datum::Float64(OrderedFloat(0.0)),
                    Datum::Float64(OrderedFloat(0.0)),
                    Datum::Float64(OrderedFloat(0.0)),
                    Datum::String("RED"),
                    Datum::String("one")
                ]
            )
        } else {
            panic!("Expected the first field to be a list of datums!");
        }

        assert_eq!(datums[1], Datum::Null);

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
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row2.iter().collect::<Vec<_>>();

        let d = datums[1];
        if let Datum::List(d) = d {
            let mut datumlist = d.iter().collect::<Vec<Datum>>();
            datumlist.sort();

            let first = datumlist[0];
            assert_eq!(first, Datum::List(DatumList::empty()));

            let second = datumlist[1];
            assert_eq!(second, Datum::List(DatumList::empty()));

            let third = datumlist[2];
            if let Datum::List(dl) = third {
                let third_datumlist = dl.iter().collect::<Vec<Datum>>();
                assert_eq!(
                    third_datumlist,
                    vec![
                        Datum::String("start"),
                        Datum::String("two"),
                        Datum::String("three"),
                    ]
                );
            } else {
                panic!("expected datum to be list")
            }
        } else {
            panic!("Expected the second field to be a list of datums!");
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
            .decode(&bytes, None, false)
            .expect("deserialize protobuf into a row")
            .unwrap();
        let datums = row.iter().collect::<Vec<_>>();

        let d = datums[0];
        if let Datum::List(d) = d {
            let datumlist = d.iter().collect::<Vec<Datum>>();

            for datum in datumlist {
                if let Datum::List(d) = datum {
                    let mut inner_datumlist = d.iter().collect::<Vec<Datum>>();
                    inner_datumlist.sort();
                    assert_eq!(
                        inner_datumlist,
                        vec![
                            Datum::Int32(1),
                            Datum::Float64(OrderedFloat(0.0)),
                            Datum::Float64(OrderedFloat(0.0)),
                            Datum::Float64(OrderedFloat(0.0)),
                            Datum::String(""),
                            Datum::String("RED")
                        ]
                    )
                } else {
                    panic!("Expected the inner elements to be lists of datums");
                }
            }
        } else {
            panic!("Expected the first field to be a list of datums!");
        }

        for i in 1..datums.len() {
            let d = datums[i];
            assert_eq!(d, Datum::List(DatumList::empty()));
        }
    }
}
