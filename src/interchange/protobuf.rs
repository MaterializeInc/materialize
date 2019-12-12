use std::fs;

use failure::{bail, Error};
use protoc::Protoc;
use serde_protobuf::descriptor::{
    Descriptors, FieldDescriptor, FieldLabel, FieldType, MessageDescriptor,
};

use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};

// Takes a path to a .proto spec and attempts to generate a binary file
// containing a set of descriptors for the message (and any nested messages)
// defined in the spec. Only used for test purposes
fn generate_descriptors(proto_path: &str, out: &str) -> Descriptors {
    let protoc = Protoc::from_env_path();
    let descriptor_set_out_args = protoc::DescriptorSetOutArgs {
        out: out,
        includes: &[],
        input: &[proto_path],
        include_imports: false,
    };

    protoc
        .write_descriptor_set(descriptor_set_out_args)
        .expect("protoc write descriptor set failed");

    let mut file = fs::File::open(out).expect("Opening descriptor set file failed");
    let proto = protobuf::parse_from_reader(&mut file).expect("parsing descriptor set failed");
    Descriptors::from_proto(&proto)
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
                FieldType::Enum(_) => bail!("Nested enums are currently unsupported"),
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
    Ok(match field.field_label() {
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
            | FieldType::Bytes => (),

            FieldType::Message(m) => {
                for f in m.fields().iter() {
                    validate_proto_field_resolved(&f, descriptors)?;
                }
                ()
            }
            FieldType::Enum(_) => bail!("Nested enums are currently unsupported"),
            FieldType::Group => bail!("Unions are currently not supported"),
            FieldType::UnresolvedMessage(a) => bail!("Nested message type {} unresolved", a),
            FieldType::UnresolvedEnum(e) => bail!("Unresolved enum type {}", e),
        },
    })
}

fn validate_proto_schema(
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
                nullable: false,
                scalar_type: validate_proto_field(&f, descriptors)?,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let column_names = message.fields().iter().map(|f| Some(f.name().clone()));
    Ok(RelationDesc::new(
        RelationType::new(column_types),
        column_names,
    ))
}

#[cfg(test)]
mod tests {
    use failure::{bail, Error};
    use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};
    use serde_protobuf::descriptor::{
        Descriptors, FieldDescriptor, FieldLabel, FieldType, InternalFieldType, MessageDescriptor,
        MessageId,
    };

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
                        FieldType::UnresolvedMessage(_) | FieldType::UnresolvedEnum(_) | FieldType::Group | FieldType::Enum(_) => {
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

        let mut relation = super::validate_proto_schema(".test.message1", &descriptors)
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

        relation = super::validate_proto_schema(".test.message2", &descriptors)
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
}
