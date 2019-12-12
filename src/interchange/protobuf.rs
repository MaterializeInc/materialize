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
                FieldType::UInt32 | FieldType::UInt64 | FieldType::Fixed32 | FieldType::Fixed64 => {
                    ScalarType::Decimal(38, 0)
                } // is that right
                FieldType::Message(m) => {
                    for f in m.fields().iter() {
                        validate_proto_field_resolved(&f, descriptors)?;
                    }
                    ScalarType::Jsonb
                }
                _ => bail!("todo write the rest of it"),
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
            FieldType::UnresolvedMessage(a) => bail!("Nested message type {} unresolved", a),
            FieldType::UnresolvedEnum(e) => bail!("Unresolved enum type {}", e),
            FieldType::Group => bail!("Unions are currently not supported"),
            FieldType::Message(m) => {
                for f in m.fields().iter() {
                    validate_proto_field_resolved(&f, descriptors)?;
                }
                ()
            }
            _ => (),
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
use serde_protobuf::descriptor::{
    Descriptors, FieldDescriptor, FieldLabel, InternalFieldType, MessageDescriptor,
};
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowPacker, ScalarType};

    #[test]
    fn test_proto_schema_parsing() -> Result<(), failure::Error> {
        let mut descriptors = Descriptors::new();
        println!("{:?}", descriptors);

        let mut m1 = MessageDescriptor::new(".test.message1");
        m1.add_field(FieldDescriptor::new("name", 1, FieldLabel::Optional, InternalFieldType::String, None));
        m1.add_field(FieldDescriptor::new("age", 2, FieldLabel::Optional, InternalFieldType::UInt32, None));
        descriptors.add_message(m1);

        let relation = super::validate_proto_schema(".test.message1", &descriptors).expect("Failed to parse descriptor");
        let expected_column_types = vec![ColumnType { nullable: false, scalar_type: ScalarType::String }, ColumnType { nullable: false, scalar_type: ScalarType::Decimal(38, 0) }];
        let expected_relation = RelationDesc::new(
            RelationType::new(expected_column_types),
            vec![Some("name".clone()), Some("age".clone())],
        );

        assert_eq!(relation, expected_relation);
        

        Ok(())
    }
}


