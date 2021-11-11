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

use anyhow::{bail, Result};
use serde_protobuf::descriptor::{Descriptors, FieldDescriptor, FieldLabel, FieldType};

use repr::{ColumnName, ColumnType, ScalarType};

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
