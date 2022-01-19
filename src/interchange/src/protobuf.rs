// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::iter;
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;

use prost_reflect::{
    Cardinality, DynamicMessage, FieldDescriptor, FileDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};
use protobuf::Message;
use serde::{Deserialize, Serialize};

use ccsr::Subject;
use mz_protoc::Protoc;
use ore::str::StrExt;
use repr::{strconv, ColumnName, ColumnType, Datum, Row, ScalarType};
use sql_parser::ast::CsrSeedCompiledEncoding;

/// Wrapper type that ensures a protobuf message name is properly normalized.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedProtobufMessageName(String);

impl NormalizedProtobufMessageName {
    /// Create a new normalized protobuf message name.  A leading dot will be
    /// prepended to the provided message name if necessary.
    pub fn new(mut message_name: String) -> Self {
        if !message_name.starts_with('.') {
            message_name = format!(".{}", message_name);
        }
        NormalizedProtobufMessageName(message_name)
    }
}

/// A decoded description of the schema of a Protobuf message.
#[derive(Debug, PartialEq)]
pub struct DecodedDescriptors {
    message_descriptor: MessageDescriptor,
    columns: Vec<(ColumnName, ColumnType)>,
    message_name: NormalizedProtobufMessageName,
}

impl DecodedDescriptors {
    /// Builds a `DecodedDescriptors` from an encoded `FileDescriptorSet` and
    /// the fully qualified name of a message inside that file descriptor set.
    pub fn from_bytes(
        bytes: &[u8],
        NormalizedProtobufMessageName(message_name): NormalizedProtobufMessageName,
    ) -> Result<Self, anyhow::Error> {
        let fds = FileDescriptor::decode(bytes).context("decoding file descriptor set")?;
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
            message_name: NormalizedProtobufMessageName(message_name),
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
    packer: Row,
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
            packer: Default::default(),
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
        pack_message(&mut self.packer, &message)?;
        Ok(Some(self.packer.finish_and_reuse()))
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
                custom_oid: None,
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
                custom_oid: None,
                custom_name: None,
            };
            Ok(ty.nullable(true))
        }
    }
}

fn pack_message(packer: &mut Row, message: &DynamicMessage) -> Result<(), anyhow::Error> {
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
    packer: &mut Row,
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

/// Given a primary subject and subjects for references (obtained using a ccsr client),
/// compile the message descriptor
pub async fn compile_proto_from_subjects(
    primary_subject: Subject,
    dependency_subjects: Vec<Subject>,
) -> Result<CsrSeedCompiledEncoding, anyhow::Error> {
    lazy_static! {
        static ref WELL_KNOWN_REGEX: Regex = Regex::new(r#"(\.)?google\.protobuf\.\w+"#).unwrap();
        static ref MISSING_IMPORT_ERROR: Regex =
            Regex::new(r#"protobuf path \\"(?P<reference>.*)\\" is not found in import path"#)
                .unwrap();
    }

    let primary_proto_name = primary_subject.name.clone();
    let include_dir = tempfile::tempdir()?;
    let primary_proto_path = include_dir.path().join(&primary_proto_name);

    for subject in iter::once(primary_subject).chain(dependency_subjects.into_iter()) {
        if WELL_KNOWN_REGEX.is_match(&subject.name) {
            continue;
        }
        let subject_pb = PathBuf::from(subject.name);
        if let Some(parent) = subject_pb.parent() {
            tokio::fs::create_dir_all(include_dir.path().join(parent)).await?;
        }
        let path = include_dir.path().join(subject_pb);
        let bytes = strconv::parse_bytes(&subject.schema.raw)?;
        tokio::fs::write(&path, &bytes).await?;
    }

    match Protoc::new()
        .include(include_dir.path())
        .input(primary_proto_path)
        .parse()
    {
        Ok(fds) => {
            let message_name = fds
                .file
                .iter()
                .find(|f| f.get_name() == primary_proto_name)
                .map(|file| file.message_type.iter().at_most_one())
                .transpose()
                .map_err(|_| anyhow!("proto files with multiple `message`'s are not yet supported"))
                .map(|found| found.flatten())
                .and_then(|message| {
                    message
                        .map(|message| format!(".{}", message.get_name()))
                        .ok_or_else(|| anyhow!("unable to compile temporary schema"))
                })?;
            let mut schema = String::new();
            strconv::format_bytes(&mut schema, &fds.write_to_bytes()?);
            Ok(CsrSeedCompiledEncoding {
                schema,
                message_name,
            })
        }
        Err(e) => {
            // Make protobuf import errors more user-friendly.
            if let Some(captures) = MISSING_IMPORT_ERROR.captures(&e.to_string()) {
                bail!(
                    "unsupported protobuf schema reference {}",
                    &captures["reference"]
                )
            } else {
                Err(e)
            }
        }
    }
}
