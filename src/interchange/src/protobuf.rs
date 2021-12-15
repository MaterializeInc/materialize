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

use protobuf::descriptor::field_descriptor_proto::Label;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::reflect::{
    FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectFieldRef, ReflectValueRef,
    RuntimeFieldType, RuntimeTypeBox,
};
use protobuf::{CodedInputStream, Message, MessageDyn};
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
    /// Builds a `DecodedDescriptors` from an encoded [`FileDescriptorSet`]
    /// and the fully qualified name of a message inside that file descriptor
    /// set.
    pub fn from_bytes(
        bytes: &[u8],
        NormalizedProtobufMessageName(message_name): NormalizedProtobufMessageName,
    ) -> Result<Self, anyhow::Error> {
        let fds =
            FileDescriptorSet::parse_from_bytes(bytes).context("parsing file descriptor set")?;
        let fds = FileDescriptor::new_dynamic_fds(fds.file);
        let message_descriptor = fds
            .iter()
            .find_map(|fd| fd.message_by_full_name(&message_name))
            .ok_or_else(|| {
                anyhow!(
                    "protobuf message {} not found in file descriptor set",
                    message_name.quoted(),
                )
            })?;
        let mut seen_messages = HashSet::new();
        seen_messages.insert(message_descriptor.name().to_owned());
        let mut columns = vec![];
        for field in message_descriptor.fields() {
            let name = ColumnName::from(field.get_name());
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
    ccsr_client: Option<ccsr::Client>,
    validated_schema_id: Option<i32>,
}

impl Decoder {
    /// Constructs a decoder for a particular Protobuf message.
    pub fn new(
        descriptors: DecodedDescriptors,
        schema_registry: Option<ccsr::ClientConfig>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Decoder {
            descriptors,
            packer: Default::default(),
            ccsr_client: schema_registry.map(|sr| sr.build()).transpose()?,
            validated_schema_id: Default::default(),
        })
    }

    /// Decodes the encoded Protobuf message into a [`Row`].
    pub async fn decode(&mut self, mut bytes: &[u8]) -> Result<Option<Row>, anyhow::Error> {
        if let Some(client) = &self.ccsr_client {
            let (schema_id, adjusted_bytes) = crate::confluent::extract_protobuf_header(bytes)?;

            if let Some(validated_schema_id) = self.validated_schema_id {
                if validated_schema_id != schema_id {
                    bail!(
                        "cannot decode protobuf, expected schema id: {}, found {}; schema evolution in protobuf is not supported. \
                        See https://github.com/MaterializeInc/materialize/issues/9598 for more details.",
                        validated_schema_id,
                        schema_id
                    );
                }
            } else {
                let compiled = compile_proto(schema_id, client).await?;
                let schema_to_compare = DecodedDescriptors::from_bytes(
                    &strconv::parse_bytes(&compiled.schema)?,
                    // Needs to match the name exactly
                    self.descriptors.message_name.clone(),
                )?;
                if schema_to_compare.message_descriptor.get_proto()
                    == self.descriptors.message_descriptor.get_proto()
                {
                    self.validated_schema_id = Some(schema_id);
                } else {
                    bail!(
                        "cannot decode protobuf, schema id: {} refers to a schema different than the expected schema; \
                        schema evolution in protobuf is not supported. \
                        See https://github.com/MaterializeInc/materialize/issues/9598 for more details.",
                        schema_id
                    );
                }
            }
            bytes = adjusted_bytes;
        }
        let mut input_stream = CodedInputStream::from_bytes(bytes);
        let mut message = self.descriptors.message_descriptor.new_instance();
        message.merge_from_dyn(&mut input_stream)?;
        pack_message(
            &mut self.packer,
            &self.descriptors.message_descriptor,
            &*message,
        )?;
        Ok(Some(self.packer.finish_and_reuse()))
    }
}

fn derive_column_type(
    seen_messages: &mut HashSet<String>,
    field: &FieldDescriptor,
) -> Result<ColumnType, anyhow::Error> {
    match field.runtime_field_type() {
        RuntimeFieldType::Singular(ty) => derive_inner_type(seen_messages, ty),
        RuntimeFieldType::Repeated(ty) => {
            let element_type = derive_inner_type(seen_messages, ty)?.scalar_type;
            Ok(ColumnType {
                nullable: false,
                scalar_type: ScalarType::List {
                    element_type: Box::new(element_type),
                    custom_oid: None,
                },
            })
        }
        RuntimeFieldType::Map(_, _) => bail!("Protobuf map fields are not supported"),
    }
}

fn derive_inner_type(
    seen_messages: &mut HashSet<String>,
    ty: RuntimeTypeBox,
) -> Result<ColumnType, anyhow::Error> {
    match ty {
        RuntimeTypeBox::Bool => Ok(ScalarType::Bool.nullable(false)),
        RuntimeTypeBox::I32 => Ok(ScalarType::Int32.nullable(false)),
        RuntimeTypeBox::I64 => Ok(ScalarType::Int64.nullable(false)),
        RuntimeTypeBox::U32 | RuntimeTypeBox::U64 => {
            bail!("Protobuf unsigned integer types are not supported")
        }
        RuntimeTypeBox::F32 => Ok(ScalarType::Float32.nullable(false)),
        RuntimeTypeBox::F64 => Ok(ScalarType::Float64.nullable(false)),
        RuntimeTypeBox::String => Ok(ScalarType::String.nullable(false)),
        RuntimeTypeBox::VecU8 => Ok(ScalarType::Bytes.nullable(false)),
        RuntimeTypeBox::Enum(_) => Ok(ScalarType::String.nullable(false)),
        RuntimeTypeBox::Message(m) => {
            if seen_messages.contains(m.name()) {
                bail!("Recursive types are not supported: {}", m.name());
            }
            seen_messages.insert(m.name().to_owned());
            let mut fields = Vec::with_capacity(m.fields().len());
            for field in m.fields() {
                let column_name = ColumnName::from(field.get_name());
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

fn pack_message(
    packer: &mut Row,
    message_desc: &MessageDescriptor,
    message: &dyn MessageDyn,
) -> Result<(), anyhow::Error> {
    for field_desc in message_desc.fields() {
        pack_field(packer, &field_desc, message)?;
    }
    Ok(())
}

fn pack_field(
    packer: &mut Row,
    field_desc: &FieldDescriptor,
    message: &dyn MessageDyn,
) -> Result<(), anyhow::Error> {
    match field_desc.get_reflect(message) {
        ReflectFieldRef::Optional(None) => {
            if field_desc.get_proto().get_label() == Label::LABEL_REQUIRED {
                bail!(
                    "protobuf message missing required field {}",
                    field_desc.get_name()
                );
            }
            match field_desc.singular_runtime_type() {
                RuntimeTypeBox::Message(_) => packer.push(Datum::Null),
                _ => pack_value(packer, field_desc, field_desc.singular_default_value())?,
            }
        }
        ReflectFieldRef::Optional(Some(value)) => pack_value(packer, field_desc, value)?,
        ReflectFieldRef::Repeated(values) => packer.push_list_with(|packer| {
            for value in values {
                pack_value(packer, field_desc, value)?;
            }
            Ok::<_, anyhow::Error>(())
        })?,
        ReflectFieldRef::Map(_) => {
            bail!("internal error: unexpected map field while decoding protobuf")
        }
    }
    Ok(())
}

fn pack_value(
    packer: &mut Row,
    field_desc: &FieldDescriptor,
    value: ReflectValueRef,
) -> Result<(), anyhow::Error> {
    match value {
        ReflectValueRef::Bool(false) => packer.push(Datum::False),
        ReflectValueRef::Bool(true) => packer.push(Datum::True),
        ReflectValueRef::I32(i) => packer.push(Datum::Int32(i)),
        ReflectValueRef::I64(i) => packer.push(Datum::Int64(i)),
        ReflectValueRef::F32(f) => packer.push(Datum::Float32(f.into())),
        ReflectValueRef::F64(f) => packer.push(Datum::Float64(f.into())),
        ReflectValueRef::String(s) => packer.push(Datum::String(s)),
        ReflectValueRef::Bytes(s) => packer.push(Datum::Bytes(s)),
        ReflectValueRef::Enum(enum_desc, i) => match enum_desc.get_value_by_number(i) {
            None => {
                bail!(
                    "error decoding protobuf: enum value {} is missing while decoding field {}",
                    i,
                    field_desc.get_name()
                );
            }
            Some(ev) => packer.push(Datum::String(ev.get_name())),
        },
        ReflectValueRef::Message(m) => {
            packer.push_list_with(|packer| pack_message(packer, &m.descriptor_dyn(), &*m))?
        }
        ReflectValueRef::U32(_) | ReflectValueRef::U64(_) => bail!(
            "internal error: unexpected value while decoding protobuf message: {:?}",
            value
        ),
    }
    Ok(())
}

/// Collect protobuf message descriptor from CSR and compile the descriptor.
///
/// This reaches out to the Confluent Schema Registry to search for the correct schema
/// for the provided subject (generally a Kafka topic with the `-value` or `-key` suffix)
/// and recursively constructs the encoding.
async fn compile_proto(
    id: i32,
    ccsr_client: &ccsr::Client,
) -> Result<CsrSeedCompiledEncoding, anyhow::Error> {
    let (primary_subject, dependency_subjects) =
        ccsr_client.get_subject_and_references_by_id(id).await?;
    compile_proto_from_subjects(primary_subject, dependency_subjects).await
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
