// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits related to the *decoding* of data for sources.

use anyhow::Context;
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_interchange::{avro, protobuf};
use mz_repr::adt::regex::any_regex;
use mz_repr::proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{ColumnType, RelationDesc, ScalarType};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.types.sources.encoding.rs"
));

/// A description of how to interpret data from various sources
///
/// Almost all sources only present values as part of their records, but Kafka allows a key to be
/// associated with each record, which has a possibly independent encoding.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SourceDataEncoding {
    Single(DataEncoding),
    KeyValue {
        key: DataEncoding,
        value: DataEncoding,
    },
}

impl RustType<ProtoSourceDataEncoding> for SourceDataEncoding {
    fn into_proto(self: &Self) -> ProtoSourceDataEncoding {
        use proto_source_data_encoding::{Kind, ProtoKeyValue};
        ProtoSourceDataEncoding {
            kind: Some(match self {
                SourceDataEncoding::Single(s) => Kind::Single(s.into_proto()),
                SourceDataEncoding::KeyValue { key, value } => Kind::KeyValue(ProtoKeyValue {
                    key: Some(key.into_proto()),
                    value: Some(value.into_proto()),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceDataEncoding) -> Result<Self, TryFromProtoError> {
        use proto_source_data_encoding::{Kind, ProtoKeyValue};
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSourceDataEncoding::kind"))?;
        Ok(match kind {
            Kind::Single(s) => SourceDataEncoding::Single(s.into_rust()?),
            Kind::KeyValue(ProtoKeyValue { key, value }) => SourceDataEncoding::KeyValue {
                key: key.into_rust_if_some("ProtoKeyValue::key")?,
                value: value.into_rust_if_some("ProtoKeyValue::value")?,
            },
        })
    }
}

impl SourceDataEncoding {
    pub fn key_ref(&self) -> Option<&DataEncoding> {
        match self {
            SourceDataEncoding::Single(_) => None,
            SourceDataEncoding::KeyValue { key, .. } => Some(key),
        }
    }

    /// Return either the Single encoding if this was a `SourceDataEncoding::Single`, else return the value encoding
    pub fn value(self) -> DataEncoding {
        match self {
            SourceDataEncoding::Single(encoding) => encoding,
            SourceDataEncoding::KeyValue { value, .. } => value,
        }
    }

    pub fn value_ref(&self) -> &DataEncoding {
        match self {
            SourceDataEncoding::Single(encoding) => encoding,
            SourceDataEncoding::KeyValue { value, .. } => value,
        }
    }

    pub fn desc(&self) -> Result<(Option<RelationDesc>, RelationDesc), anyhow::Error> {
        Ok(match self {
            SourceDataEncoding::Single(value) => (None, value.desc()?),
            SourceDataEncoding::KeyValue { key, value } => (Some(key.desc()?), value.desc()?),
        })
    }
}

/// A description of how each row should be decoded, from a string of bytes to a sequence of
/// Differential updates.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DataEncoding {
    Avro(AvroEncoding),
    Protobuf(ProtobufEncoding),
    Csv(CsvEncoding),
    Regex(RegexEncoding),
    Postgres,
    Bytes,
    Text,
    RowCodec(RelationDesc),
}

impl RustType<ProtoDataEncoding> for DataEncoding {
    fn into_proto(self: &Self) -> ProtoDataEncoding {
        use proto_data_encoding::Kind;
        ProtoDataEncoding {
            kind: Some(match self {
                DataEncoding::Avro(e) => Kind::Avro(e.into_proto()),
                DataEncoding::Protobuf(e) => Kind::Protobuf(e.into_proto()),
                DataEncoding::Csv(e) => Kind::Csv(e.into_proto()),
                DataEncoding::Regex(e) => Kind::Regex(e.into_proto()),
                DataEncoding::Postgres => Kind::Postgres(()),
                DataEncoding::Bytes => Kind::Bytes(()),
                DataEncoding::Text => Kind::Text(()),
                DataEncoding::RowCodec(e) => Kind::RowCodec(e.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDataEncoding) -> Result<Self, TryFromProtoError> {
        use proto_data_encoding::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoDataEncoding::kind"))?;
        Ok(match kind {
            Kind::Avro(e) => DataEncoding::Avro(e.into_rust()?),
            Kind::Protobuf(e) => DataEncoding::Protobuf(e.into_rust()?),
            Kind::Csv(e) => DataEncoding::Csv(e.into_rust()?),
            Kind::Regex(e) => DataEncoding::Regex(e.into_rust()?),
            Kind::Postgres(()) => DataEncoding::Postgres,
            Kind::Bytes(()) => DataEncoding::Bytes,
            Kind::Text(()) => DataEncoding::Text,
            Kind::RowCodec(e) => DataEncoding::RowCodec(e.into_rust()?),
        })
    }
}

pub fn included_column_desc(included_columns: Vec<(&str, ColumnType)>) -> RelationDesc {
    let mut desc = RelationDesc::empty();
    for (name, ty) in included_columns {
        desc = desc.with_column(name, ty);
    }
    desc
}

impl DataEncoding {
    /// Computes the [`RelationDesc`] for the relation specified by this
    /// data encoding and envelope.
    ///
    /// If a key desc is provided it will be prepended to the returned desc
    fn desc(&self) -> Result<RelationDesc, anyhow::Error> {
        // Add columns for the data, based on the encoding format.
        Ok(match self {
            DataEncoding::Bytes => {
                RelationDesc::empty().with_column("data", ScalarType::Bytes.nullable(false))
            }
            DataEncoding::Avro(AvroEncoding { schema, .. }) => {
                let parsed_schema = avro::parse_schema(schema).context("validating avro schema")?;
                avro::schema_to_relationdesc(parsed_schema).context("validating avro schema")?
            }
            DataEncoding::Protobuf(ProtobufEncoding {
                descriptors,
                message_name,
                confluent_wire_format: _,
            }) => protobuf::DecodedDescriptors::from_bytes(descriptors, message_name.to_owned())?
                .columns()
                .iter()
                .fold(RelationDesc::empty(), |desc, (name, ty)| {
                    desc.with_column(name, ty.clone())
                }),
            DataEncoding::Regex(RegexEncoding { regex }) => regex
                .capture_names()
                .enumerate()
                // The first capture is the entire matched string. This will
                // often not be useful, so skip it. If people want it they can
                // just surround their entire regex in an explicit capture
                // group.
                .skip(1)
                .fold(RelationDesc::empty(), |desc, (i, name)| {
                    let name = match name {
                        None => format!("column{}", i),
                        Some(name) => name.to_owned(),
                    };
                    let ty = ScalarType::String.nullable(true);
                    desc.with_column(name, ty)
                }),
            DataEncoding::Csv(CsvEncoding { columns, .. }) => match columns {
                ColumnSpec::Count(n) => {
                    (1..=*n).into_iter().fold(RelationDesc::empty(), |desc, i| {
                        desc.with_column(format!("column{}", i), ScalarType::String.nullable(false))
                    })
                }
                ColumnSpec::Header { names } => names
                    .iter()
                    .map(|s| &**s)
                    .fold(RelationDesc::empty(), |desc, name| {
                        desc.with_column(name, ScalarType::String.nullable(false))
                    }),
            },
            DataEncoding::Text => {
                RelationDesc::empty().with_column("text", ScalarType::String.nullable(false))
            }
            DataEncoding::Postgres => RelationDesc::empty()
                .with_column("oid", ScalarType::Int32.nullable(false))
                .with_column(
                    "row_data",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::String),
                        custom_id: None,
                    }
                    .nullable(false),
                ),
            DataEncoding::RowCodec(desc) => desc.clone(),
        })
    }

    pub fn op_name(&self) -> &'static str {
        match self {
            DataEncoding::Bytes => "Bytes",
            DataEncoding::Avro(_) => "Avro",
            DataEncoding::Protobuf(_) => "Protobuf",
            DataEncoding::Regex { .. } => "Regex",
            DataEncoding::Csv(_) => "Csv",
            DataEncoding::Text => "Text",
            DataEncoding::Postgres => "Postgres",
            DataEncoding::RowCodec(_) => "RowCodec",
        }
    }
}

/// Encoding in Avro format.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AvroEncoding {
    pub schema: String,
    pub schema_registry_config: Option<mz_ccsr::ClientConfig>,
    pub confluent_wire_format: bool,
}

impl RustType<ProtoAvroEncoding> for AvroEncoding {
    fn into_proto(self: &Self) -> ProtoAvroEncoding {
        ProtoAvroEncoding {
            schema: self.schema.clone(),
            schema_registry_config: self.schema_registry_config.into_proto(),
            confluent_wire_format: self.confluent_wire_format,
        }
    }

    fn from_proto(proto: ProtoAvroEncoding) -> Result<Self, TryFromProtoError> {
        Ok(AvroEncoding {
            schema: proto.schema,
            schema_registry_config: proto.schema_registry_config.into_rust()?,
            confluent_wire_format: proto.confluent_wire_format,
        })
    }
}

/// Encoding in Protobuf format.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtobufEncoding {
    pub descriptors: Vec<u8>,
    pub message_name: String,
    pub confluent_wire_format: bool,
}

impl RustType<ProtoProtobufEncoding> for ProtobufEncoding {
    fn into_proto(self: &Self) -> ProtoProtobufEncoding {
        ProtoProtobufEncoding {
            descriptors: self.descriptors.clone(),
            message_name: self.message_name.clone(),
            confluent_wire_format: self.confluent_wire_format,
        }
    }

    fn from_proto(proto: ProtoProtobufEncoding) -> Result<Self, TryFromProtoError> {
        Ok(ProtobufEncoding {
            descriptors: proto.descriptors,
            message_name: proto.message_name,
            confluent_wire_format: proto.confluent_wire_format,
        })
    }
}

/// Arguments necessary to define how to decode from CSV format
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CsvEncoding {
    pub columns: ColumnSpec,
    pub delimiter: u8,
}

impl RustType<ProtoCsvEncoding> for CsvEncoding {
    fn into_proto(self: &Self) -> ProtoCsvEncoding {
        ProtoCsvEncoding {
            columns: Some(self.columns.into_proto()),
            delimiter: self.delimiter.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCsvEncoding) -> Result<Self, TryFromProtoError> {
        Ok(CsvEncoding {
            columns: proto
                .columns
                .into_rust_if_some("ProtoCsvEncoding::columns")?,
            delimiter: proto.delimiter.into_rust()?,
        })
    }
}

/// Determines the RelationDesc and decoding of CSV objects
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ColumnSpec {
    /// The first row is not a header row, and all columns get default names like `columnN`.
    Count(usize),
    /// The first row is a header row and therefore does become data
    ///
    /// Each of the values in `names` becomes the default name of a column in the dataflow.
    Header { names: Vec<String> },
}

impl RustType<ProtoColumnSpec> for ColumnSpec {
    fn into_proto(self: &Self) -> ProtoColumnSpec {
        use proto_column_spec::{Kind, ProtoHeader};
        ProtoColumnSpec {
            kind: Some(match self {
                ColumnSpec::Count(c) => Kind::Count(c.into_proto()),
                ColumnSpec::Header { names } => Kind::Header(ProtoHeader {
                    names: names.clone(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoColumnSpec) -> Result<Self, TryFromProtoError> {
        use proto_column_spec::{Kind, ProtoHeader};
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoColumnSpec::kind"))?;
        Ok(match kind {
            Kind::Count(c) => ColumnSpec::Count(c.into_rust()?),
            Kind::Header(ProtoHeader { names }) => ColumnSpec::Header { names },
        })
    }
}

impl ColumnSpec {
    /// The number of columns described by the column spec.
    pub fn arity(&self) -> usize {
        match self {
            ColumnSpec::Count(n) => *n,
            ColumnSpec::Header { names } => names.len(),
        }
    }

    pub fn into_header_names(self) -> Option<Vec<String>> {
        match self {
            ColumnSpec::Count(_) => None,
            ColumnSpec::Header { names } => Some(names),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RegexEncoding {
    pub regex: mz_repr::adt::regex::Regex,
}

impl Arbitrary for RegexEncoding {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_regex()
            .prop_map(|regex| RegexEncoding { regex })
            .boxed()
    }
}

impl RustType<ProtoRegexEncoding> for RegexEncoding {
    fn into_proto(self: &Self) -> ProtoRegexEncoding {
        ProtoRegexEncoding {
            regex: Some(self.regex.into_proto()),
        }
    }

    fn from_proto(proto: ProtoRegexEncoding) -> Result<Self, TryFromProtoError> {
        Ok(RegexEncoding {
            regex: proto.regex.into_rust_if_some("ProtoRegexEncoding::regex")?,
        })
    }
}
