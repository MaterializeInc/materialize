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
use mz_interchange::{avro, protobuf};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::regex::any_regex;
use mz_repr::{ColumnType, GlobalId, RelationDesc, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::AlterCompatible;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.encoding.rs"
));

/// A description of how to interpret data from various sources
///
/// Almost all sources only present values as part of their records, but Kafka allows a key to be
/// associated with each record, which has a possibly independent encoding.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SourceDataEncoding<C: ConnectionAccess = InlinedConnection> {
    pub key: Option<DataEncoding<C>>,
    pub value: DataEncoding<C>,
}

impl<C: ConnectionAccess> SourceDataEncoding<C> {
    pub fn desc(&self) -> Result<(Option<RelationDesc>, RelationDesc), anyhow::Error> {
        Ok(match &self.key {
            None => (None, self.value.desc()?),
            Some(key) => (Some(key.desc()?), self.value.desc()?),
        })
    }
}

impl<R: ConnectionResolver> IntoInlineConnection<SourceDataEncoding, R>
    for SourceDataEncoding<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> SourceDataEncoding {
        SourceDataEncoding {
            key: self.key.map(|enc| enc.into_inline_connection(&r)),
            value: self.value.into_inline_connection(&r),
        }
    }
}

impl RustType<ProtoSourceDataEncoding> for SourceDataEncoding {
    fn into_proto(&self) -> ProtoSourceDataEncoding {
        ProtoSourceDataEncoding {
            key: self.key.into_proto(),
            value: Some(self.value.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceDataEncoding) -> Result<Self, TryFromProtoError> {
        Ok(SourceDataEncoding {
            key: proto.key.into_rust()?,
            value: proto.value.into_rust_if_some("ProtoKeyValue::value")?,
        })
    }
}

impl<C: ConnectionAccess> AlterCompatible for SourceDataEncoding<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let SourceDataEncoding { key, value } = self;

        let compatibility_checks = [
            (
                match (key, &other.key) {
                    (Some(s), Some(o)) => s.alter_compatible(id, o).is_ok(),
                    (s, o) => s == o,
                },
                "key",
            ),
            (value.alter_compatible(id, &other.value).is_ok(), "value"),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "SourceDataEncoding incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

/// A description of how each row should be decoded, from a string of bytes to a sequence of
/// Differential updates.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DataEncoding<C: ConnectionAccess = InlinedConnection> {
    Avro(AvroEncoding<C>),
    Protobuf(ProtobufEncoding),
    Csv(CsvEncoding),
    Regex(RegexEncoding),
    Bytes,
    Json,
    Text,
}

impl<R: ConnectionResolver> IntoInlineConnection<DataEncoding, R>
    for DataEncoding<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> DataEncoding {
        match self {
            Self::Avro(conn) => DataEncoding::Avro(conn.into_inline_connection(r)),
            Self::Protobuf(conn) => DataEncoding::Protobuf(conn),
            Self::Csv(conn) => DataEncoding::Csv(conn),
            Self::Regex(conn) => DataEncoding::Regex(conn),
            Self::Bytes => DataEncoding::Bytes,
            Self::Json => DataEncoding::Json,
            Self::Text => DataEncoding::Text,
        }
    }
}

impl RustType<ProtoDataEncoding> for DataEncoding {
    fn into_proto(&self) -> ProtoDataEncoding {
        use proto_data_encoding::Kind;
        ProtoDataEncoding {
            kind: Some(match self {
                DataEncoding::Avro(e) => Kind::Avro(e.into_proto()),
                DataEncoding::Protobuf(e) => Kind::Protobuf(e.into_proto()),
                DataEncoding::Csv(e) => Kind::Csv(e.into_proto()),
                DataEncoding::Regex(e) => Kind::Regex(e.into_proto()),
                DataEncoding::Bytes => Kind::Bytes(()),
                DataEncoding::Text => Kind::Text(()),
                DataEncoding::Json => Kind::Json(()),
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
            Kind::Bytes(()) => DataEncoding::Bytes,
            Kind::Text(()) => DataEncoding::Text,
            Kind::Json(()) => DataEncoding::Json,
        })
    }
}

pub fn included_column_desc(included_columns: Vec<(&str, ColumnType)>) -> RelationDesc {
    let mut desc = RelationDesc::builder();
    for (name, ty) in included_columns {
        desc = desc.with_column(name, ty);
    }
    desc.finish()
}

impl<C: ConnectionAccess> DataEncoding<C> {
    /// A human-readable name for the type of encoding
    pub fn type_(&self) -> &str {
        match self {
            Self::Avro(_) => "avro",
            Self::Protobuf(_) => "protobuf",
            Self::Csv(_) => "csv",
            Self::Regex(_) => "regex",
            Self::Bytes => "bytes",
            Self::Json => "json",
            Self::Text => "text",
        }
    }

    /// Computes the [`RelationDesc`] for the relation specified by this
    /// data encoding.
    fn desc(&self) -> Result<RelationDesc, anyhow::Error> {
        // Add columns for the data, based on the encoding format.
        Ok(match self {
            Self::Bytes => RelationDesc::builder()
                .with_column("data", ScalarType::Bytes.nullable(false))
                .finish(),
            Self::Json => RelationDesc::builder()
                .with_column("data", ScalarType::Jsonb.nullable(false))
                .finish(),
            Self::Avro(AvroEncoding { schema, .. }) => {
                let parsed_schema = avro::parse_schema(schema).context("validating avro schema")?;
                avro::schema_to_relationdesc(parsed_schema).context("validating avro schema")?
            }
            Self::Protobuf(ProtobufEncoding {
                descriptors,
                message_name,
                confluent_wire_format: _,
            }) => protobuf::DecodedDescriptors::from_bytes(descriptors, message_name.to_owned())?
                .columns()
                .iter()
                .fold(RelationDesc::builder(), |desc, (name, ty)| {
                    desc.with_column(name, ty.clone())
                })
                .finish(),
            Self::Regex(RegexEncoding { regex }) => regex
                .capture_names()
                .enumerate()
                // The first capture is the entire matched string. This will
                // often not be useful, so skip it. If people want it they can
                // just surround their entire regex in an explicit capture
                // group.
                .skip(1)
                .fold(RelationDesc::builder(), |desc, (i, name)| {
                    let name = match name {
                        None => format!("column{}", i),
                        Some(name) => name.to_owned(),
                    };
                    let ty = ScalarType::String.nullable(true);
                    desc.with_column(name, ty)
                })
                .finish(),
            Self::Csv(CsvEncoding { columns, .. }) => match columns {
                ColumnSpec::Count(n) => (1..=*n)
                    .fold(RelationDesc::builder(), |desc, i| {
                        desc.with_column(format!("column{}", i), ScalarType::String.nullable(false))
                    })
                    .finish(),
                ColumnSpec::Header { names } => names
                    .iter()
                    .map(|s| &**s)
                    .fold(RelationDesc::builder(), |desc, name| {
                        desc.with_column(name, ScalarType::String.nullable(false))
                    })
                    .finish(),
            },
            Self::Text => RelationDesc::builder()
                .with_column("text", ScalarType::String.nullable(false))
                .finish(),
        })
    }

    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Bytes => "Bytes",
            Self::Json => "Json",
            Self::Avro(_) => "Avro",
            Self::Protobuf(_) => "Protobuf",
            Self::Regex { .. } => "Regex",
            Self::Csv(_) => "Csv",
            Self::Text => "Text",
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for DataEncoding<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let compatible = match (self, other) {
            (DataEncoding::Avro(avro), DataEncoding::Avro(other_avro)) => {
                avro.alter_compatible(id, other_avro).is_ok()
            }
            (s, o) => s == o,
        };

        if !compatible {
            tracing::warn!(
                "DataEncoding incompatible :\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );

            return Err(AlterError { id });
        }

        Ok(())
    }
}

/// Encoding in Avro format.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AvroEncoding<C: ConnectionAccess = InlinedConnection> {
    pub schema: String,
    pub csr_connection: Option<C::Csr>,
    pub confluent_wire_format: bool,
}

impl<R: ConnectionResolver> IntoInlineConnection<AvroEncoding, R>
    for AvroEncoding<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> AvroEncoding {
        let AvroEncoding {
            schema,
            csr_connection,
            confluent_wire_format,
        } = self;
        AvroEncoding {
            schema,
            csr_connection: csr_connection.map(|csr| r.resolve_connection(csr).unwrap_csr()),
            confluent_wire_format,
        }
    }
}

impl<C: ConnectionAccess> AlterCompatible for AvroEncoding<C> {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        if self == other {
            return Ok(());
        }

        let AvroEncoding {
            schema,
            csr_connection,
            confluent_wire_format,
        } = self;

        let compatibility_checks = [
            (schema == &other.schema, "schema"),
            (
                match (csr_connection, &other.csr_connection) {
                    (Some(s), Some(o)) => s.alter_compatible(id, o).is_ok(),
                    (s, o) => s == o,
                },
                "csr_connection",
            ),
            (
                confluent_wire_format == &other.confluent_wire_format,
                "confluent_wire_format",
            ),
        ];

        for (compatible, field) in compatibility_checks {
            if !compatible {
                tracing::warn!(
                    "AvroEncoding incompatible at {field}:\nself:\n{:#?}\n\nother\n{:#?}",
                    self,
                    other
                );

                return Err(AlterError { id });
            }
        }

        Ok(())
    }
}

impl RustType<ProtoAvroEncoding> for AvroEncoding {
    fn into_proto(&self) -> ProtoAvroEncoding {
        ProtoAvroEncoding {
            schema: self.schema.clone(),
            csr_connection: self.csr_connection.into_proto(),
            confluent_wire_format: self.confluent_wire_format,
        }
    }

    fn from_proto(proto: ProtoAvroEncoding) -> Result<Self, TryFromProtoError> {
        Ok(AvroEncoding {
            schema: proto.schema,
            csr_connection: proto.csr_connection.into_rust()?,
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
    fn into_proto(&self) -> ProtoProtobufEncoding {
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
    fn into_proto(&self) -> ProtoCsvEncoding {
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
    fn into_proto(&self) -> ProtoColumnSpec {
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct RegexEncoding {
    #[proptest(strategy = "any_regex()")]
    pub regex: mz_repr::adt::regex::Regex,
}

impl RustType<ProtoRegexEncoding> for RegexEncoding {
    fn into_proto(&self) -> ProtoRegexEncoding {
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
