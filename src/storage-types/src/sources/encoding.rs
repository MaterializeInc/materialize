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
use itertools::Itertools;
use mz_interchange::{avro, protobuf};
use mz_repr::{Datum, GlobalId, RelationDesc, Row, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::errors::DecodeErrorKind;
use crate::wire_format::WireFormat;

/// A description of how to interpret data from various sources
///
/// Almost all sources only present values as part of their records, but Kafka allows a key to be
/// associated with each record, which has a possibly independent encoding.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
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
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
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

pub fn included_column_desc(included_columns: Vec<(&str, SqlColumnType)>) -> RelationDesc {
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
                .with_column("data", SqlScalarType::Bytes.nullable(false))
                .finish(),
            Self::Json => RelationDesc::builder()
                .with_column("data", SqlScalarType::Jsonb.nullable(false))
                .finish(),
            Self::Avro(AvroEncoding {
                schema,
                reference_schemas,
                ..
            }) => {
                let parsed_schema = avro::parse_schema(schema, reference_schemas)
                    .context("validating avro schema")?;
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
                    let ty = SqlScalarType::String.nullable(true);
                    desc.with_column(name, ty)
                })
                .finish(),
            Self::Csv(CsvEncoding { columns, .. }) => match columns {
                ColumnSpec::Count(n) => (1..=*n)
                    .fold(RelationDesc::builder(), |desc, i| {
                        desc.with_column(
                            format!("column{}", i),
                            SqlScalarType::String.nullable(false),
                        )
                    })
                    .finish(),
                ColumnSpec::Header { names } => names
                    .iter()
                    .map(|s| &**s)
                    .fold(RelationDesc::builder(), |desc, name| {
                        desc.with_column(name, SqlScalarType::String.nullable(false))
                    })
                    .finish(),
            },
            Self::Text => RelationDesc::builder()
                .with_column("text", SqlScalarType::String.nullable(false))
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
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AvroEncoding<C: ConnectionAccess = InlinedConnection> {
    pub schema: String,
    /// Schemas for types referenced by the main schema, in dependency order.
    /// These are fetched from the schema registry when the source is created.
    #[serde(default)]
    pub reference_schemas: Vec<String>,
    /// How schema identifiers are framed in the Kafka payload, and the
    /// registry (if any) used to resolve writer schemas.
    pub wire_format: WireFormat<C>,
}

impl<R: ConnectionResolver> IntoInlineConnection<AvroEncoding, R>
    for AvroEncoding<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> AvroEncoding {
        let AvroEncoding {
            schema,
            reference_schemas,
            wire_format,
        } = self;
        AvroEncoding {
            schema,
            reference_schemas,
            wire_format: wire_format.into_inline_connection(r),
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
            reference_schemas,
            wire_format,
        } = self;

        let compatibility_checks = [
            (schema == &other.schema, "schema"),
            (
                reference_schemas == &other.reference_schemas,
                "reference_schemas",
            ),
            (
                wire_format.alter_compatible(id, &other.wire_format).is_ok(),
                "wire_format",
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

/// Encoding in Protobuf format.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtobufEncoding {
    pub descriptors: Vec<u8>,
    pub message_name: String,
    pub confluent_wire_format: bool,
}

/// Arguments necessary to define how to decode from CSV format
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CsvEncoding {
    pub columns: ColumnSpec,
    pub delimiter: u8,
}

/// Determines the RelationDesc and decoding of CSV objects
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ColumnSpec {
    /// The first row is not a header row, and all columns get default names like `columnN`.
    Count(usize),
    /// The first row is a header row and therefore does become data
    ///
    /// Each of the values in `names` becomes the default name of a column in the dataflow.
    Header { names: Vec<String> },
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

/// Incrementally decodes CSV bytes into `Row`s for a [`CsvEncoding`].
///
/// This is the pure, dataflow-independent half of CSV source decoding (it lives
/// here, rather than in `mz-storage`, so it can be exercised without that
/// crate's heavy dependency tree). `mz-storage`'s decode operator drives it.
#[derive(Debug)]
pub struct CsvDecoderState {
    next_row_is_header: bool,
    header_names: Option<Vec<String>>,
    n_cols: usize,
    output: Vec<u8>,
    output_cursor: usize,
    ends: Vec<usize>,
    ends_cursor: usize,
    csv_reader: csv_core::Reader,
    row_buf: Row,
    events_error: usize,
    events_success: usize,
}

impl CsvDecoderState {
    fn total_events(&self) -> usize {
        self.events_error + self.events_success
    }

    pub fn new(format: CsvEncoding) -> Self {
        let CsvEncoding { columns, delimiter } = format;
        let n_cols = columns.arity();

        let header_names = columns.into_header_names();
        Self {
            next_row_is_header: header_names.is_some(),
            header_names,
            n_cols,
            output: vec![0],
            output_cursor: 0,
            ends: vec![0],
            ends_cursor: 1,
            csv_reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            row_buf: Row::default(),
            events_error: 0,
            events_success: 0,
        }
    }

    pub fn reset_for_new_object(&mut self) {
        if self.header_names.is_some() {
            self.next_row_is_header = true;
        }
    }

    pub fn decode(&mut self, chunk: &mut &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
        loop {
            let (result, n_input, n_output, n_ends) = self.csv_reader.read_record(
                *chunk,
                &mut self.output[self.output_cursor..],
                &mut self.ends[self.ends_cursor..],
            );
            self.output_cursor += n_output;
            *chunk = &(*chunk)[n_input..];
            self.ends_cursor += n_ends;
            match result {
                // Error cases
                csv_core::ReadRecordResult::InputEmpty => break Ok(None),
                csv_core::ReadRecordResult::OutputFull => {
                    let length = self.output.len();
                    self.output.extend(std::iter::repeat(0).take(length));
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let length = self.ends.len();
                    self.ends.extend(std::iter::repeat(0).take(length));
                }
                // Success cases
                csv_core::ReadRecordResult::Record | csv_core::ReadRecordResult::End => {
                    let result = {
                        let ends_valid = self.ends_cursor - 1;
                        if ends_valid == 0 {
                            break Ok(None);
                        }
                        if ends_valid != self.n_cols {
                            self.events_error += 1;
                            Err(DecodeErrorKind::Text(
                                format!(
                                    "CSV error at record number {}: expected {} columns, got {}.",
                                    self.total_events(),
                                    self.n_cols,
                                    ends_valid
                                )
                                .into(),
                            ))
                        } else {
                            // Validate each field's bytes as UTF-8 independently.
                            // Validating the whole record's output at once is
                            // wrong: the delimiter can split a byte sequence so
                            // that the concatenation is valid UTF-8 while an
                            // individual field boundary falls inside a multi-byte
                            // character (which panicked the `&str` slice), or so
                            // that bytes are silently merged across a field
                            // boundary. A field whose bytes are not valid UTF-8 on
                            // their own is a decode error.
                            // Pack each field straight into `row_buf` as it is
                            // validated rather than collecting into a temporary
                            // `Vec`. A UTF-8 error mid-record leaves a partial row
                            // behind, but that is harmless: the error path never
                            // reads `row_buf`, and the next record's `.packer()`
                            // clears it before packing.
                            let mut row_packer = self.row_buf.packer();
                            let mut utf8_error = None;
                            for i in 0..self.n_cols {
                                match std::str::from_utf8(
                                    &self.output[self.ends[i]..self.ends[i + 1]],
                                ) {
                                    Ok(field) => row_packer.push(Datum::String(field)),
                                    Err(e) => {
                                        utf8_error = Some(e);
                                        break;
                                    }
                                }
                            }
                            match utf8_error {
                                None => {
                                    self.events_success += 1;
                                    Ok(Some(self.row_buf.clone()))
                                }
                                Some(e) => {
                                    self.events_error += 1;
                                    Err(DecodeErrorKind::Text(
                                        format!(
                                            "CSV error at record number {}: invalid UTF-8 ({})",
                                            self.total_events(),
                                            e
                                        )
                                        .into(),
                                    ))
                                }
                            }
                        }
                    };

                    // Clear this record's scratch buffers now that it has been
                    // consumed, whether it decoded or errored. Doing this only on
                    // the success path (as the code used to) left stale field
                    // offsets behind after a malformed record — `csv_core` resets
                    // its own per-record output position while these cursors kept
                    // advancing — so the *next* record read back non-monotonic
                    // `ends`, panicking the `output[..]` slice (or silently
                    // producing corrupt rows). One bad row must not corrupt the
                    // rest of the source.
                    self.output_cursor = 0;
                    self.ends_cursor = 1;

                    // Header rows are validated but never sent into the dataflow;
                    // every other record is handed straight back to the caller.
                    if !self.next_row_is_header {
                        break result;
                    }
                    self.next_row_is_header = false;

                    let row = match result {
                        // Don't swallow a parse error just because it occurred on
                        // the header row.
                        Err(e) => break Err(e),
                        Ok(Some(row)) => row,
                        // An empty record (`ends_valid == 0`) already breaks out of
                        // the loop above, so a decoded record always carries a row.
                        Ok(None) => unreachable!("decoded record without a row"),
                    };

                    let mismatched = row
                        .iter()
                        .zip_eq(self.header_names.iter().flatten())
                        .enumerate()
                        .find(|(_, (actual, expected))| actual.unwrap_str() != &**expected);
                    if let Some((i, (actual, expected))) = mismatched {
                        break Err(DecodeErrorKind::Text(
                            format!(
                                "source file contains incorrect columns '{:?}', \
                             first mismatched column at index {} expected={} actual={}",
                                row,
                                i + 1,
                                expected,
                                actual
                            )
                            .into(),
                        ));
                    }

                    // Header looks good. If the chunk is exhausted we're done for
                    // now; otherwise loop around to decode the first data row.
                    if chunk.is_empty() {
                        break Ok(None);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::Datum;

    use super::{ColumnSpec, CsvDecoderState, CsvEncoding};

    /// A malformed record (wrong column count) must not corrupt the records that
    /// follow it: the decoder has to reset its scratch buffers after every
    /// record, not just successful ones. Previously the stale field offsets left
    /// behind by the error made the next record decode to a panic or a wrong row.
    #[mz_ore::test]
    fn test_csv_decoder_recovers_after_error() {
        let mut state = CsvDecoderState::new(CsvEncoding {
            columns: ColumnSpec::Count(2),
            delimiter: b',',
        });

        // One column where two are expected, then a well-formed two-column row.
        let mut input: &[u8] = b"oops\na,b\n";

        let first = state.decode(&mut input);
        assert!(
            first.is_err(),
            "expected a column-count error for the first record, got {first:?}"
        );

        let row = state
            .decode(&mut input)
            .expect("the second record must decode, not error/panic")
            .expect("the second record must yield a row");
        let datums: Vec<Datum> = row.iter().collect();
        assert_eq!(
            datums,
            vec![Datum::String("a"), Datum::String("b")],
            "the record after an error decoded to the wrong values",
        );
    }

    /// A delimiter can fall between two bytes that, with the delimiter removed,
    /// would form a single multi-byte UTF-8 character (here `0xdf 0x8d`). The
    /// whole record's bytes then look like valid UTF-8, but the individual field
    /// `0xdf` is not — validating per field must reject it as a decode error
    /// rather than panicking on a slice that falls inside the character.
    #[mz_ore::test]
    fn test_csv_decoder_rejects_field_split_inside_char() {
        let mut state = CsvDecoderState::new(CsvEncoding {
            columns: ColumnSpec::Count(2),
            delimiter: b'!',
        });

        // Fields "0xdf" and "0x8d 0x33"; `0xdf 0x8d` is a valid 2-byte char.
        let mut input: &[u8] = b"\xdf!\x8d3\n";
        let result = state.decode(&mut input);
        assert!(
            result.is_err(),
            "a field that is not valid UTF-8 on its own must be an error, got {result:?}"
        );

        // The decoder stays usable: a clean record after it still decodes.
        let mut input: &[u8] = b"a!b\n";
        let row = state
            .decode(&mut input)
            .expect("the following record must decode")
            .expect("the following record must yield a row");
        let datums: Vec<Datum> = row.iter().collect();
        assert_eq!(datums, vec![Datum::String("a"), Datum::String("b")]);
    }
}
