// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use super::types::PgType;
use repr::decimal::Decimal;
use repr::{ColumnType, Datum, Interval, RelationType, ScalarType};

#[allow(dead_code)]
#[derive(Debug)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn string(&self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        }
    }
}

/// A parsed frontend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum FrontendMessage {
    Startup {
        version: u32,
    },
    Query {
        query: Bytes,
    },
    /// Start an extended query
    Parse {
        name: String,
        sql: String,
        parameter_data_type_count: u16,
        parameter_data_types: Vec<u32>,
    },
    /// Describe a statement by name during an extended query
    DescribeStatement {
        name: String,
    },
    /// Connect the Prepared statement from `Parse` to a `Portal`
    ///
    /// Note that we can't actually bind parameters yet (issue#609), but that is an
    /// important part of this command.
    Bind {
        /// The portal being bound to
        ///
        /// All `Bind' commands are followed by an execute, which just names this portal
        portal_name: String,
        statement_name: String,
        /// The format of each field, if the field is empty then it should be Text
        return_field_formats: Vec<FieldFormat>,
    },

    /// Execute a bound portal
    Execute {
        portal_name: String,
    },

    /// Finish an extended query
    Sync,

    /// Terminate a connection
    Terminate,
}

/// Internal representation of a backend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[allow(dead_code)]
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,
    ReadyForQuery,
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<FieldValue>>, FieldFormatIter),
    ParameterStatus(&'static str, String),
    ParameterDescription,
    ParseComplete,
    BindComplete,
    ErrorResponse {
        severity: Severity,
        code: &'static str,
        message: String,
        detail: Option<String>,
    },
    CopyOutResponse,
    CopyData(Vec<u8>),
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: String,
    pub table_id: u32,
    pub column_id: u16,
    pub type_oid: u32,
    pub type_len: i16,
    // https://github.com/cockroachdb/cockroach/blob/3e8553e249a842e206aa9f4f8be416b896201f10/pkg/sql/pgwire/conn.go#L1115-L1123
    pub type_mod: i32,
    pub format: FieldFormat,
}

/// A postgres input or output format
///
/// https://www.postgresql.org/docs/11/protocol-overview.html#PROTOCOL-FORMAT-CODES
#[derive(Copy, Clone, Debug)]
pub enum FieldFormat {
    /// Text encoding, the default
    ///
    /// From the docs:
    /// > The text representation of values is whatever strings are produced and accepted
    /// > by the input/output conversion functions for the particular data type. In the
    /// > transmitted representation, there is no trailing null character; the frontend
    /// > must add one to received values if it wants to process them as C strings. (The
    /// > text format does not allow embedded nulls, by the way.)
    Text = 0,
    /// Binary encoding
    ///
    /// From the docs:
    /// > Binary representations for integers use network byte order (most significant byte
    /// > first). For other data types consult the documentation or source code to learn about
    /// > the binary representation. Keep in mind that binary representations for complex data
    /// > types might change across server versions; the text format is usually the more
    /// > portable choice.
    Binary = 1,
}

impl TryFrom<u16> for FieldFormat {
    type Error = failure::Error;

    fn try_from(source: u16) -> Result<FieldFormat, Self::Error> {
        match source {
            0 => Ok(FieldFormat::Text),
            1 => Ok(FieldFormat::Binary),
            _ => failure::bail!("Invalid FieldFormat source: {}", source),
        }
    }
}

impl From<&FieldFormat> for bool {
    fn from(source: &FieldFormat) -> bool {
        match source {
            FieldFormat::Text => false,
            FieldFormat::Binary => true,
        }
    }
}

impl From<&bool> for FieldFormat {
    fn from(source: &bool) -> FieldFormat {
        match source {
            false => FieldFormat::Text,
            true => FieldFormat::Binary,
        }
    }
}

/// Retrieve all the [`FieldFormat`]s, repeatably
///
/// Any extended query can request that individual fields come back encoded either as
/// text or binary.
///
/// This implements the following rules:
///
/// * Default is `Text` if no formats are specified
/// * If a single field is provided then that is used for every column
/// * Otherwise use the specified fields
/// * Returns Text and logs a warning if we ever go past the end of the
///   client-provided format list
#[derive(Debug)]
pub struct FieldFormatIter {
    formats: Option<Arc<Vec<FieldFormat>>>,
    idx: usize,
}

impl FieldFormatIter {
    pub fn new(formats: Option<Arc<Vec<FieldFormat>>>) -> FieldFormatIter {
        FieldFormatIter { formats, idx: 0 }
    }

    /// Get a fresh iterator over the same values
    pub fn fresh(&self) -> FieldFormatIter {
        FieldFormatIter::new(self.formats.as_ref().map(Arc::clone))
    }
}

impl Iterator for FieldFormatIter {
    type Item = FieldFormat;
    fn next(&mut self) -> Option<Self::Item> {
        Some(match &mut self.formats {
            Some(values) if values.len() == 1 => values[0],
            Some(values) => {
                self.idx += 1;
                values.get(self.idx - 1).copied().unwrap_or_else(|| {
                    // It's unclear what the default should be here, if this actually
                    // comes up maybe we should return an error
                    log::warn!("requested a FieldFormat that was not specified, returning Text");
                    FieldFormat::Text
                })
            }
            None => FieldFormat::Text,
        })
    }
}

/// PGWire-specific representations of Datums
#[derive(Debug)]
pub enum FieldValue {
    Bool(bool),
    Bytea(Vec<u8>),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Interval(Interval),
    Text(String),
    Numeric(Decimal),
}

impl FieldValue {
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<FieldValue> {
        match datum {
            Datum::Null => None,
            Datum::True => Some(FieldValue::Bool(true)),
            Datum::False => Some(FieldValue::Bool(false)),
            Datum::Int32(i) => Some(FieldValue::Int4(i)),
            Datum::Int64(i) => Some(FieldValue::Int8(i)),
            Datum::Float32(f) => Some(FieldValue::Float4(*f)),
            Datum::Float64(f) => Some(FieldValue::Float8(*f)),
            Datum::Date(d) => Some(FieldValue::Date(d)),
            Datum::Timestamp(d) => Some(FieldValue::Timestamp(d)),
            Datum::Interval(i) => Some(FieldValue::Interval(i)),
            Datum::Decimal(d) => {
                let (_, scale) = typ.scalar_type.unwrap_decimal_parts();
                Some(FieldValue::Numeric(d.with_scale(scale)))
            }
            Datum::Bytes(b) => Some(FieldValue::Bytea(b)),
            Datum::String(s) => Some(FieldValue::Text(s)),
            Datum::Regex(_) => panic!("Datum::Regex cannot be converted into a FieldValue"),
        }
    }

    /// Errors on some types:
    ///
    /// * Numeric
    /// * Interval
    pub(crate) fn to_text(&self) -> Cow<[u8]> {
        match self {
            FieldValue::Bool(false) => b"f"[..].into(),
            FieldValue::Bool(true) => b"t"[..].into(),
            FieldValue::Bytea(b) => b.into(),
            FieldValue::Date(d) => d.to_string().into_bytes().into(),
            FieldValue::Timestamp(ts) => ts.to_string().into_bytes().into(),
            FieldValue::Interval(i) => match i {
                repr::Interval::Months(count) => format!("{} months", count).into_bytes().into(),
                repr::Interval::Duration {
                    is_positive,
                    duration,
                } => format!("{}{:?}", if *is_positive { "" } else { "-" }, duration)
                    .into_bytes()
                    .into(),
            },
            FieldValue::Int4(i) => format!("{}", i).into_bytes().into(),
            FieldValue::Int8(i) => format!("{}", i).into_bytes().into(),
            FieldValue::Float4(f) => format!("{}", f).into_bytes().into(),
            FieldValue::Float8(f) => format!("{}", f).into_bytes().into(),
            FieldValue::Numeric(n) => format!("{}", n).into_bytes().into(),
            FieldValue::Text(ref s) => s.as_bytes().into(),
        }
    }

    /// Convert to the binary postgres wire format
    ///
    /// Some "docs" are at https://www.npgsql.org/dev/types.html
    pub(crate) fn to_binary(&self) -> Result<Cow<[u8]>, failure::Error> {
        use byteorder::{ByteOrder, NetworkEndian};

        Ok(match self {
            FieldValue::Bool(false) => [0u8][..].into(),
            FieldValue::Bool(true) => [1u8][..].into(),
            FieldValue::Bytea(b) => b.into(),
            // https://github.com/postgres/postgres/blob/59354ccef5d7/src/backend/utils/adt/date.c#L223
            FieldValue::Date(d) => {
                let day = d.num_days_from_ce() - 719_163;
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_i32(&mut buf, day);
                buf.into()
            }
            FieldValue::Timestamp(ts) => {
                let timestamp =
                    ts.timestamp() * 1_000_000 + i64::from(ts.timestamp_subsec_micros());

                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, timestamp);
                buf.into()
            }
            FieldValue::Interval(_i) => failure::bail!("cannot serialize binary: interval"),
            FieldValue::Int4(i) => {
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_i32(&mut buf, *i);
                buf.into()
            }
            FieldValue::Int8(i) => {
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, *i);
                buf.into()
            }
            FieldValue::Float4(f) => {
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_f32(&mut buf, *f);
                buf.into()
            }
            FieldValue::Float8(f) => {
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_f64(&mut buf, *f);
                buf.into()
            }
            // https://github.com/postgres/postgres/blob/59354ccef5/src/backend/utils/adt/numeric.c#L868-L891
            FieldValue::Numeric(_n) => failure::bail!("cannot serialize binary: numeric"),
            FieldValue::Text(ref s) => s.as_bytes().into(),
        })
    }
}

pub fn field_values_from_row(row: Vec<Datum>, typ: &RelationType) -> Vec<Option<FieldValue>> {
    row.into_iter()
        .zip(typ.column_types.iter())
        .map(|(col, typ)| FieldValue::from_datum(col, typ))
        .collect()
}

pub fn row_description_from_type(typ: &RelationType) -> Vec<FieldDescription> {
    typ.column_types
        .iter()
        .map(|typ| {
            let pg_type: PgType = (&typ.scalar_type).into();
            FieldDescription {
                name: typ.name.as_ref().unwrap_or(&"?column?".into()).to_owned(),
                table_id: 0,
                column_id: 0,
                type_oid: pg_type.oid,
                type_len: pg_type.typlen,
                type_mod: match &typ.scalar_type {
                    // NUMERIC types pack their precision and size into the
                    // type_mod field. The high order bits store the precision
                    // while the low order bits store the scale + 4 (!).
                    //
                    // https://github.com/postgres/postgres/blob/e435c1e7d/src/backend/utils/adt/numeric.c#L6364-L6367
                    ScalarType::Decimal(precision, scale) => {
                        ((i32::from(*precision) << 16) | i32::from(*scale)) + 4
                    }
                    _ => -1,
                },
                format: FieldFormat::Text,
            }
        })
        .collect()
}
