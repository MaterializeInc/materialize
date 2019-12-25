// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Cow;
use std::convert::TryFrom;
use std::sync::Arc;

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use lazy_static::lazy_static;

use dataflow_types::Update;
use repr::decimal::Decimal;
use repr::{ColumnName, ColumnType, Datum, Interval, RelationDesc, RelationType, Row, ScalarType};
use sql::{FieldFormat as SqlFieldFormat, TransactionStatus as SqlTransactionStatus};

use super::types::PgType;
use crate::codec::RawParameterBytes;

// Pgwire protocol versions are represented as 32-bit integers, where the
// high 16 bits represent the major version and the low 16 bits represent the
// minor version.
//
// There have only been three released protocol versions, v1.0, v2.0, and v3.0.
// The protocol changes very infrequently: the most recent protocol version,
// v3.0, was released with Postgres v7.4 in 2003.
//
// Somewhat unfortunately, the protocol overloads the version field to indicate
// special types of connections, namely, SSL connections and cancellation
// connections. These pseudo-versions were constructed to avoid ever matching
// a true protocol version.

pub const VERSION_1: i32 = 0x10000;
pub const VERSION_2: i32 = 0x20000;
pub const VERSION_3: i32 = 0x30000;
pub const VERSION_CANCEL: i32 = (1234 << 16) + 5678;
pub const VERSION_SSL: i32 = (1234 << 16) + 5679;
pub const VERSION_GSSENC: i32 = (1234 << 16) + 5680;

pub const VERSIONS: &[i32] = &[
    VERSION_1,
    VERSION_2,
    VERSION_3,
    VERSION_CANCEL,
    VERSION_SSL,
    VERSION_GSSENC,
];

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

/// A decoded frontend pgwire [message], representing instructions for the
/// backend.
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum FrontendMessage {
    /// Begin a connection.
    Startup { version: i32 },

    /// Request SSL encryption for the connection.
    SslRequest,

    /// Request GSSAPI encryption for the connection.
    GssEncRequest,

    /// Cancel a query that is running on another connection.
    CancelRequest {
        /// The target connection ID.
        conn_id: u32,
        /// The secret key for the target connection.
        secret_key: u32,
    },

    /// Execute the specified SQL.
    ///
    /// This is issued as part of the simple query flow.
    Query {
        /// The SQL to execute.
        sql: String,
    },

    /// Parse the specified SQL into a prepared statement.
    ///
    /// This starts the extended query flow.
    Parse {
        /// The name of the prepared statement to create. An empty string
        /// specifies the unnamed prepared statement.
        name: String,
        /// The SQL to parse.
        sql: String,
        /// The number of parameter data types specified. It can be zero.
        /// Note that this is not an indication of the number of parameters that
        /// might appear in the query string, but only the number that the
        /// frontend wants to prespecify types for.
        parameter_data_type_count: i16,
        /// The OID of each parameter data type. Placing a zero here is
        /// equivalent to leaving the type unspecified.
        parameter_data_types: Vec<i32>,
    },

    /// Describe an existing prepared statement.
    ///
    /// This command is part of the extended query flow.
    DescribeStatement {
        /// The name of the prepared statement to describe.
        name: String,
    },

    /// Describe an existing portal.
    ///
    /// This command is part of the extended query flow.
    DescribePortal {
        /// The name of the portal to describe.
        name: String,
    },

    /// Bind an existing prepared statement to a portal.
    ///
    /// Note that we can't actually bind parameters yet (issue#609), but that is
    /// an important part of this command.
    ///
    /// This command is part of the extended query flow.
    Bind {
        /// The destination portal. An empty string selects the unnamed
        /// portal. The portal can later be executed with the `Execute` command.
        portal_name: String,
        /// The source prepared statement. An empty string selects the unnamed
        /// prepared statement.
        statement_name: String,
        /// Struct holding the raw bytes representing parameter values passed
        /// from Postgres and their format codes
        raw_parameter_bytes: RawParameterBytes,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<FieldFormat>,
    },

    /// Execute a bound portal.
    ///
    /// This command is part of the extended query flow.
    Execute {
        /// The name of the portal to execute.
        portal_name: String,
        /// The maximum number number of rows to return before suspending.
        ///
        /// 0 or negative means infinite.
        max_rows: i32,
    },

    /// Finish an extended query.
    ///
    /// This command is part of the extended query flow.
    Sync,

    /// Close the named statement.
    ///
    /// This command is part of the extended query flow.
    CloseStatement { name: String },

    /// Close the named portal.
    ///
    // This command is part of the extended query flow.
    ClosePortal { name: String },

    /// Terminate a connection.
    Terminate,
}

impl FrontendMessage {
    pub fn name(&self) -> &'static str {
        match self {
            FrontendMessage::Startup { .. } => "startup",
            FrontendMessage::SslRequest => "ssl_request",
            FrontendMessage::GssEncRequest => "gssenc_request",
            FrontendMessage::CancelRequest { .. } => "cancel_request",
            FrontendMessage::Query { .. } => "query",
            FrontendMessage::Parse { .. } => "parse",
            FrontendMessage::DescribeStatement { .. } => "describe_statement",
            FrontendMessage::DescribePortal { .. } => "describe_portal",
            FrontendMessage::Bind { .. } => "bind",
            FrontendMessage::Execute { .. } => "execute",
            FrontendMessage::Sync => "sync",
            FrontendMessage::CloseStatement { .. } => "close_statement",
            FrontendMessage::ClosePortal { .. } => "close_portal",
            FrontendMessage::Terminate => "terminate",
        }
    }
}

#[derive(Debug)]
pub enum EncryptionType {
    None,
    Ssl,
    GssApi,
}

/// Internal representation of a backend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,
    EncryptionResponse(EncryptionType),
    ReadyForQuery(TransactionStatus),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<FieldValue>>, Arc<Vec<FieldFormat>>),
    ParameterStatus(&'static str, String),
    BackendKeyData {
        conn_id: u32,
        secret_key: u32,
    },
    ParameterDescription(Vec<ParameterDescription>),
    PortalSuspended,
    NoData,
    ParseComplete,
    BindComplete,
    CloseComplete,
    ErrorResponse {
        severity: Severity,
        code: &'static str,
        message: String,
        detail: Option<String>,
    },
    CopyOutResponse {
        overall_format: FieldFormat,
        column_formats: Vec<FieldFormat>,
    },
    CopyData(Vec<u8>),
    CopyDone,
}

/// A local representation of [`SqlTransactionStatus`]
#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    /// Not currently in a transaction
    Idle,
    /// Currently in a transaction
    InTransaction,
    /// Currently in a transaction block which is failed
    Failed,
}

impl From<SqlTransactionStatus> for TransactionStatus {
    /// Convert from the Session's version
    fn from(status: SqlTransactionStatus) -> TransactionStatus {
        match status {
            SqlTransactionStatus::Idle => TransactionStatus::Idle,
            SqlTransactionStatus::InTransaction => TransactionStatus::InTransaction,
            SqlTransactionStatus::Failed => TransactionStatus::Failed,
        }
    }
}

impl From<&SqlTransactionStatus> for TransactionStatus {
    /// Convert from the Session's version
    fn from(status: &SqlTransactionStatus) -> TransactionStatus {
        TransactionStatus::from(*status)
    }
}

#[derive(Debug)]
pub struct ParameterDescription {
    pub type_oid: u32,
}

impl From<&ScalarType> for ParameterDescription {
    fn from(typ: &ScalarType) -> Self {
        let pg_type: PgType = typ.into();
        ParameterDescription {
            type_oid: pg_type.oid,
        }
    }
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: ColumnName,
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

impl TryFrom<i16> for FieldFormat {
    type Error = failure::Error;

    fn try_from(source: i16) -> Result<FieldFormat, Self::Error> {
        match source {
            0 => Ok(FieldFormat::Text),
            1 => Ok(FieldFormat::Binary),
            _ => failure::bail!("Invalid FieldFormat source: {}", source),
        }
    }
}

impl From<&SqlFieldFormat> for FieldFormat {
    fn from(source: &SqlFieldFormat) -> FieldFormat {
        match source {
            SqlFieldFormat::Text => FieldFormat::Text,
            SqlFieldFormat::Binary => FieldFormat::Binary,
        }
    }
}

impl Into<SqlFieldFormat> for FieldFormat {
    fn into(self) -> SqlFieldFormat {
        match self {
            FieldFormat::Text => SqlFieldFormat::Text,
            FieldFormat::Binary => SqlFieldFormat::Binary,
        }
    }
}

lazy_static! {
    static ref EPOCH: NaiveDateTime = NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0);
    static ref EPOCH_NUM_DAYS_FROM_CE: i32 = EPOCH.num_days_from_ce();
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
    TimestampTz(DateTime<Utc>),
    Interval(Interval),
    Text(String),
    Numeric(Decimal),
    Jsonb(String),
}

impl FieldValue {
    pub fn from_datum(datum: Datum, typ: &ColumnType) -> Option<FieldValue> {
        if let Datum::Null = datum {
            None
        } else if let ScalarType::Jsonb = &typ.scalar_type {
            let string = expr::datum_to_serde(datum).to_string();
            Some(FieldValue::Jsonb(string))
        } else {
            Some(match datum {
                Datum::Null => unreachable!(),
                Datum::True => FieldValue::Bool(true),
                Datum::False => FieldValue::Bool(false),
                Datum::Int32(i) => FieldValue::Int4(i),
                Datum::Int64(i) => FieldValue::Int8(i),
                Datum::Float32(f) => FieldValue::Float4(*f),
                Datum::Float64(f) => FieldValue::Float8(*f),
                Datum::Date(d) => FieldValue::Date(d),
                Datum::Timestamp(d) => FieldValue::Timestamp(d),
                Datum::TimestampTz(d) => FieldValue::TimestampTz(d),
                Datum::Interval(i) => FieldValue::Interval(i),
                Datum::Decimal(d) => {
                    let (_, scale) = typ.scalar_type.unwrap_decimal_parts();
                    FieldValue::Numeric(d.with_scale(scale))
                }
                Datum::Bytes(b) => FieldValue::Bytea(b.to_vec()),
                Datum::String(s) => FieldValue::Text(s.to_owned()),
                Datum::JsonNull | Datum::List(_) | Datum::Dict(_) => {
                    panic!("Don't know how to serialize {}::{}", datum, typ)
                }
            })
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
            FieldValue::Timestamp(ts) => ts
                .format("%Y-%m-%d %H:%M:%S.%f")
                .to_string()
                .into_bytes()
                .into(),
            FieldValue::TimestampTz(ts) => ts
                .format("%Y-%m-%d %H:%M:%S.%f%:z")
                .to_string()
                .into_bytes()
                .into(),
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
            FieldValue::Text(s) => s.as_bytes().into(),
            FieldValue::Jsonb(s) => s.as_bytes().into(),
        }
    }

    /// Convert to the binary postgres wire format
    ///
    /// Some "docs" are at https://www.npgsql.org/dev/types.html
    pub(crate) fn to_binary(&self) -> Result<Cow<[u8]>, failure::Error> {
        Ok(match self {
            FieldValue::Bool(false) => [0u8][..].into(),
            FieldValue::Bool(true) => [1u8][..].into(),
            FieldValue::Bytea(b) => b.into(),
            // https://github.com/postgres/postgres/blob/59354ccef5d7/src/backend/utils/adt/date.c#L223
            FieldValue::Date(d) => {
                let day = d.num_days_from_ce() - *EPOCH_NUM_DAYS_FROM_CE;
                let mut buf = vec![0u8; 4];
                NetworkEndian::write_i32(&mut buf, day);
                buf.into()
            }
            FieldValue::Timestamp(ts) => {
                let timestamp = (ts.timestamp() - EPOCH.timestamp()) * 1_000_000
                    + i64::from(ts.timestamp_subsec_micros());
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, timestamp);
                buf.into()
            }
            FieldValue::TimestampTz(ts) => {
                let timestamp = (ts.timestamp() - EPOCH.timestamp()) * 1_000_000
                    + i64::from(ts.timestamp_subsec_micros());
                let mut buf = vec![0u8; 8];
                NetworkEndian::write_i64(&mut buf, timestamp);
                buf.into()
            }
            // https://github.com/postgres/postgres/blob/517bf2d9107f0d45c5fea2e3904e8d3b10ce6bb2/src/backend/utils/adt/timestamp.c#L1008
            // Postgres stores interval objects as a 16 byte memory blob split into 3 parts: 64 bits representing the interval in microseconds,
            // then 32 bits describing the interval in days, then 32 bits representing the interval in months
            // See also: https://github.com/diesel-rs/diesel/blob/a8b52bd05be202807e71579acf841735b6f1765e/diesel/src/pg/types/date_and_time/mod.rs#L39
            // for the Diesel implementation of the same logic
            FieldValue::Interval(i) => {
                let mut buf = Vec::with_capacity(16);
                match i {
                    repr::Interval::Months(n) => {
                        buf.write_i64::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(*n as i32)?;
                    }
                    repr::Interval::Duration { duration, .. } => {
                        buf.write_i64::<NetworkEndian>(duration.as_micros() as i64)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                        buf.write_i32::<NetworkEndian>(0)?;
                    }
                }
                buf.into()
            }
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
            FieldValue::Numeric(n) => {
                // This implementation is derived from Diesel.
                // https://github.com/diesel-rs/diesel/blob/bd13f24609c6893166aab2aaf92020bb5899f402/diesel/src/pg/types/numeric.rs
                let mut significand = n.significand();
                let scale = u16::from(n.scale());
                let non_neg = significand >= 0;
                significand = significand.abs();

                // Ensure that the significand will always lie on a digit boundary
                for _ in 0..(4 - scale % 4) {
                    significand *= 10;
                }

                let mut digits = vec![];
                while significand > 0 {
                    digits.push((significand % 10_000) as i16);
                    significand /= 10_000;
                }
                digits.reverse();
                let digits_after_decimal = scale / 4 + 1;
                let weight = digits.len() as i16 - digits_after_decimal as i16 - 1;

                let unnecessary_zeroes = if weight >= 0 {
                    let index_of_decimal = (weight + 1) as usize;
                    digits
                        .get(index_of_decimal..)
                        .expect("enough digits exist")
                        .iter()
                        .rev()
                        .take_while(|i| **i == 0)
                        .count()
                } else {
                    0
                };

                let relevant_digits = digits.len() - unnecessary_zeroes;
                digits.truncate(relevant_digits);

                let sign = if non_neg { 0 } else { 0x4000 };

                let mut buf = Vec::with_capacity(8 + 2 * digits.len());
                buf.write_u16::<NetworkEndian>(digits.len() as u16)?;
                buf.write_i16::<NetworkEndian>(weight)?;
                buf.write_u16::<NetworkEndian>(sign)?;
                buf.write_u16::<NetworkEndian>(scale)?;
                for digit in digits.iter() {
                    buf.write_i16::<NetworkEndian>(*digit)?;
                }
                buf.into()
            }
            FieldValue::Text(ref s) => s.as_bytes().into(),
            FieldValue::Jsonb(s) => {
                // https://github.com/postgres/postgres/blob/14aec03502302eff6c67981d8fd121175c436ce9/src/backend/utils/adt/jsonb.c#L148
                let version = 1;
                let mut buf: Vec<u8> = vec![version];
                buf.extend_from_slice(s.as_bytes());
                buf.into()
            }
        })
    }
}

pub fn encode_update(update: Update, typ: &RelationType) -> Vec<u8> {
    let mut out = Vec::new();
    for field in field_values_from_row(update.row, typ) {
        match field {
            None => out.extend(b"\\N"),
            Some(field) => {
                for b in &*field.to_text() {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        b => out.push(*b),
                    }
                }
            }
        }
        out.push(b'\t');
    }
    out.extend(format!("Diff: {} at {}\n", update.diff, update.timestamp).bytes());
    out
}

pub fn field_values_from_row(row: Row, typ: &RelationType) -> Vec<Option<FieldValue>> {
    row.iter()
        .zip(typ.column_types.iter())
        .map(|(col, typ)| FieldValue::from_datum(col, typ))
        .collect()
}

pub fn row_description_from_desc(desc: &RelationDesc) -> Vec<FieldDescription> {
    desc.iter()
        .map(|(name, typ)| {
            let pg_type: PgType = (&typ.scalar_type).into();
            FieldDescription {
                name: name.cloned().unwrap_or_else(|| "?column?".into()),
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
