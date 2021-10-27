// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::io;

use bytes::BytesMut;
use coord::{CoordError, StartupMessage};
use itertools::Itertools;
use postgres::error::SqlState;

use coord::session::TransactionStatus as CoordTransactionStatus;
use repr::adt::numeric::NUMERIC_DATUM_MAX_PRECISION;
use repr::{
    ColumnName, Datum, NotNullViolation, RelationDesc, RelationType, Row, RowArena, ScalarType,
};

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

/// Like [`FrontendMessage`], but only the messages that can occur during
/// startup protocol negotiation.
#[derive(Debug)]
pub enum FrontendStartupMessage {
    /// Begin a connection.
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },

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
}

/// A decoded frontend pgwire [message], representing instructions for the
/// backend.
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum FrontendMessage {
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
        /// The OID of each parameter data type for which the client wants to
        /// prespecify types. A zero OID is equivalent to leaving the type
        /// unspecified.
        ///
        /// The number of specified parameter data types can be less than the
        /// number of parameters specified in the query.
        param_types: Vec<u32>,
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
        /// The formats used to encode the parameters in `raw_parameters`.
        param_formats: Vec<pgrepr::Format>,
        /// The value of each parameter, encoded using the formats described
        /// by `parameter_formats`.
        raw_params: Vec<Option<Vec<u8>>>,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<pgrepr::Format>,
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

    /// Flush any pending output.
    ///
    /// This command is part of the extended query flow.
    Flush,

    /// Finish an extended query.
    ///
    /// This command is part of the extended query flow.
    Sync,

    /// Close the named statement.
    ///
    /// This command is part of the extended query flow.
    CloseStatement {
        name: String,
    },

    /// Close the named portal.
    ///
    // This command is part of the extended query flow.
    ClosePortal {
        name: String,
    },

    /// Terminate a connection.
    Terminate,

    CopyData(Vec<u8>),

    CopyDone,

    CopyFail(String),
}

impl FrontendMessage {
    pub fn name(&self) -> &'static str {
        match self {
            FrontendMessage::Query { .. } => "query",
            FrontendMessage::Parse { .. } => "parse",
            FrontendMessage::DescribeStatement { .. } => "describe_statement",
            FrontendMessage::DescribePortal { .. } => "describe_portal",
            FrontendMessage::Bind { .. } => "bind",
            FrontendMessage::Execute { .. } => "execute",
            FrontendMessage::Flush => "flush",
            FrontendMessage::Sync => "sync",
            FrontendMessage::CloseStatement { .. } => "close_statement",
            FrontendMessage::ClosePortal { .. } => "close_portal",
            FrontendMessage::Terminate => "terminate",
            FrontendMessage::CopyData(_) => "copy_data",
            FrontendMessage::CopyDone => "copy_done",
            FrontendMessage::CopyFail(_) => "copy_fail",
        }
    }
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
    ReadyForQuery(TransactionStatus),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<pgrepr::Value>>),
    ParameterStatus(&'static str, String),
    BackendKeyData {
        conn_id: u32,
        secret_key: u32,
    },
    ParameterDescription(Vec<pgrepr::Type>),
    PortalSuspended,
    NoData,
    ParseComplete,
    BindComplete,
    CloseComplete,
    ErrorResponse(ErrorResponse),
    CopyInResponse {
        overall_format: pgrepr::Format,
        column_formats: Vec<pgrepr::Format>,
    },
    CopyOutResponse {
        overall_format: pgrepr::Format,
        column_formats: Vec<pgrepr::Format>,
    },
    CopyData(Vec<u8>),
    CopyDone,
}

impl From<ErrorResponse> for BackendMessage {
    fn from(err: ErrorResponse) -> BackendMessage {
        BackendMessage::ErrorResponse(err)
    }
}

/// A local representation of [`CoordTransactionStatus`]
#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    /// Not currently in a transaction
    Idle,
    /// Currently in a transaction
    InTransaction,
    /// Currently in a transaction block which is failed
    Failed,
}

impl From<&CoordTransactionStatus> for TransactionStatus {
    /// Convert from the Session's version
    fn from(status: &CoordTransactionStatus) -> TransactionStatus {
        match status {
            CoordTransactionStatus::Default => TransactionStatus::Idle,
            CoordTransactionStatus::Started(_) => TransactionStatus::InTransaction,
            CoordTransactionStatus::InTransaction(_) => TransactionStatus::InTransaction,
            CoordTransactionStatus::InTransactionImplicit(_) => TransactionStatus::InTransaction,
            CoordTransactionStatus::Failed(_) => TransactionStatus::Failed,
        }
    }
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
}

impl ErrorResponse {
    pub fn fatal<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Fatal, code, message)
    }

    pub fn error<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Error, code, message)
    }

    pub fn notice<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Notice, code, message)
    }

    pub fn warning<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Warning, code, message)
    }

    fn new<S>(severity: Severity, code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse {
            severity,
            code,
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
        }
    }

    pub fn from_coord(severity: Severity, e: CoordError) -> ErrorResponse {
        // TODO(benesch): we should only use `SqlState::INTERNAL_ERROR` for
        // those errors that are truly internal errors. At the moment we have
        // a various classes of uncategorized errors that use this error code
        // inappropriately.
        let code = match e {
            CoordError::InvalidAlterOnDisabledIndex(_) => SqlState::INTERNAL_ERROR,
            CoordError::Catalog(_) => SqlState::INTERNAL_ERROR,
            CoordError::ChangedPlan => SqlState::FEATURE_NOT_SUPPORTED,
            CoordError::ConstrainedParameter(_) => SqlState::INVALID_PARAMETER_VALUE,
            CoordError::AutomaticTimestampFailure { .. } => SqlState::INTERNAL_ERROR,
            CoordError::DuplicateCursor(_) => SqlState::DUPLICATE_CURSOR,
            CoordError::Eval(_) => SqlState::INTERNAL_ERROR,
            CoordError::IdExhaustionError => SqlState::INTERNAL_ERROR,
            CoordError::IncompleteTimestamp(_) => SqlState::SQL_STATEMENT_NOT_YET_COMPLETE,
            CoordError::InvalidParameterType(_) => SqlState::INVALID_PARAMETER_VALUE,
            CoordError::InvalidParameterValue { .. } => SqlState::INVALID_PARAMETER_VALUE,
            CoordError::InvalidTableMutationSelection => SqlState::INVALID_TRANSACTION_STATE,
            CoordError::ConstraintViolation(NotNullViolation(_)) => SqlState::NOT_NULL_VIOLATION,
            CoordError::OperationProhibitsTransaction(_) => SqlState::ACTIVE_SQL_TRANSACTION,
            CoordError::OperationRequiresTransaction(_) => SqlState::NO_ACTIVE_SQL_TRANSACTION,
            CoordError::PreparedStatementExists(_) => SqlState::DUPLICATE_PSTATEMENT,
            CoordError::ReadOnlyTransaction => SqlState::READ_ONLY_SQL_TRANSACTION,
            CoordError::ReadOnlyParameter(_) => SqlState::CANT_CHANGE_RUNTIME_PARAM,
            CoordError::RelationOutsideTimeDomain { .. } => SqlState::INVALID_TRANSACTION_STATE,
            CoordError::SafeModeViolation(_) => SqlState::INTERNAL_ERROR,
            CoordError::SqlCatalog(_) => SqlState::INTERNAL_ERROR,
            CoordError::TailOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            CoordError::Transform(_) => SqlState::INTERNAL_ERROR,
            CoordError::UnknownCursor(_) => SqlState::INVALID_CURSOR_NAME,
            CoordError::UnknownParameter(_) => SqlState::INVALID_SQL_STATEMENT_NAME,
            CoordError::UnknownPreparedStatement(_) => SqlState::UNDEFINED_PSTATEMENT,
            CoordError::UnknownLoginRole(_) => SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
            CoordError::Unsupported(..) => SqlState::FEATURE_NOT_SUPPORTED,
            CoordError::Unstructured(_) => SqlState::INTERNAL_ERROR,
            // It's not immediately clear which error code to use here because a
            // "write-only transaction" is not a thing in Postgres. This error
            // code is the generic "bad txn thing" code, so it's probably the
            // best choice.
            CoordError::WriteOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
        };
        ErrorResponse {
            severity,
            code,
            message: e.to_string(),
            detail: e.detail(),
            hint: e.hint(),
            position: None,
        }
    }

    pub fn from_startup_message(message: StartupMessage) -> ErrorResponse {
        ErrorResponse {
            severity: Severity::Notice,
            code: SqlState::SUCCESSFUL_COMPLETION,
            message: message.to_string(),
            detail: message.detail(),
            hint: message.hint(),
            position: None,
        }
    }

    pub fn with_position(mut self, position: usize) -> ErrorResponse {
        self.position = Some(position);
        self
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Severity {
    Panic,
    Fatal,
    Error,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn is_error(&self) -> bool {
        matches!(self, Severity::Panic | Severity::Fatal | Severity::Error)
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, Severity::Fatal)
    }

    pub fn as_str(&self) -> &'static str {
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

#[derive(Debug)]
pub struct FieldDescription {
    pub name: ColumnName,
    pub table_id: u32,
    pub column_id: u16,
    pub type_oid: u32,
    pub type_len: i16,
    // https://github.com/cockroachdb/cockroach/blob/3e8553e249a842e206aa9f4f8be416b896201f10/pkg/sql/pgwire/conn.go#L1115-L1123
    pub type_mod: i32,
    pub format: pgrepr::Format,
}

pub fn encode_copy_row_binary(
    row: Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    const NULL_BYTES: [u8; 4] = (-1i32).to_be_bytes();

    // 16-bit int of number of tuples.
    let count = i16::try_from(typ.column_types.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::Other,
            "column count does not fit into an i16",
        )
    })?;

    out.extend(&count.to_be_bytes());
    let mut buf = BytesMut::new();
    for (field, typ) in row
        .iter()
        .zip(&typ.column_types)
        .map(|(datum, typ)| (pgrepr::Value::from_datum(datum, &typ.scalar_type), typ))
    {
        match field {
            None => out.extend(&NULL_BYTES),
            Some(field) => {
                buf.clear();
                field.encode_binary(&pgrepr::Type::from(&typ.scalar_type), &mut buf)?;
                out.extend(
                    &i32::try_from(buf.len())
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                "field length does not fit into an i32",
                            )
                        })?
                        .to_be_bytes(),
                );
                out.extend(&buf);
            }
        }
    }
    Ok(())
}

pub fn encode_copy_row_text(
    row: Row,
    typ: &RelationType,
    out: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let delim = b'\t';
    let null = b"\\N";
    let mut buf = BytesMut::new();
    for (idx, field) in pgrepr::values_from_row(row, typ).into_iter().enumerate() {
        if idx > 0 {
            out.push(delim);
        }
        match field {
            None => out.extend(null),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                for b in &buf {
                    match b {
                        b'\\' => out.extend(b"\\\\"),
                        b'\n' => out.extend(b"\\n"),
                        b'\r' => out.extend(b"\\r"),
                        b'\t' => out.extend(b"\\t"),
                        _ => out.push(*b),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    Ok(())
}

struct CopyTextFormatParser<'a> {
    data: &'a [u8],
    position: usize,
    column_delimiter: &'a str,
    null_string: &'a str,
    buffer: Vec<u8>,
}

impl<'a> CopyTextFormatParser<'a> {
    fn new(data: &'a [u8], column_delimiter: &'a str, null_string: &'a str) -> Self {
        Self {
            data,
            position: 0,
            column_delimiter,
            null_string,
            buffer: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.position < self.data.len() {
            Some(self.data[self.position])
        } else {
            None
        }
    }

    fn consume_n(&mut self, n: usize) {
        self.position = std::cmp::min(self.position + n, self.data.len());
    }

    fn is_eof(&self) -> bool {
        self.peek().is_none() || self.is_end_of_copy_marker()
    }

    fn end_of_copy_marker() -> &'static [u8] {
        "\\.".as_bytes()
    }

    fn is_end_of_copy_marker(&self) -> bool {
        self.check_bytes(Self::end_of_copy_marker())
    }

    /// Verifies there is no extra data after the end-of-copy marker.
    fn expect_no_junk_data(&mut self) -> Result<(), io::Error> {
        if self.is_end_of_copy_marker() {
            self.consume_bytes(Self::end_of_copy_marker());
            if self.is_end_of_line() {
                self.consume_n(1);
            }
            if self.peek().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "end-of-copy marker corrupt",
                ));
            }
        }
        Ok(())
    }

    fn is_end_of_line(&self) -> bool {
        match self.peek() {
            Some(b'\n') | None => true,
            _ => false,
        }
    }

    fn expect_end_of_line(&mut self) -> Result<(), io::Error> {
        if self.is_end_of_line() {
            self.consume_n(1);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            ))
        }
    }

    fn is_column_delimiter(&self) -> bool {
        self.check_bytes(self.column_delimiter.as_bytes())
    }

    fn expect_column_delimiter(&mut self) -> Result<(), io::Error> {
        if self.consume_bytes(self.column_delimiter.as_bytes()) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            ))
        }
    }

    fn check_bytes(&self, bytes: &[u8]) -> bool {
        let remaining_bytes = self.data.len() - self.position;
        remaining_bytes >= bytes.len()
            && self.data[self.position..]
                .iter()
                .zip(bytes.iter())
                .all(|(x, y)| x == y)
    }

    fn consume_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.check_bytes(bytes) {
            self.consume_n(bytes.len());
            true
        } else {
            false
        }
    }

    fn consume_null_string(&mut self) -> bool {
        if self.null_string.is_empty() {
            // An empty NULL marker is supported. Look ahead to ensure that is followed by
            // a column delimiter, an end of line or it is at the end of the data.
            self.is_column_delimiter()
                || self.is_end_of_line()
                || self.is_end_of_copy_marker()
                || self.is_eof()
        } else {
            self.consume_bytes(self.null_string.as_bytes())
        }
    }

    fn consume_raw_value(&mut self) -> Result<Option<&[u8]>, io::Error> {
        if self.consume_null_string() {
            return Ok(None);
        }

        let mut start = self.position;

        // buffer where unescaped data is accumulated
        self.buffer.clear();

        while !self.is_eof() && !self.is_end_of_copy_marker() {
            if self.is_end_of_line() || self.is_column_delimiter() {
                break;
            }
            match self.peek() {
                Some(b'\\') => {
                    // Add non-escaped data parsed so far
                    self.buffer.extend(&self.data[start..self.position]);

                    self.consume_n(1);
                    match self.peek() {
                        Some(b'b') => {
                            self.consume_n(1);
                            self.buffer.push(8);
                        }
                        Some(b'f') => {
                            self.consume_n(1);
                            self.buffer.push(12);
                        }
                        Some(b'n') => {
                            self.consume_n(1);
                            self.buffer.push(b'\n');
                        }
                        Some(b'r') => {
                            self.consume_n(1);
                            self.buffer.push(b'\r');
                        }
                        Some(b't') => {
                            self.consume_n(1);
                            self.buffer.push(b'\t');
                        }
                        Some(b'v') => {
                            self.consume_n(1);
                            self.buffer.push(11);
                        }
                        Some(b'x') => {
                            self.consume_n(1);
                            match self.peek() {
                                Some(_c @ b'0'..=b'9')
                                | Some(_c @ b'A'..=b'F')
                                | Some(_c @ b'a'..=b'f') => {
                                    let mut value: u8 = 0;
                                    let decode_nibble = |b| match b {
                                        Some(c @ b'a'..=b'f') => Some(c - b'a' + 10),
                                        Some(c @ b'A'..=b'F') => Some(c - b'A' + 10),
                                        Some(c @ b'0'..=b'9') => Some(c - b'0'),
                                        _ => None,
                                    };
                                    for _ in 0..2 {
                                        match decode_nibble(self.peek()) {
                                            Some(c) => {
                                                self.consume_n(1);
                                                value = value << 4 | c;
                                            }
                                            _ => break,
                                        }
                                    }
                                    self.buffer.push(value);
                                }
                                _ => {
                                    self.buffer.push(b'x');
                                }
                            }
                        }
                        Some(_c @ b'0'..=b'7') => {
                            let mut value: u8 = 0;
                            for _ in 0..3 {
                                match self.peek() {
                                    Some(c @ b'0'..=b'7') => {
                                        self.consume_n(1);
                                        value = value << 3 | (c - b'0');
                                    }
                                    _ => break,
                                }
                            }
                            self.buffer.push(value);
                        }
                        Some(c) => {
                            self.consume_n(1);
                            self.buffer.push(c);
                        }
                        None => {
                            self.buffer.push(b'\\');
                        }
                    }

                    start = self.position;
                }
                Some(_) => {
                    self.consume_n(1);
                }
                None => {}
            }
        }

        // Return a slice of the original buffer if no escaped characters where processed
        if self.buffer.is_empty() {
            Ok(Some(&self.data[start..self.position]))
        } else {
            // ... otherwise, add the remaining non-escaped data to the decoding buffer
            // and return a pointer to it
            self.buffer.extend(&self.data[start..self.position]);
            Ok(Some(&self.buffer[..]))
        }
    }
}

pub fn decode_copy_text_format(
    data: &[u8],
    column_types: &Vec<pgrepr::Type>,
    delimiter: &Option<String>,
    null: &Option<String>,
) -> Result<Vec<Row>, io::Error> {
    let mut rows = Vec::new();
    let null = if let Some(null) = null {
        null.as_str()
    } else {
        "\\N"
    };
    let delimiter = if let Some(delimiter) = delimiter {
        delimiter.as_str()
    } else {
        "\t"
    };
    let mut parser = CopyTextFormatParser::new(data, delimiter, null);
    while !parser.is_eof() && !parser.is_end_of_copy_marker() {
        let mut row = Vec::new();
        let buf = RowArena::new();
        for (col, typ) in column_types.iter().enumerate() {
            if col > 0 {
                parser.expect_column_delimiter()?;
            }
            let raw_value = parser.consume_raw_value()?;
            if let Some(raw_value) = raw_value {
                match pgrepr::Value::decode_text(&typ, raw_value) {
                    Ok(value) => row.push(value.into_datum(&buf, &typ).0),
                    Err(err) => {
                        let msg = format!("unable to decode column: {}", err);
                        return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                    }
                }
            } else {
                row.push(Datum::Null);
            }
        }
        parser.expect_end_of_line()?;
        rows.push(Row::pack(row));
    }
    parser.expect_no_junk_data()?;
    Ok(rows)
}

pub fn encode_row_description(
    desc: &RelationDesc,
    formats: &[pgrepr::Format],
) -> Vec<FieldDescription> {
    desc.iter()
        .zip_eq(formats)
        .map(|((name, typ), format)| {
            let pg_type = pgrepr::Type::from(&typ.scalar_type);
            FieldDescription {
                name: name.cloned().unwrap_or_else(|| "?column?".into()),
                table_id: 0,
                column_id: 0,
                type_oid: pg_type.oid(),
                type_len: pg_type.typlen(),
                type_mod: match &typ.scalar_type {
                    // NUMERIC types pack their precision and size into the
                    // type_mod field. The high order bits store the precision
                    // while the low order bits store the scale + 4 (!).
                    //
                    // https://github.com/postgres/postgres/blob/e435c1e7d/src/backend/utils/adt/numeric.c#L6364-L6367
                    ScalarType::Numeric { scale } => match scale {
                        // -1 represents default typemod, which allows values of differing scales
                        None => -1i32,
                        Some(scale) => {
                            ((i32::try_from(NUMERIC_DATUM_MAX_PRECISION).unwrap() << 16)
                                | i32::from(*scale))
                                + 4
                        }
                    },
                    _ => -1,
                },
                format: *format,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_copy_format_text_parser() {
        let text = "\t\\nt e\t\\N\t\n\\x60\\xA\\x7D\\x4a\n\\44\\044\\123".as_bytes();
        let mut parser = CopyTextFormatParser::new(text, "\t", "\\N");
        assert!(parser.is_column_delimiter());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "\nt e".as_bytes()
        );
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        // null value
        assert!(parser
            .consume_raw_value()
            .expect("unexpected error")
            .is_none());
        parser
            .expect_column_delimiter()
            .expect("expected column delimiter");
        assert!(parser.is_end_of_line());
        parser.expect_end_of_line().expect("expected eol");
        // hex value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "`\n}J".as_bytes()
        );
        parser.expect_end_of_line().expect("expected eol");
        // octal value
        assert_eq!(
            parser
                .consume_raw_value()
                .expect("unexpected error")
                .expect("unexpected empty result"),
            "$$S".as_bytes()
        );
        assert!(parser.is_eof());
    }

    #[test]
    fn test_copy_format_text_empty_null_string() {
        let text = "\t\n10\t20\n30\t\n40\t".as_bytes();
        let expect = vec![
            vec![None, None],
            vec![Some("10"), Some("20")],
            vec![Some("30"), None],
            vec![Some("40"), None],
        ];
        let mut parser = CopyTextFormatParser::new(text, "\t", "");
        for line in expect {
            for (i, value) in line.iter().enumerate() {
                if i > 0 {
                    parser
                        .expect_column_delimiter()
                        .expect("expected column delimiter");
                }
                match value {
                    Some(s) => {
                        assert!(!parser.consume_null_string());
                        assert_eq!(
                            parser
                                .consume_raw_value()
                                .expect("unexpected error")
                                .expect("unexpected empty result"),
                            s.as_bytes()
                        );
                    }
                    None => {
                        assert!(parser.consume_null_string());
                    }
                }
            }
            parser.expect_end_of_line().expect("expected eol");
        }
        parser.expect_no_junk_data().expect("expected data end");
    }

    #[test]
    fn test_copy_format_text_parser_escapes() {
        struct TestCase {
            input: &'static str,
            expect: &'static [u8],
        }
        let tests = vec![
            TestCase {
                input: "simple",
                expect: b"simple",
            },
            TestCase {
                input: r#"new\nline"#,
                expect: b"new\nline",
            },
            TestCase {
                input: r#"\b\f\n\r\t\v\\"#,
                expect: b"\x08\x0c\n\r\t\x0b\\",
            },
            TestCase {
                input: r#"\0\12\123"#,
                expect: &[0, 0o12, 0o123],
            },
            TestCase {
                input: r#"\x1\xaf"#,
                expect: &[0x01, 0xaf],
            },
            TestCase {
                input: r#"T\n\07\xEV\x0fA\xb2C\1"#,
                expect: b"T\n\x07\x0eV\x0fA\xb2C\x01",
            },
            TestCase {
                input: r#"\\\""#,
                expect: b"\\\"",
            },
            TestCase {
                input: r#"\x"#,
                expect: b"x",
            },
            TestCase {
                input: r#"\xg"#,
                expect: b"xg",
            },
            TestCase {
                input: r#"\"#,
                expect: b"\\",
            },
            TestCase {
                input: r#"\8"#,
                expect: b"8",
            },
            TestCase {
                input: r#"\a"#,
                expect: b"a",
            },
            TestCase {
                input: r#"\x\xg\8\xH\x32\s\"#,
                expect: b"xxg8xH2s\\",
            },
        ];

        for test in tests {
            let mut parser = CopyTextFormatParser::new(test.input.as_bytes(), "\t", "\\N");
            assert_eq!(
                parser
                    .consume_raw_value()
                    .expect("unexpected error")
                    .expect("unexpected empty result"),
                test.expect,
                "input: {}, expect: {:?}",
                test.input,
                std::str::from_utf8(test.expect),
            );
            assert!(parser.is_eof());
        }
    }
}
