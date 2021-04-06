// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;

use bytes::BytesMut;
use coord::{CoordError, StartupMessage};
use itertools::Itertools;
use postgres::error::SqlState;

use coord::session::TransactionStatus as CoordTransactionStatus;
use repr::{ColumnName, RelationDesc, RelationType, Row, ScalarType};

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
            CoordTransactionStatus::Failed => TransactionStatus::Failed,
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
            CoordError::Catalog(_) => SqlState::INTERNAL_ERROR,
            CoordError::ConstrainedParameter(_) => SqlState::INVALID_PARAMETER_VALUE,
            CoordError::DuplicateCursor(_) => SqlState::DUPLICATE_CURSOR,
            CoordError::Eval(_) => SqlState::INTERNAL_ERROR,
            CoordError::IdExhaustionError => SqlState::INTERNAL_ERROR,
            CoordError::InvalidParameterType(_) => SqlState::INVALID_PARAMETER_VALUE,
            CoordError::OperationProhibitsTransaction(_) => SqlState::ACTIVE_SQL_TRANSACTION,
            CoordError::OperationRequiresTransaction(_) => SqlState::NO_ACTIVE_SQL_TRANSACTION,
            CoordError::ReadOnlyTransaction => SqlState::READ_ONLY_SQL_TRANSACTION,
            CoordError::ReadOnlyParameter(_) => SqlState::CANT_CHANGE_RUNTIME_PARAM,
            CoordError::SafeModeViolation(_) => SqlState::INTERNAL_ERROR,
            CoordError::SqlCatalog(_) => SqlState::INTERNAL_ERROR,
            CoordError::Transform(_) => SqlState::INTERNAL_ERROR,
            CoordError::UnknownCursor(_) => SqlState::INVALID_CURSOR_NAME,
            CoordError::UnknownParameter(_) => SqlState::UNDEFINED_OBJECT,
            CoordError::UnknownLoginRole(_) => SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
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
                    ScalarType::Decimal(precision, scale) => {
                        ((i32::from(*precision) << 16) | i32::from(*scale)) + 4
                    }
                    _ => -1,
                },
                format: *format,
            }
        })
        .collect()
}
