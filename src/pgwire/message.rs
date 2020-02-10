// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::BytesMut;

use dataflow_types::Update;
use repr::{ColumnName, RelationDesc, RelationType, ScalarType};
use sql::TransactionStatus as SqlTransactionStatus;

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

#[derive(Debug)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

impl ErrorSeverity {
    pub fn string(&self) -> &'static str {
        match self {
            ErrorSeverity::Error => "ERROR",
            ErrorSeverity::Fatal => "FATAL",
            ErrorSeverity::Panic => "PANIC",
        }
    }
}

#[derive(Debug)]
pub enum NoticeSeverity {
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl NoticeSeverity {
    pub fn string(&self) -> &'static str {
        match self {
            NoticeSeverity::Warning => "WARNING",
            NoticeSeverity::Notice => "NOTICE",
            NoticeSeverity::Debug => "DEBUG",
            NoticeSeverity::Info => "INFO",
            NoticeSeverity::Log => "LOG",
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
    Startup {
        version: i32,
        params: Vec<(String, String)>,
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
            FrontendMessage::Flush => "flush",
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
    DataRow(Vec<Option<pgrepr::Value>>, Arc<Vec<pgrepr::Format>>),
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
    NoticeResponse {
        severity: NoticeSeverity,
        code: &'static str,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },
    ErrorResponse {
        severity: ErrorSeverity,
        code: &'static str,
        message: String,
        detail: Option<String>,
    },
    CopyOutResponse {
        overall_format: pgrepr::Format,
        column_formats: Vec<pgrepr::Format>,
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

pub fn encode_update(update: Update, typ: &RelationType) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = BytesMut::new();
    for field in pgrepr::values_from_row(update.row, typ) {
        match field {
            None => out.extend(b"\\N"),
            Some(field) => {
                buf.clear();
                field.encode_text(&mut buf);
                for b in &buf {
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

pub fn row_description_from_desc(desc: &RelationDesc) -> Vec<FieldDescription> {
    desc.iter()
        .map(|(name, typ)| {
            let pg_type: pgrepr::Type = typ.scalar_type.clone().into();
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
                format: pgrepr::Format::Text,
            }
        })
        .collect()
}
