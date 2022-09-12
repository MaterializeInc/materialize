// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use derivative::Derivative;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_postgres::error::SqlState;

use mz_ore::str::StrExt;
use mz_pgcopy::CopyFormatParams;
use mz_repr::{GlobalId, Row, ScalarType};
use mz_sql::ast::{FetchDirection, NoticeSeverity, ObjectType, Raw, Statement};
use mz_sql::plan::ExecuteTimeout;

use crate::client::ConnectionId;
use crate::coord::peek::PeekResponseUnary;
use crate::error::AdapterError;
use crate::session::ClientSeverity;
use crate::session::{EndTransactionAction, RowBatchStream, Session};

#[derive(Debug)]
pub enum Command {
    Startup {
        session: Session,
        create_user_if_not_exists: bool,
        cancel_tx: Arc<watch::Sender<Canceled>>,
        tx: oneshot::Sender<Response<StartupResponse>>,
    },

    Declare {
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    Describe {
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    VerifyPreparedStatement {
        name: String,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    Execute {
        portal_name: String,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
        span: tracing::Span,
    },

    StartTransaction {
        implicit: Option<usize>,
        session: Session,
        tx: oneshot::Sender<Response<()>>,
    },

    Commit {
        action: EndTransactionAction,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
    },

    CancelRequest {
        conn_id: ConnectionId,
        secret_key: u32,
    },

    DumpCatalog {
        session: Session,
        tx: oneshot::Sender<Response<String>>,
    },

    CopyRows {
        id: GlobalId,
        columns: Vec<usize>,
        rows: Vec<Row>,
        session: Session,
        tx: oneshot::Sender<Response<ExecuteResponse>>,
    },

    Terminate {
        session: Session,
    },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, AdapterError>,
    pub session: Session,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = PeekResponseUnary> + Send>>;

/// The response to [`ConnClient::startup`](crate::ConnClient::startup).
#[derive(Debug)]
pub struct StartupResponse {
    /// An opaque secret associated with this session.
    pub secret_key: u32,
    /// Notifications associated with session startup.
    pub messages: Vec<StartupMessage>,
}

/// Messages in a [`StartupResponse`].
#[derive(Debug)]
pub enum StartupMessage {
    /// The database specified in the initial session does not exist.
    UnknownSessionDatabase(String),
}

impl StartupMessage {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        None
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            StartupMessage::UnknownSessionDatabase(_) => Some(
                "Create the database with CREATE DATABASE \
                 or pick an extant database with SET DATABASE = name. \
                 List available databases with SHOW DATABASES."
                    .into(),
            ),
        }
    }
}

impl fmt::Display for StartupMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StartupMessage::UnknownSessionDatabase(name) => {
                write!(f, "session database {} does not exist", name.quoted())
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ExecuteResponsePartialError {
    pub severity: ClientSeverity,
    #[serde(skip)]
    pub code: SqlState,
    pub message: String,
}

/// The response to [`SessionClient::execute`](crate::SessionClient::execute).
#[derive(Derivative)]
#[derivative(Debug)]
pub enum ExecuteResponse {
    /// The active transaction was exited.
    TransactionExited {
        was_implicit: bool,
        tag: &'static str,
    },
    // The requested object was altered.
    AlteredObject(ObjectType),
    // The index was altered.
    AlteredIndexLogicalCompaction,
    // The system configuration was altered.
    AlteredSystemConfiguraion,
    // The query was canceled.
    Canceled,
    /// The requested cursor was closed.
    ClosedCursor,
    CopyTo {
        format: mz_sql::plan::CopyFormat,
        resp: Box<ExecuteResponse>,
    },
    CopyFrom {
        id: GlobalId,
        columns: Vec<usize>,
        params: CopyFormatParams<'static>,
    },
    /// The requested connection was created.
    CreatedConnection {
        existed: bool,
    },
    /// The requested database was created.
    CreatedDatabase {
        existed: bool,
    },
    /// The requested schema was created.
    CreatedSchema {
        existed: bool,
    },
    /// The requested role was created.
    CreatedRole,
    /// The requested compute instance was created.
    CreatedComputeInstance {
        existed: bool,
    },
    /// The requested compute instance replica was created.
    CreatedComputeInstanceReplica {
        existed: bool,
    },
    /// The requested index was created.
    CreatedIndex {
        existed: bool,
    },
    /// The requested secret was created.
    CreatedSecret {
        existed: bool,
    },
    /// The requested sink was created.
    CreatedSink {
        existed: bool,
    },
    /// The requested source was created.
    CreatedSource {
        existed: bool,
    },
    /// The requested sources were created.
    CreatedSources,
    /// The requested table was created.
    CreatedTable {
        existed: bool,
    },
    /// The requested view was created.
    CreatedView {
        existed: bool,
    },
    /// The requested views were created.
    CreatedViews {
        existed: bool,
    },
    /// The requested materialized view was created.
    CreatedMaterializedView {
        existed: bool,
    },
    /// The requested type was created.
    CreatedType,
    /// The requested prepared statement was removed.
    Deallocate {
        all: bool,
    },
    /// The requested cursor was declared.
    DeclaredCursor,
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
    /// The temporary objects associated with the session have been discarded.
    DiscardedTemp,
    /// All state associated with the session has been discarded.
    DiscardedAll,
    /// The requested connection was dropped
    DroppedConnection,
    /// The requested compute instance was dropped.
    DroppedComputeInstance,
    /// The requested compute instance replicas were dropped.
    DroppedComputeInstanceReplicas,
    /// The requested database was dropped.
    DroppedDatabase,
    /// The requested role was dropped.
    DroppedRole,
    /// The requested schema was dropped.
    DroppedSchema,
    /// The requested source was dropped.
    DroppedSource,
    /// The requested table was dropped.
    DroppedTable,
    /// The requested view was dropped.
    DroppedView,
    /// The requested materialized view was dropped.
    DroppedMaterializedView,
    /// The requested index was dropped.
    DroppedIndex,
    /// The requested sink was dropped.
    DroppedSink,
    /// The requested type was dropped.
    DroppedType,
    /// The requested secret was dropped.
    DroppedSecret,
    /// The provided query was empty.
    EmptyQuery,
    /// Fetch results from a cursor.
    Fetch {
        /// The name of the cursor from which to fetch results.
        name: String,
        /// The number of results to fetch.
        count: Option<FetchDirection>,
        /// How long to wait for results to arrive.
        timeout: ExecuteTimeout,
    },
    /// The specified number of rows were inserted into the requested table.
    Inserted(usize),
    /// The specified prepared statement was created.
    Prepare,
    /// Rows will be delivered via the specified future.
    SendingRows {
        #[derivative(Debug = "ignore")]
        future: RowsFuture,
        #[derivative(Debug = "ignore")]
        span: tracing::Span,
    },
    /// The specified variable was set to a new value.
    SetVariable {
        name: String,
        tag: &'static str,
    },
    /// A new transaction was started.
    StartedTransaction {
        duplicated: bool, // true if a transaction is in progress
    },
    /// Updates to the requested source or view will be streamed to the
    /// contained receiver.
    Tailing {
        rx: RowBatchStream,
    },
    /// The specified number of rows were updated in the requested table.
    Updated(usize),
    /// Raise a warning.
    Raise {
        severity: NoticeSeverity,
    },
}

impl ExecuteResponse {
    pub fn tag(&self) -> Option<String> {
        use ExecuteResponse::*;
        macro_rules! generic_concat {
            ($lede:expr, $type:expr) => {{
                Some(format!("{} {}", $lede, $type.to_uppercase()))
            }};
        }
        macro_rules! created {
            ($type:expr) => {{
                generic_concat!("CREATE", $type)
            }};
        }
        macro_rules! dropped {
            ($type:expr) => {{
                generic_concat!("DROP", $type)
            }};
        }

        match self {
            TransactionExited { tag, .. } => Some(tag.to_string()),
            AlteredObject(o) => Some(format!("ALTER {}", o)),
            AlteredIndexLogicalCompaction => Some("ALTER INDEX".into()),
            AlteredSystemConfiguraion => Some("ALTER SYSTEM".into()),
            Canceled => None,
            ClosedCursor => Some("CLOSE CURSOR".into()),
            CopyTo { .. } => None,
            CopyFrom { .. } => None,
            CreatedConnection { .. } => created!("connection"),
            CreatedDatabase { .. } => created!("database"),
            CreatedSchema { .. } => created!("schema"),
            CreatedRole => created!("role"),
            CreatedComputeInstance { .. } => created!("cluster"),
            CreatedComputeInstanceReplica { .. } => created!("cluster replica"),
            CreatedIndex { .. } => created!("index"),
            CreatedSecret { .. } => created!("secret"),
            CreatedSink { .. } => created!("sink"),
            CreatedSource { .. } => created!("source"),
            CreatedSources => created!("sources"),
            CreatedTable { .. } => created!("table"),
            CreatedView { .. } => created!("view"),
            CreatedViews { .. } => created!("views"),
            CreatedMaterializedView { .. } => created!("materialized view"),
            CreatedType => created!("type"),
            Deallocate { all } => Some(format!("DEALLOCATE{}", if *all { " ALL" } else { "" })),
            DeclaredCursor => Some("DECLARE CURSOR".into()),
            Deleted(n) => Some(format!("DELETE {}", n)),
            DiscardedTemp => Some("DISCARD TEMP".into()),
            DiscardedAll => Some("DISCARD ALL".into()),
            DroppedConnection => dropped!("connection"),
            DroppedComputeInstance => dropped!("cluster"),
            DroppedComputeInstanceReplicas => dropped!("cluster replica"),
            DroppedDatabase => dropped!("database"),
            DroppedRole => dropped!("role"),
            DroppedSchema => dropped!("schema"),
            DroppedSource => dropped!("source"),
            DroppedTable => dropped!("table"),
            DroppedView => dropped!("view"),
            DroppedMaterializedView => dropped!("materialized view"),
            DroppedIndex => dropped!("index"),
            DroppedSink => dropped!("sink"),
            DroppedType => dropped!("type"),
            DroppedSecret => dropped!("secret"),
            EmptyQuery => None,
            Fetch { .. } => None,
            Inserted(n) => {
                // "On successful completion, an INSERT command returns a
                // command tag of the form `INSERT <oid> <count>`."
                //     -- https://www.postgresql.org/docs/11/sql-insert.html
                //
                // OIDs are a PostgreSQL-specific historical quirk, but we
                // can return a 0 OID to indicate that the table does not
                // have OIDs.
                Some(format!("INSERT 0 {}", n))
            }
            Prepare => Some("PREPARE".into()),
            SendingRows { .. } => None,
            SetVariable { tag, .. } => Some(tag.to_string()),
            StartedTransaction { .. } => Some("BEGIN".into()),
            Tailing { .. } => None,
            Updated(n) => Some(format!("UPDATE {}", n)),
            Raise { .. } => Some("RAISE".into()),
        }
    }

    /// When an appropriate error response can be totally determined by the
    /// `ExecuteResponse`, generate it if it is non-terminal, i.e. should not be
    /// returned as an error instead of the value.
    ///
    /// # Panics
    /// - If returns an error with [`ClientSeverity::Error`].
    pub fn partial_err(&self) -> Option<ExecuteResponsePartialError> {
        use ExecuteResponse::*;

        macro_rules! existed {
            ($existed:expr, $code:expr, $type:expr) => {{
                if $existed {
                    Some(ExecuteResponsePartialError {
                        severity: ClientSeverity::Notice,
                        code: $code,
                        message: format!("{} already exists, skipping", $type),
                    })
                } else {
                    None
                }
            }};
        }

        let r = match self {
            CreatedConnection { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "connection")
            }
            CreatedDatabase { existed } => {
                existed!(*existed, SqlState::DUPLICATE_DATABASE, "database")
            }
            CreatedSchema { existed } => {
                existed!(*existed, SqlState::DUPLICATE_SCHEMA, "schema")
            }
            CreatedRole => {
                existed!(false, SqlState::DUPLICATE_OBJECT, "role")
            }
            CreatedComputeInstance { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "cluster")
            }
            CreatedComputeInstanceReplica { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "cluster replica")
            }
            CreatedTable { existed } => {
                existed!(*existed, SqlState::DUPLICATE_TABLE, "table")
            }
            CreatedIndex { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "index")
            }
            CreatedSecret { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "secret")
            }
            CreatedSource { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "source")
            }
            CreatedSink { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "sink")
            }
            CreatedView { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "view")
            }
            CreatedViews { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "views")
            }
            CreatedMaterializedView { existed } => {
                existed!(*existed, SqlState::DUPLICATE_OBJECT, "materialized view")
            }

            Raise { severity } => Some(match severity {
                NoticeSeverity::Debug => ExecuteResponsePartialError {
                    severity: ClientSeverity::Debug1,
                    code: SqlState::WARNING,
                    message: "raised a test debug".to_string(),
                },
                NoticeSeverity::Info => ExecuteResponsePartialError {
                    severity: ClientSeverity::Info,
                    code: SqlState::WARNING,
                    message: "raised a test info".to_string(),
                },
                NoticeSeverity::Log => ExecuteResponsePartialError {
                    severity: ClientSeverity::Log,
                    code: SqlState::WARNING,
                    message: "raised a test log".to_string(),
                },
                NoticeSeverity::Notice => ExecuteResponsePartialError {
                    severity: ClientSeverity::Notice,
                    code: SqlState::WARNING,
                    message: "raised a test notice".to_string(),
                },
                NoticeSeverity::Warning => ExecuteResponsePartialError {
                    severity: ClientSeverity::Warning,
                    code: SqlState::WARNING,
                    message: "raised a test warning".to_string(),
                },
            }),

            StartedTransaction { duplicated } => {
                if *duplicated {
                    Some(ExecuteResponsePartialError {
                        severity: ClientSeverity::Warning,
                        code: SqlState::ACTIVE_SQL_TRANSACTION,
                        message: "there is already a transaction in progress".to_string(),
                    })
                } else {
                    None
                }
            }

            TransactionExited { was_implicit, .. } => {
                // In Postgres, if a user sends a COMMIT or ROLLBACK in an implicit
                // transaction, a warning is sent warning them. (The transaction is still closed
                // and a new implicit transaction started, though.)
                if *was_implicit {
                    Some(ExecuteResponsePartialError {
                        severity: ClientSeverity::Warning,
                        code: SqlState::NO_ACTIVE_SQL_TRANSACTION,
                        message: "there is no transaction in progress".to_string(),
                    })
                } else {
                    None
                }
            }

            CreatedSources
            | CreatedType
            | AlteredObject(..)
            | AlteredIndexLogicalCompaction
            | AlteredSystemConfiguraion
            | Canceled
            | ClosedCursor
            | CopyTo { .. }
            | CopyFrom { .. }
            | Deallocate { .. }
            | DeclaredCursor
            | Deleted(..)
            | DiscardedTemp
            | DiscardedAll
            | DroppedConnection
            | DroppedComputeInstance
            | DroppedComputeInstanceReplicas
            | DroppedDatabase
            | DroppedRole
            | DroppedSchema
            | DroppedSource
            | DroppedTable
            | DroppedView
            | DroppedMaterializedView
            | DroppedIndex
            | DroppedSink
            | DroppedType
            | DroppedSecret
            | EmptyQuery
            | Fetch { .. }
            | Inserted(..)
            | Prepare
            | SendingRows { .. }
            | SetVariable { .. }
            | Tailing { .. }
            | Updated(..) => None,
        };

        assert!(
            !matches!(
                r,
                Some(ExecuteResponsePartialError {
                    severity: ClientSeverity::Error,
                    ..
                })
            ),
            "partial_err cannot generate errors"
        );

        r
    }
}

/// The response to [`SessionClient::simple_execute`](crate::SessionClient::simple_execute).
#[derive(Debug, Serialize)]
pub struct SimpleExecuteResponse {
    pub results: Vec<SimpleResult>,
}

/// The result of a single query executed with `simple_execute`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum SimpleResult {
    /// The query returned rows.
    Rows {
        /// The result rows.
        rows: Vec<Vec<serde_json::Value>>,
        /// The name of the columns in the row.
        col_names: Vec<String>,
    },
    /// The query executed successfully but did not return rows.
    Ok {
        ok: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partial_err: Option<ExecuteResponsePartialError>,
    },
    /// The query returned an error.
    Err { error: String },
}

impl SimpleResult {
    pub(crate) fn err(msg: impl fmt::Display) -> SimpleResult {
        SimpleResult::Err {
            error: msg.to_string(),
        }
    }

    /// Generates a `SimpleResult::Ok` based on an `ExecuteResponse`.
    ///
    /// # Panics
    /// - If [`ExecuteResponse::partial_err`] returns an error with
    ///   [`ClientSeverity::Error`].
    pub(crate) fn ok(res: ExecuteResponse) -> SimpleResult {
        let ok = res.tag();
        let partial_err = res.partial_err();
        SimpleResult::Ok { ok, partial_err }
    }
}

/// The state of a cancellation request.
#[derive(Debug, Clone, Copy)]
pub enum Canceled {
    /// A cancellation request has occurred.
    Canceled,
    /// No cancellation request has yet occurred, or a previous request has been
    /// cleared.
    NotCanceled,
}
