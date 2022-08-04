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
use enum_kinds::EnumKind;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::watch;

use mz_ore::str::StrExt;
use mz_pgcopy::CopyFormatParams;
use mz_repr::{GlobalId, Row, ScalarType};
use mz_sql::ast::AstInfo;
use mz_sql::ast::{FetchDirection, NoticeSeverity, ObjectType, Raw, Statement};
use mz_sql::plan::ExecuteTimeout;

use crate::client::ConnectionId;
use crate::coord::peek::PeekResponseUnary;
use crate::error::AdapterError;
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

/// The response to [`SessionClient::execute`](crate::SessionClient::execute).
#[derive(Derivative, EnumKind)]
#[derivative(Debug)]
#[enum_kind(ExecuteResponseKind)]
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
    /// Describes the potential set of `ExcecuteResponse` values a `Statement`
    /// can return.
    ///
    /// The primary usecase of this is prohibiting statements in the HTTP API,
    /// so we include some of those semantics here in that we return empty
    /// slices to indicate that a statement is prohibited. If other uses of this
    /// function grow, this should be reconsidered.
    pub fn expected_statement_responses<'stmt, 'res, T: AstInfo>(
        statement: &'stmt Statement<T>,
    ) -> &'res [ExecuteResponseKind] {
        match statement {
            Statement::Close(_) => &[ExecuteResponseKind::ClosedCursor],
            Statement::Copy(_) => &[ExecuteResponseKind::CopyFrom, ExecuteResponseKind::Tailing],
            Statement::Declare(_) => &[ExecuteResponseKind::DeclaredCursor],
            Statement::Fetch(_) => &[ExecuteResponseKind::Fetch],
            Statement::Raise(_) => &[ExecuteResponseKind::Raise],
            Statement::SetVariable(_) | Statement::ResetVariable(_) => {
                &[ExecuteResponseKind::SetVariable]
            }
            Statement::Tail(_) => &[ExecuteResponseKind::Tailing],
            // Hack to prohibit EXECUTE in simple execution
            Statement::Execute(_) => &[],
            // Unsupported
            Statement::SetTransaction(_) => &[],

            Statement::AlterIndex(_) => &[
                ExecuteResponseKind::AlteredIndexLogicalCompaction,
                ExecuteResponseKind::AlteredObject,
            ],
            Statement::AlterSystemReset(_)
            | Statement::AlterSystemResetAll(_)
            | Statement::AlterObjectRename(_)
            | Statement::AlterSecret(_)
            | Statement::AlterSource(_)
            | Statement::AlterSystemSet(_) => &[ExecuteResponseKind::AlteredObject],
            Statement::Commit(_) | Statement::Rollback(_) => {
                &[ExecuteResponseKind::TransactionExited]
            }
            Statement::CreateCluster(_) => &[ExecuteResponseKind::CreatedComputeInstance],
            Statement::CreateClusterReplica(_) => {
                &[ExecuteResponseKind::CreatedComputeInstanceReplica]
            }
            Statement::CreateConnection(_) => &[ExecuteResponseKind::CreatedConnection],
            Statement::CreateDatabase(_) => &[ExecuteResponseKind::CreatedDatabase],
            Statement::CreateIndex(_) => &[ExecuteResponseKind::CreatedIndex],
            Statement::CreateMaterializedView(_) => &[ExecuteResponseKind::CreatedMaterializedView],
            Statement::CreateRole(_) => &[ExecuteResponseKind::CreatedRole],
            Statement::CreateSchema(_) => &[ExecuteResponseKind::CreatedSchema],
            Statement::CreateSecret(_) => &[ExecuteResponseKind::CreatedSecret],
            Statement::CreateSink(_) => &[ExecuteResponseKind::CreatedSink],
            Statement::CreateSource(_) => &[ExecuteResponseKind::CreatedSource],
            Statement::CreateTable(_) => &[ExecuteResponseKind::CreatedTable],
            Statement::CreateType(_) => &[ExecuteResponseKind::CreatedType],
            Statement::CreateView(_) | Statement::CreateViews(_) => {
                &[ExecuteResponseKind::CreatedView]
            }
            Statement::Deallocate(_) => &[ExecuteResponseKind::Deallocate],
            Statement::Delete(_) => &[
                ExecuteResponseKind::Deleted,
                ExecuteResponseKind::SendingRows,
            ],
            Statement::Discard(_) => &[
                ExecuteResponseKind::DiscardedAll,
                ExecuteResponseKind::DiscardedTemp,
            ],
            Statement::DropClusterReplicas(_) => {
                &[ExecuteResponseKind::DroppedComputeInstanceReplicas]
            }
            Statement::DropClusters(_) => &[ExecuteResponseKind::DroppedComputeInstance],
            Statement::DropDatabase(_) => &[ExecuteResponseKind::DroppedDatabase],
            Statement::DropObjects(_) => &[
                ExecuteResponseKind::DroppedConnection,
                ExecuteResponseKind::DroppedComputeInstance,
                ExecuteResponseKind::DroppedComputeInstanceReplicas,
                ExecuteResponseKind::DroppedDatabase,
                ExecuteResponseKind::DroppedSource,
                ExecuteResponseKind::DroppedTable,
                ExecuteResponseKind::DroppedView,
                ExecuteResponseKind::DroppedMaterializedView,
                ExecuteResponseKind::DroppedIndex,
                ExecuteResponseKind::DroppedSink,
                ExecuteResponseKind::DroppedType,
                ExecuteResponseKind::DroppedSecret,
            ],
            Statement::DropRoles(_) => &[ExecuteResponseKind::DroppedRole],
            Statement::DropSchema(_) => &[ExecuteResponseKind::DroppedSchema],

            Statement::Explain(_)
            | Statement::Select(_)
            | Statement::ShowColumns(_)
            | Statement::ShowCreateConnection(_)
            | Statement::ShowCreateIndex(_)
            | Statement::ShowCreateMaterializedView(_)
            | Statement::ShowCreateSink(_)
            | Statement::ShowCreateSource(_)
            | Statement::ShowCreateTable(_)
            | Statement::ShowCreateView(_)
            | Statement::ShowDatabases(_)
            | Statement::ShowIndexes(_)
            | Statement::ShowObjects(_)
            | Statement::ShowSchemas(_)
            | Statement::ShowVariable(_) => &[ExecuteResponseKind::SendingRows],
            Statement::Insert(_) => &[
                ExecuteResponseKind::Inserted,
                ExecuteResponseKind::SendingRows,
            ],
            Statement::Prepare(_) => &[ExecuteResponseKind::Prepare],

            Statement::StartTransaction(_) => &[ExecuteResponseKind::StartedTransaction],
            Statement::Update(_) => &[ExecuteResponseKind::Updated],
        }
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
    Ok,
    /// The query returned an error.
    Err { error: String },
}

impl SimpleResult {
    pub(crate) fn err(msg: impl fmt::Display) -> SimpleResult {
        SimpleResult::Err {
            error: msg.to_string(),
        }
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
