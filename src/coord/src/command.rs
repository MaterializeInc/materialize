// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use derivative::Derivative;

use dataflow_types::PeekResponse;
use repr::Row;
use sql::ast::{FetchDirection, ObjectType, Raw, Statement};
use sql::plan::ExecuteTimeout;
use tokio::sync::watch;

use crate::error::CoordError;
use crate::session::{EndTransactionAction, Session};

#[derive(Debug)]
pub enum Command {
    /// Notify the coordinator of a new client session.
    Startup {
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<Vec<StartupMessage>>>,
    },

    Declare {
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<pgrepr::Type>>,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<()>>,
    },

    Describe {
        name: String,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<pgrepr::Type>>,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<()>>,
    },

    Execute {
        portal_name: String,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<ExecuteResponse>>,
    },

    Commit {
        action: EndTransactionAction,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<ExecuteResponse>>,
    },

    CancelRequest {
        conn_id: u32,
    },

    DumpCatalog {
        tx: futures::channel::oneshot::Sender<String>,
    },

    Terminate {
        session: Session,
    },

    NoSessionExecute {
        stmt: Statement<Raw>,
        params: sql::plan::Params,
        tx: futures::channel::oneshot::Sender<Result<NoSessionExecuteResponse, CoordError>>,
    },

    RegisterCancel {
        conn_id: u32,
        cancel_tx: Arc<Mutex<watch::Sender<Cancelled>>>,
        tx: futures::channel::oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, CoordError>,
    pub session: Session,
}

#[derive(Debug)]
pub struct NoSessionExecuteResponse {
    pub desc: Option<repr::RelationDesc>,
    pub response: ExecuteResponse,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = Result<PeekResponse, comm::Error>> + Send>>;

/// Notifications that may be generated in response to [`Command::Startup`].
#[derive(Debug)]
pub enum StartupMessage {
    /// The database specified in the initial session does not exist.
    UnknownSessionDatabase,
}

/// The response to [`Command::Execute]`.
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
    /// The requested cursor was closed.
    ClosedCursor,
    CopyTo {
        format: sql::plan::CopyFormat,
        #[derivative(Debug = "ignore")]
        resp: Box<ExecuteResponse>,
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
    /// The requested index was created.
    CreatedIndex {
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
    /// The requested table was created.
    CreatedTable {
        existed: bool,
    },
    /// The requested view was created.
    CreatedView {
        existed: bool,
    },
    /// The requested type was created.
    CreatedType,
    /// The requested cursor was declared.
    DeclaredCursor,
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
    /// The temporary objects associated with the session have been discarded.
    DiscardedTemp,
    /// All state associated with the session has been discarded.
    DiscardedAll,
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
    /// The requested index was dropped.
    DroppedIndex,
    /// The requested sink was dropped.
    DroppedSink,
    /// The requested type was dropped.
    DroppedType,
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
    /// Rows will be delivered via the specified future.
    SendingRows(#[derivative(Debug = "ignore")] RowsFuture),
    /// The specified variable was set to a new value.
    SetVariable {
        name: String,
    },
    /// A new transaction was started.
    StartedTransaction,
    /// Updates to the requested source or view will be streamed to the
    /// contained receiver.
    Tailing {
        rx: comm::mpsc::Receiver<Vec<Row>>,
    },
    /// The specified number of rows were updated in the requested table.
    Updated(usize),
}

/// The state of a cancellation request.
#[derive(Debug, Clone, Copy)]
pub enum Cancelled {
    /// A cancellation request has occurred.
    Cancelled,
    /// No cancellation request has yet occurred, or a previous request has been
    /// cleared.
    NotCancelled,
}
