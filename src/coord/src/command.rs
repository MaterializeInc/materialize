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

use derivative::Derivative;

use dataflow_types::PeekResponse;
use repr::Row;
use sql::ast::{ObjectType, Statement};

use crate::session::Session;

#[derive(Debug)]
pub enum Command {
    /// Notify the coordinator of a new client session.
    Startup {
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<Vec<StartupMessage>>>,
    },

    Describe {
        name: String,
        stmt: Option<Statement>,
        param_types: Vec<Option<pgrepr::Type>>,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<()>>,
    },

    Execute {
        portal_name: String,
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
        stmt: Statement,
        params: sql::plan::Params,
        tx: futures::channel::oneshot::Sender<anyhow::Result<NoSessionExecuteResponse>>,
    },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, anyhow::Error>,
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
        tag: String,
    },
    // The requested object was altered.
    AlteredObject(ObjectType),
    // The index was altered.
    AlteredIndexLogicalCompaction,
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
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
    /// The temporary objects associated with the session have been discarded.
    DiscardedTemp,
    /// All state associated with the session has been discarded.
    DiscardedAll,
    /// The requested database was dropped.
    DroppedDatabase,
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
