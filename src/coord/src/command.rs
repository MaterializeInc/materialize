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

use dataflow_types::{PeekResponse, Update};
use sql::ast::Statement;

use crate::session::Session;

/// The requests the client can make of a [`Coordinator`](crate::Coordinator).
#[derive(Debug)]
pub enum Command {
    /// Notify the coordinator of a new client session.
    Startup {
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<Vec<StartupMessage>>>,
    },

    /// Save the specified statement as a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`sql::Session`]
    /// under the specified name.
    Describe {
        name: String,
        stmt: Option<Statement>,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<()>>,
    },

    /// Execute a bound portal.
    Execute {
        portal_name: String,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<ExecuteResponse>>,
    },

    /// Cancel the query currently running on another connection.
    CancelRequest { conn_id: u32 },

    /// Dump the catalog to a JSON string.
    DumpCatalog {
        tx: futures::channel::oneshot::Sender<String>,
    },

    /// Remove temporary objects created by a given connection.
    Terminate { conn_id: u32 },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, failure::Error>,
    pub session: Session,
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
    /// The active transaction was rolled back.
    AbortedTransaction,
    /// The active transaction was committed.
    CommittedTransaction,
    /// The requested database was created.
    CreatedDatabase { existed: bool },
    /// The requested schema was created.
    CreatedSchema { existed: bool },
    /// The requested index was created.
    CreatedIndex { existed: bool },
    /// The requested sink was created.
    CreatedSink { existed: bool },
    /// The requested source was created.
    CreatedSource { existed: bool },
    /// The requested table was created.
    CreatedTable { existed: bool },
    /// The requested view was created.
    CreatedView { existed: bool },
    /// The specified number of rows were deleted from the requested table.
    Deleted(usize),
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
    /// The requested sink wqs dropped.
    DroppedSink,
    /// The provided query was empty.
    EmptyQuery,
    /// The specified number of rows were inserted into the requested table.
    Inserted(usize),
    /// Rows will be delivered via the specified future.
    SendingRows(#[derivative(Debug = "ignore")] RowsFuture),
    /// The specified variable was set to a new value.
    SetVariable { name: String },
    /// A new transaction was started.
    StartedTransaction,
    /// Updates to the requested source or view will be streamed to the
    /// contained receiver.
    Tailing {
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },
    /// The specified number of rows were updated in the requested table.
    Updated(usize),
}
