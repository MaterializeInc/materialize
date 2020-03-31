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
use sql::Session;

/// The requests the client can make of a [`Coordinator`](crate::Coordinator).
#[derive(Debug)]
pub enum Command {
    /// Notify the coordinator of a new client session.
    Startup {
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<Vec<StartupMessage>>>,
    },

    /// Parse the specified SQL into a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`sql::Session`]
    /// under the specified name.
    Parse {
        name: String,
        sql: String,
        session: Session,
        tx: futures::channel::oneshot::Sender<Response<()>>,
    },

    /// Execute a bound portal.
    Execute {
        portal_name: String,
        session: Session,
        conn_id: u32,
        tx: futures::channel::oneshot::Sender<Response<ExecuteResponse>>,
    },

    /// Cancel the query currently running on another connection.
    CancelRequest { conn_id: u32 },

    /// Dump the catalog to a JSON string.
    DumpCatalog {
        tx: futures::channel::oneshot::Sender<String>,
    },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, failure::Error>,
    pub session: Session,
}

pub type RowsFuture = Pin<Box<dyn Future<Output = Result<PeekResponse, comm::Error>> + Send>>;

#[derive(Debug)]
pub enum StartupMessage {
    UnknownSessionDatabase,
}

/// Response from the queue to an `Execute` command.
#[derive(Derivative)]
#[derivative(Debug)]
pub enum ExecuteResponse {
    /// The current session has been taken out of transaction mode by COMMIT
    Commit,
    CreatedDatabase {
        existed: bool,
    },
    CreatedSchema {
        existed: bool,
    },
    CreatedIndex {
        existed: bool,
    },
    CreatedSink {
        existed: bool,
    },
    CreatedSource {
        existed: bool,
    },
    CreatedTable {
        existed: bool,
    },
    CreatedView {
        existed: bool,
    },
    Deleted(usize),
    DroppedDatabase,
    DroppedSchema,
    DroppedSource,
    DroppedTable,
    DroppedView,
    DroppedIndex,
    DroppedSink,
    EmptyQuery,
    Inserted(usize),
    /// The current session has been taken out of transaction mode by ROLLBACK
    Rollback,
    SendRows(#[derivative(Debug = "ignore")] RowsFuture),
    SetVariable {
        name: String,
    },
    /// The current session has been placed into transaction mode
    StartTransaction,
    Tailing {
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },
    Updated(usize),
}
