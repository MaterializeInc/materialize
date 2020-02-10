// Copyright Materialize, Inc. All rights reserved.
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
    CreatedView,
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
    SendRows(RowsFuture),
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

impl fmt::Debug for ExecuteResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecuteResponse::CreatedDatabase { existed } => write!(
                f,
                "ExecuteResponse::CreatedDatabase {{ existed: {} }}",
                existed
            ),
            ExecuteResponse::CreatedSchema { existed } => write!(
                f,
                "ExecuteResponse::CreatedSchema {{ existed: {} }}",
                existed
            ),
            ExecuteResponse::CreatedIndex { existed } => write!(
                f,
                "ExecuteResponse::CreatedIndex {{ existed: {} }}",
                existed
            ),
            ExecuteResponse::CreatedSink { existed } => {
                write!(f, "ExecuteResponse::CreatedSink {{ existed: {} }}", existed)
            }
            ExecuteResponse::CreatedSource { existed } => write!(
                f,
                "ExecuteResponse::CreatedSource {{ existed: {} }}",
                existed
            ),
            ExecuteResponse::CreatedTable { existed } => write!(
                f,
                "ExecuteResponse::CreatedTable {{ existed: {} }}",
                existed
            ),
            ExecuteResponse::CreatedView => f.write_str("ExecuteResponse::CreatedView"),
            ExecuteResponse::Deleted(n) => write!(f, "ExecuteResponse::Deleted({})", n),
            ExecuteResponse::DroppedDatabase => f.write_str("ExecuteResponse::DroppedDatabase"),
            ExecuteResponse::DroppedSchema => f.write_str("ExecuteResponse::DroppedSchema"),
            ExecuteResponse::DroppedIndex => f.write_str("ExecuteResponse::DroppedIndex"),
            ExecuteResponse::DroppedSink => f.write_str("ExecuteResponse::DroppedSink"),
            ExecuteResponse::DroppedSource => f.write_str("ExecuteResponse::DroppedSource"),
            ExecuteResponse::DroppedTable => f.write_str("ExecuteResponse::DroppedTable"),
            ExecuteResponse::DroppedView => f.write_str("ExecuteResponse::DroppedView"),
            ExecuteResponse::EmptyQuery => f.write_str("ExecuteResponse::EmptyQuery"),
            ExecuteResponse::Commit => f.write_str("ExecuteResponse::Commit"),
            ExecuteResponse::Rollback => f.write_str("ExecuteResponse::Rollback"),
            ExecuteResponse::Inserted(n) => write!(f, "ExecuteResponse::Inserted({})", n),
            ExecuteResponse::SendRows(_) => write!(f, "ExecuteResponse::SendRows(<rx>)"),
            ExecuteResponse::SetVariable { name } => {
                write!(f, "ExecuteResponse::SetVariable({})", name)
            }
            ExecuteResponse::StartTransaction => f.write_str("ExecuteResponse::StartTransaction"),
            ExecuteResponse::Tailing { rx: _ } => f.write_str("ExecuteResponse::Tailing"),
            ExecuteResponse::Updated(n) => write!(f, "ExecuteResponse::Updated({})", n),
        }
    }
}
