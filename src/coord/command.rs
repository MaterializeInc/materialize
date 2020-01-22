// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use dataflow_types::{PeekResponse, Update};
use sql::Session;

/// The requests the client can make of a [`Coordinator`](crate::Coordinator).
#[derive(Debug)]
pub enum Command {
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

/// Response from the queue to an `Execute` command.
pub enum ExecuteResponse {
    /// The current session has been taken out of transaction mode by COMMIT
    Commit,
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
