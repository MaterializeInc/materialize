// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Coordinates client requests with the dataflow layer.
//!
//! Client requests are either a "simple" query, in which SQL is parsed,
//! planned, and executed in one shot, or an "extended" query, where the client
//! controls the parsing, planning, and execution via individual messages,
//! allowing it to reuse pre-parsed and pre-planned queries (i.e., via "prepared
//! statements"), which can be more efficient when the same query is executed
//! multiple times.
//!
//! These commands are derived directly from the commands that
//! [`pgwire`](../pgwire/index.html) produces, though they can, in theory, be
//! provided by something other than a pgwire server.

use dataflow_types::{PeekResponse, Update};
use futures::Future;
use sql::Session;
use std::fmt;

pub mod coordinator;
pub mod transient;

/// The requests the client can make of the coordinator.
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
        tx: futures::sync::oneshot::Sender<Response<()>>,
    },

    /// Bind a prepared statement to a portal, filling in any placeholders
    /// in the prepared statement.
    ///
    /// Not currently enabled. Since we don't support placeholders yet,
    /// parameter "binding" takes place entirely in the pgwire crate.
    ///
    // Bind {
    //     statement_name: String,
    //     portal_name: String,
    //     parameter_values: Vec<Datum>,
    // }

    /// Execute a bound portal.
    Execute {
        portal_name: String,
        session: Session,
        conn_id: u32,
        tx: futures::sync::oneshot::Sender<Response<ExecuteResponse>>,
    },

    /// Cancel the query currently running on another connection.
    CancelRequest { conn_id: u32 },
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, failure::Error>,
    pub session: Session,
}

pub type RowsFuture = Box<dyn Future<Item = PeekResponse, Error = failure::Error> + Send>;

/// Response from the queue to an `Execute` command.
pub enum ExecuteResponse {
    /// The current session has been taken out of transaction mode by COMMIT
    Commit,
    CreatedIndex,
    CreatedSink,
    CreatedSource,
    CreatedTable,
    CreatedView,
    Deleted(usize),
    DroppedSource,
    DroppedTable,
    DroppedView,
    DroppedIndex,
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
            ExecuteResponse::CreatedIndex => f.write_str("ExecuteResponse::CreatedIndex"),
            ExecuteResponse::CreatedSink => f.write_str("ExecuteResponse::CreatedSink"),
            ExecuteResponse::CreatedSource => f.write_str("ExecuteResponse::CreatedSource"),
            ExecuteResponse::CreatedTable => f.write_str("ExecuteResponse::CreatedTable"),
            ExecuteResponse::CreatedView => f.write_str("ExecuteResponse::CreatedView"),
            ExecuteResponse::Deleted(n) => write!(f, "ExecuteResponse::Deleted({})", n),
            ExecuteResponse::DroppedIndex => f.write_str("ExecuteResponse::DroppedIndex"),
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
