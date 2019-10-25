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
//! These commands are derived directly from the commands that [`pgwire`]
//! produces, though they can, in theory, be provided by something other than a
//! pgwire server.

use dataflow_types::{PeekResponse, Update};
use futures::Future;
use repr::RelationDesc;
use sql::Session;
use std::fmt;

pub mod coordinator;
pub mod transient;

/// The kinds of requests the client can make of the coordinator.
#[derive(Debug)]
pub enum Command {
    /// Parse and execute the specified SQL.
    Query {
        sql: String,
        session: Session,
        conn_id: u32,
        tx: futures::sync::oneshot::Sender<Response<QueryExecuteResponse>>,
    },

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
        tx: futures::sync::oneshot::Sender<Response<QueryExecuteResponse>>,
    },

    /// Cancel the query currently running on another connection.
    CancelRequest { conn_id: u32 },

    /// Shut down the coordinator thread.
    Shutdown,
}

#[derive(Debug)]
pub struct Response<T> {
    pub result: Result<T, failure::Error>,
    pub session: Session,
}

pub type RowsFuture = Box<dyn Future<Item = PeekResponse, Error = failure::Error> + Send>;

/// Response from the queue to a `Query` or `Execute` command.
pub enum QueryExecuteResponse {
    CreatedSink,
    CreatedSource,
    CreatedView,
    DroppedSource,
    DroppedView,
    EmptyQuery,
    SendRows {
        desc: RelationDesc,
        rx: RowsFuture,
    },
    SetVariable {
        name: String,
    },
    Tailing {
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },
}

impl fmt::Debug for QueryExecuteResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryExecuteResponse::CreatedSink => f.write_str("QueryExecuteResponse::CreatedSink"),
            QueryExecuteResponse::CreatedSource => {
                f.write_str("QueryExecuteResponse::CreatedSource")
            }
            QueryExecuteResponse::CreatedView => f.write_str("QueryExecuteResponse::CreatedView"),
            QueryExecuteResponse::DroppedSource => {
                f.write_str("QueryExecuteResponse::DroppedSource")
            }
            QueryExecuteResponse::DroppedView => f.write_str("QueryExecuteResponse::DroppedView"),
            QueryExecuteResponse::EmptyQuery => f.write_str("QueryExecuteResponse::EmptyQuery"),
            QueryExecuteResponse::SendRows { desc, rx: _ } => {
                write!(f, "QueryExecuteResponse::SendRows({:?})", desc)
            }
            QueryExecuteResponse::SetVariable { name } => {
                write!(f, "QueryExecuteResponse::SetVariable({})", name)
            }
            QueryExecuteResponse::Tailing { rx: _ } => f.write_str("QueryExecuteResponse::Tailing"),
        }
    }
}
