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

use dataflow_types::Update;
use futures::Future;
use repr::{Datum, RelationDesc};
use sql::Session;
use std::fmt;

pub mod coordinator;
pub mod transient;

/// An incoming client request.
pub struct Command {
    /// The kind of request.
    pub kind: CommandKind,
    /// The ID of the connection making the request.
    pub conn_id: u32,
    /// The connection's session.
    pub session: sql::Session,
    /// A transmitter over which the response to the request should be sent.
    pub tx: futures::sync::oneshot::Sender<Response>,
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Command")
            .field("kind", &self.kind)
            .field("conn_id", &self.conn_id)
            .field("session", &self.session)
            .field("tx", &"<>")
            .finish()
    }
}

/// The kinds of requests the client can make of the coordinator.
#[derive(Debug)]
pub enum CommandKind {
    /// Parse and execute the specified SQL.
    Query { sql: String },

    /// Parse the specified SQL into a prepared statement.
    ///
    /// The prepared statement is saved in the connection's [`sql::Session`]
    /// under the specified name.
    Parse { name: String, sql: String },

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
    Execute { portal_name: String },
}

/// Responses from the queue to SQL commands.
pub struct Response {
    pub sql_result: Result<SqlResponse, failure::Error>,
    pub session: Session,
}

pub type RowsFuture = Box<dyn Future<Item = Vec<Vec<Datum>>, Error = failure::Error> + Send>;

/// The SQL portition of [`Response`].
pub enum SqlResponse {
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
    /// We have successfully parsed the query and stashed it in the [`Session`]
    Parsed {
        name: String,
    },
    SetVariable,
    Tailing {
        rx: comm::mpsc::Receiver<Vec<Update>>,
    },
}

impl fmt::Debug for SqlResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SqlResponse::CreatedSink => f.write_str("SqlResponse::CreatedSink"),
            SqlResponse::CreatedSource => f.write_str("SqlResponse::CreatedSource"),
            SqlResponse::CreatedView => f.write_str("SqlResponse::CreatedView"),
            SqlResponse::DroppedSource => f.write_str("SqlResponse::DroppedSource"),
            SqlResponse::DroppedView => f.write_str("SqlResposne::DroppedView"),
            SqlResponse::EmptyQuery => f.write_str("SqlResponse::EmptyQuery"),
            SqlResponse::Parsed { name } => write!(f, "SqlResponse::Parsed(name: {})", name),
            SqlResponse::SendRows { desc, rx: _ } => write!(f, "SqlResponse::SendRows({:?})", desc),
            SqlResponse::SetVariable => f.write_str("SqlResponse::SetVariable"),
            SqlResponse::Tailing { rx: _ } => f.write_str("SqlResponse::Tailing"),
        }
    }
}
