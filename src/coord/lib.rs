// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Strictly ordered queues.
//!
//! The queues in this module will eventually grow to include queues that
//! provide distribution, replication, and durability. At the moment,
//! only a simple, transient, single-node queue is provided.

use dataflow_types::Update;
use futures::Future;
use repr::{Datum, RelationDesc};
use sql::Session;
use std::fmt;

pub mod coordinator;
pub mod transient;

pub struct Command {
    pub kind: CommandKind,
    pub conn_id: u32,
    pub session: sql::Session,
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

/// Things a user could request of the dataflow layer
///
/// [`CommandKind::Query`] is the most general command. Everything else is part of the
/// extended query flow
#[derive(Debug)]
pub enum CommandKind {
    /// Incoming raw sql from users, arbitrary one-off queries come in this way
    Query { sql: String },

    // Extended query flow
    /// Parse a statement but do not execute it
    ///
    /// This results in a Prepared Statement and begins the extended query flow, see the
    /// `pgwire::protocol` module for the full flow, some parts of it don't need to go
    /// all the way to the query layer so they aren't reflected here.
    Parse { name: String, sql: String },
    /// Execute a bound statement
    ///
    /// Bind doesn't need to go through the planner, so there is no `Bind` command right
    /// now, although we could imagine creating a dataflow at that point, which would
    /// make it show up in this enum
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
