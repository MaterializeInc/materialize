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
use repr::{Datum, RelationType};
use sql::Session;
use std::fmt;

pub mod coordinator;
pub mod transient;

/// Incoming raw SQL from users.
pub struct Command {
    pub conn_id: u32,
    pub sql: String,
    pub session: sql::Session,
    pub tx: futures::sync::oneshot::Sender<Response>,
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
        typ: RelationType,
        rx: RowsFuture,
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
            SqlResponse::SendRows { typ, rx: _ } => write!(f, "SqlResponse::SendRows({:?})", typ),
            SqlResponse::SetVariable => f.write_str("SqlResponse::SetVariable"),
            SqlResponse::Tailing { rx: _ } => f.write_str("SqlResponse::Tailing"),
        }
    }
}
