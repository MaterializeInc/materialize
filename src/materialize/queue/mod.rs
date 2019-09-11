// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Strictly ordered queues.
//!
//! The queues in this module will eventually grow to include queues that
//! provide distribution, replication, and durability. At the moment,
//! only a simple, transient, single-node queue is provided.

use dataflow_types::RowSetFinishing;
use repr::{Datum, RelationType};
use sql::Session;

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

/// Flag for whether optimizer or workers will chime in as well.
#[derive(Debug)]
pub enum WaitFor {
    NoOne,
    Optimizer,
    Workers,
}

#[derive(Debug)]
/// Responses from the planner to SQL commands.
pub enum SqlResponse {
    CreatedSink,
    CreatedSource,
    CreatedView,
    DroppedSource,
    DroppedView,
    EmptyQuery,
    SendRows {
        typ: RelationType,
        rows: Vec<Vec<Datum>>,
        wait_for: WaitFor,
        transform: RowSetFinishing,
    },
    SetVariable,
    Tailing,
}
