// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Types and data structures used to glue the various components of
//! Materialize together.

use crate::dataflow::{Dataflow, Timestamp};
use expr::RelationExpr;
use repr::{Datum, RelationType};
use serde::{Deserialize, Serialize};

pub use uuid::Uuid;

// These work in both async and sync settings, so prefer them over std::sync::mpsc
// (For sync settings, use `sender.unbounded_send`, `receiver.try_next` and `receiver.wait`)
pub use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};

/// Various metadata that gets attached to commands at all stages.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandMeta {
    /// The pgwire connection on which this command originated.
    pub connection_uuid: Uuid,
}

impl CommandMeta {
    pub fn nil() -> CommandMeta {
        CommandMeta {
            connection_uuid: Uuid::nil(),
        }
    }
}

/// Incoming raw SQL from users.
pub struct SqlCommand {
    pub sql: String,
    pub session: crate::sql::Session,
}

/// Responses from the queue to SQL commands.
pub struct SqlResult {
    pub result: Result<SqlResponse, failure::Error>,
    pub session: crate::sql::Session,
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
    Peeking {
        typ: RelationType,
    },
    SendRows {
        typ: RelationType,
        rows: Vec<Vec<Datum>>,
    },
    SetVariable,
    Tailing,
}

/// The commands that a running dataflow server can accept.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataflowCommand {
    CreateDataflows(Vec<Dataflow>),
    DropDataflows(Vec<String>),
    Peek {
        source: RelationExpr,
        when: PeekWhen,
    },
    Tail {
        typ: RelationType,
        name: String,
    },
    Shutdown,
}

/// Specifies when a `Peek` should occur.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeekWhen {
    /// The peek should occur at the latest possible timestamp that allows the
    /// peek to complete immediately.
    Immediately,
    /// The peek should occur at the latest possible timestamp that has been
    /// accepted by each input source.
    EarliestSource,
    /// The peek should occur at the specified timestamp.
    AtTimestamp(Timestamp),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update {
    pub row: Vec<Datum>,
    pub timestamp: u64,
    pub diff: isize,
}

#[derive(Debug, Clone)]
pub enum LocalInput {
    /// Send a batch of updates to the input
    Updates(Vec<Update>),
    /// All future updates will have timestamps >= this timestamp
    Watermark(u64),
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DataflowResults {
    Peeked(Vec<Vec<Datum>>),
    Tailed(Vec<Update>),
}

impl DataflowResults {
    pub fn unwrap_peeked(self) -> Vec<Vec<Datum>> {
        match self {
            DataflowResults::Peeked(v) => v,
            _ => panic!(
                "DataflowResults::unwrap_peeked called on a {:?} variant",
                self
            ),
        }
    }
}

