// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Strictly ordered queues.
//!
//! The queues in this module will eventually grow to include queues that
//! provide distribution, replication, and durability. At the moment,
//! only a simple, transient, single-node queue is provided.

use uuid::Uuid;

pub mod transient;

/// Incoming raw SQL from users.
pub struct Command {
    pub connection_uuid: Uuid,
    pub sql: String,
    pub session: sql::Session,
    pub tx: futures::sync::oneshot::Sender<Response>,
}

/// Responses from the queue to SQL commands.
pub struct Response {
    pub sql_result: Result<sql::SqlResponse, failure::Error>,
    pub session: sql::Session,
}
