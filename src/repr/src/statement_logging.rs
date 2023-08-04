// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::now::EpochMillis;
use uuid::Uuid;
/// Contains all the information necessary to generate the initial
/// entry in `mz_statement_execution_history`. We need to keep this
/// around in order to modify the entry later once the statement finishes executing.
#[derive(Clone, Debug)]
pub struct StatementBeganExecutionRecord {
    pub id: Uuid,
    pub prepared_statement_id: Uuid,
    pub sample_rate: f64,
    pub params: Vec<Option<String>>,
    pub began_at: EpochMillis,
}

#[derive(Clone, Copy, Debug)]
pub enum StatementExecutionStrategy {
    /// The statement was executed by spinning up a dataflow.
    Standard,
    /// The statement was executed by reading from an existing
    /// arrangement.
    FastPath,
    /// The statement was determined to be constant by
    /// environmentd, and not sent to a cluster.
    Constant,
}

impl StatementExecutionStrategy {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::FastPath => "fast-path",
            Self::Constant => "constant",
        }
    }
}

#[derive(Clone, Debug)]
pub enum StatementEndedExecutionReason {
    Success {
        rows_returned: Option<u64>,
        execution_strategy: Option<StatementExecutionStrategy>,
    },
    Canceled,
    Errored {
        error: String,
    },
    Aborted,
}

#[derive(Clone, Debug)]
pub struct StatementEndedExecutionRecord {
    pub id: Uuid,
    pub reason: StatementEndedExecutionReason,
    pub ended_at: EpochMillis,
}

/// Contains all the information necessary to generate an entry in
/// `mz_prepared_statement_history`
#[derive(Clone, Debug)]
pub struct StatementPreparedRecord {
    pub id: Uuid,
    pub sql: String,
    pub name: String,
    pub session_id: Uuid,
    pub prepared_at: EpochMillis,
}

#[derive(Clone, Debug)]
pub enum StatementLoggingEvent {
    Prepared(StatementPreparedRecord),
    BeganExecution(StatementBeganExecutionRecord),
    EndedExecution(StatementEndedExecutionRecord),
    BeganSession(SessionHistoryEvent),
}

#[derive(Clone, Debug)]
pub struct SessionHistoryEvent {
    pub id: Uuid,
    pub connected_at: EpochMillis,
    pub application_name: String,
    pub authenticated_user: String,
}
