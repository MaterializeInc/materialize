// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_ore::now::EpochMillis;
use mz_repr::{GlobalId, RowIterator};
use mz_sql_parser::ast::StatementKind;
use uuid::Uuid;

use crate::session::TransactionId;
use crate::{AdapterError, ExecuteResponse};

#[derive(Clone, Debug)]
pub enum StatementLifecycleEvent {
    ExecutionBegan,
    OptimizationBegan,
    OptimizationFinished,
    StorageDependenciesFinished,
    ComputeDependenciesFinished,
    ExecutionFinished,
}

impl StatementLifecycleEvent {
    pub fn as_str(&self) -> &str {
        match self {
            Self::ExecutionBegan => "execution-began",
            Self::OptimizationBegan => "optimization-began",
            Self::OptimizationFinished => "optimization-finished",
            Self::StorageDependenciesFinished => "storage-dependencies-finished",
            Self::ComputeDependenciesFinished => "compute-dependencies-finished",
            Self::ExecutionFinished => "execution-finished",
        }
    }
}

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
    pub cluster_id: Option<ClusterId>,
    pub cluster_name: Option<String>,
    pub database_name: String,
    pub search_path: Vec<String>,
    pub application_name: String,
    pub transaction_isolation: String,
    pub execution_timestamp: Option<EpochMillis>,
    pub transaction_id: TransactionId,
    pub transient_index_id: Option<GlobalId>,
    pub mz_version: String,
}

#[derive(Clone, Copy, Debug)]
pub enum StatementExecutionStrategy {
    /// The statement was executed by spinning up a dataflow.
    Standard,
    /// The statement was executed by reading from an existing
    /// arrangement.
    FastPath,
    /// Experimental: The statement was executed by reading from an existing
    /// persist collection.
    PersistFastPath,
    /// The statement was determined to be constant by
    /// environmentd, and not sent to a cluster.
    Constant,
}

impl StatementExecutionStrategy {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::FastPath => "fast-path",
            Self::PersistFastPath => "persist-fast-path",
            Self::Constant => "constant",
        }
    }
}

#[derive(Clone, Debug)]
pub enum StatementEndedExecutionReason {
    Success {
        result_size: Option<u64>,
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
    pub sql_hash: [u8; 32],
    pub name: String,
    pub session_id: Uuid,
    pub prepared_at: EpochMillis,
    pub kind: Option<StatementKind>,
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

impl From<&Result<ExecuteResponse, AdapterError>> for StatementEndedExecutionReason {
    fn from(value: &Result<ExecuteResponse, AdapterError>) -> StatementEndedExecutionReason {
        match value {
            Ok(resp) => resp.into(),
            Err(e) => StatementEndedExecutionReason::Errored {
                error: e.to_string(),
            },
        }
    }
}

impl From<&ExecuteResponse> for StatementEndedExecutionReason {
    fn from(value: &ExecuteResponse) -> StatementEndedExecutionReason {
        match value {
            ExecuteResponse::CopyTo { resp, .. } => match resp.as_ref() {
                // NB [btv]: It's not clear that this combination
                // can ever actually happen.
                ExecuteResponse::SendingRowsImmediate { rows, .. } => {
                    // Note(parkmycar): It potentially feels bad here to iterate over the entire
                    // iterator _just_ to get the encoded result size. As noted above, it's not
                    // entirely clear this case ever happens, so the simplicity is worth it.
                    let result_size: usize = rows.box_clone().map(|row| row.byte_len()).sum();
                    StatementEndedExecutionReason::Success {
                        result_size: Some(u64::cast_from(result_size)),
                        rows_returned: Some(u64::cast_from(rows.count())),
                        execution_strategy: Some(StatementExecutionStrategy::Constant),
                    }
                }
                ExecuteResponse::SendingRowsStreaming { .. } => {
                    panic!("SELECTs terminate on peek finalization, not here.")
                }
                ExecuteResponse::Subscribing { .. } => {
                    panic!("SUBSCRIBEs terminate in the protocol layer, not here.")
                }
                _ => panic!("Invalid COPY response type"),
            },
            ExecuteResponse::CopyFrom { .. } => {
                panic!("COPY FROMs terminate in the protocol layer, not here.")
            }
            ExecuteResponse::Fetch { .. } => {
                panic!("FETCHes terminate after a follow-up message is sent.")
            }
            ExecuteResponse::SendingRowsStreaming { .. } => {
                panic!("SELECTs terminate on peek finalization, not here.")
            }
            ExecuteResponse::Subscribing { .. } => {
                panic!("SUBSCRIBEs terminate in the protocol layer, not here.")
            }

            ExecuteResponse::SendingRowsImmediate { rows, .. } => {
                // Note(parkmycar): It potentially feels bad here to iterate over the entire
                // iterator _just_ to get the encoded result size, the number of Rows returned here
                // shouldn't be too large though. An alternative is to pre-compute some of the
                // result size, but that would require always decoding Rows to handle projecting
                // away columns, which has a negative impact for much larger response sizes.
                let result_size: usize = rows.box_clone().map(|row| row.byte_len()).sum();
                StatementEndedExecutionReason::Success {
                    result_size: Some(u64::cast_from(result_size)),
                    rows_returned: Some(u64::cast_from(rows.count())),
                    execution_strategy: Some(StatementExecutionStrategy::Constant),
                }
            }

            ExecuteResponse::AlteredDefaultPrivileges
            | ExecuteResponse::AlteredObject(_)
            | ExecuteResponse::AlteredRole
            | ExecuteResponse::AlteredSystemConfiguration
            | ExecuteResponse::ClosedCursor
            | ExecuteResponse::Comment
            | ExecuteResponse::Copied(_)
            | ExecuteResponse::CreatedConnection
            | ExecuteResponse::CreatedDatabase
            | ExecuteResponse::CreatedSchema
            | ExecuteResponse::CreatedRole
            | ExecuteResponse::CreatedCluster
            | ExecuteResponse::CreatedClusterReplica
            | ExecuteResponse::CreatedIndex
            | ExecuteResponse::CreatedIntrospectionSubscribe
            | ExecuteResponse::CreatedSecret
            | ExecuteResponse::CreatedSink
            | ExecuteResponse::CreatedSource
            | ExecuteResponse::CreatedTable
            | ExecuteResponse::CreatedView
            | ExecuteResponse::CreatedViews
            | ExecuteResponse::CreatedMaterializedView
            | ExecuteResponse::CreatedContinualTask
            | ExecuteResponse::CreatedType
            | ExecuteResponse::CreatedNetworkPolicy
            | ExecuteResponse::Deallocate { .. }
            | ExecuteResponse::DeclaredCursor
            | ExecuteResponse::Deleted(_)
            | ExecuteResponse::DiscardedTemp
            | ExecuteResponse::DiscardedAll
            | ExecuteResponse::DroppedObject(_)
            | ExecuteResponse::DroppedOwned
            | ExecuteResponse::EmptyQuery
            | ExecuteResponse::GrantedPrivilege
            | ExecuteResponse::GrantedRole
            | ExecuteResponse::Inserted(_)
            | ExecuteResponse::Prepare
            | ExecuteResponse::Raised
            | ExecuteResponse::ReassignOwned
            | ExecuteResponse::RevokedPrivilege
            | ExecuteResponse::RevokedRole
            | ExecuteResponse::SetVariable { .. }
            | ExecuteResponse::StartedTransaction
            | ExecuteResponse::TransactionCommitted { .. }
            | ExecuteResponse::TransactionRolledBack { .. }
            | ExecuteResponse::Updated(_)
            | ExecuteResponse::ValidatedConnection { .. } => {
                StatementEndedExecutionReason::Success {
                    result_size: None,
                    rows_returned: None,
                    execution_strategy: None,
                }
            }
        }
    }
}
