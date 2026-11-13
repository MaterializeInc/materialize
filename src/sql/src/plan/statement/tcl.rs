// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transaction control language (TCL).
//!
//! This module houses the handlers for statements that manipulate the session's transaction,
//! like `BEGIN` and `COMMIT`.

use std::time::Duration;

use mz_repr::adt::interval::Interval;
use mz_sql_parser::ast::TransactionIsolationLevel;

use crate::ast::{
    CommitContinualTransactionStatement, CommitStatement, ContinualCommitMode, RollbackStatement,
    SetTransactionStatement, StartContinualTransactionStatement, StartTransactionStatement,
    TransactionAccessMode, TransactionMode, Value,
};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::with_options::TryFromValue;
use crate::plan::{
    self, AbortTransactionPlan, CommitContinualTransactionPlan, CommitTransactionPlan, Plan,
    PlanError, SetTransactionPlan, StartContinualTransactionPlan, StartTransactionPlan,
    TransactionType,
};

pub fn describe_start_transaction(
    _: &StatementContext,
    _: StartTransactionStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_start_transaction(
    _: &StatementContext,
    StartTransactionStatement { modes }: StartTransactionStatement,
) -> Result<Plan, PlanError> {
    let (access, isolation_level) = verify_transaction_modes(modes)?;
    Ok(Plan::StartTransaction(StartTransactionPlan {
        access,
        isolation_level,
    }))
}

pub fn describe_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_set_transaction(
    _: &StatementContext,
    SetTransactionStatement { local, modes }: SetTransactionStatement,
) -> Result<Plan, PlanError> {
    // The sequencer applies each mode's side effect in order and rejects access
    // modes outright. Reject them here, before any plan is produced, so a
    // statement that cannot fully succeed leaves no partially-applied state
    // behind (e.g. a silently changed isolation level). This mirrors
    // `plan_start_transaction`, which validates all modes at plan time.
    if modes
        .iter()
        .any(|m| matches!(m, TransactionMode::AccessMode(_)))
    {
        bail_unsupported!("SET TRANSACTION <access-mode>");
    }
    Ok(Plan::SetTransaction(SetTransactionPlan { local, modes }))
}

fn verify_transaction_modes(
    modes: Vec<TransactionMode>,
) -> Result<
    (
        Option<TransactionAccessMode>,
        Option<TransactionIsolationLevel>,
    ),
    PlanError,
> {
    let mut access = None;
    let mut isolation = None;
    for mode in modes {
        match mode {
            TransactionMode::IsolationLevel(level) => isolation = Some(level),
            TransactionMode::AccessMode(mode) => {
                access = Some(mode);
            }
        }
    }
    Ok((access, isolation))
}

pub fn describe_rollback(
    _: &StatementContext,
    _: RollbackStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_rollback(
    _: &StatementContext,
    RollbackStatement { chain }: RollbackStatement,
) -> Result<Plan, PlanError> {
    verify_chain(chain)?;
    Ok(Plan::AbortTransaction(AbortTransactionPlan {
        transaction_type: TransactionType::Explicit,
    }))
}

pub fn describe_commit(
    _: &StatementContext,
    _: CommitStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_commit(
    _: &StatementContext,
    CommitStatement { chain }: CommitStatement,
) -> Result<Plan, PlanError> {
    verify_chain(chain)?;
    Ok(Plan::CommitTransaction(CommitTransactionPlan {
        transaction_type: TransactionType::Explicit,
    }))
}

fn verify_chain(chain: bool) -> Result<(), PlanError> {
    if chain {
        bail_unsupported!("CHAIN");
    }
    Ok(())
}

pub fn describe_start_continual_transaction(
    _: &StatementContext,
    _: StartContinualTransactionStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_start_continual_transaction(
    _: &StatementContext,
    StartContinualTransactionStatement {}: StartContinualTransactionStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::StartContinualTransaction(
        StartContinualTransactionPlan {},
    ))
}

pub fn describe_commit_continual_transaction(
    _: &StatementContext,
    _: CommitContinualTransactionStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_commit_continual_transaction(
    _: &StatementContext,
    CommitContinualTransactionStatement { commit_mode }: CommitContinualTransactionStatement,
) -> Result<Plan, PlanError> {
    let commit_mode = match commit_mode {
        ContinualCommitMode::Every(interval_value) => {
            let interval = Interval::try_from_value(Value::Interval(interval_value))
                .map_err(|e| PlanError::Unstructured(e.to_string()))?;
            let micros = interval.as_microseconds();
            if micros <= 0 {
                sql_bail!("COMMIT EVERY interval must be positive");
            }
            plan::ContinualCommitMode::Every(Duration::from_micros(u64::try_from(micros).map_err(
                |_| PlanError::Unstructured("COMMIT EVERY interval is too large".into()),
            )?))
        }
        ContinualCommitMode::Asap => plan::ContinualCommitMode::Asap,
    };
    Ok(Plan::CommitContinualTransaction(
        CommitContinualTransactionPlan { commit_mode },
    ))
}
