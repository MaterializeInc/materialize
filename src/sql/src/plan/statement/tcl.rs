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
//! This module houses the handlers for statements that manipulate the session,
//! like `BEGIN` and `COMMIT`.

use crate::ast::{
    CommitStatement, RollbackStatement, SetTransactionStatement, StartTransactionStatement,
    TransactionAccessMode, TransactionMode,
};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{Plan, PlanError, StartTransactionPlan};

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
    let access = verify_transaction_modes(modes)?;
    Ok(Plan::StartTransaction(StartTransactionPlan { access }))
}

pub fn describe_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<StatementDesc, PlanError> {
    bail_unsupported!("SET TRANSACTION")
}

pub fn plan_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<Plan, PlanError> {
    bail_unsupported!("SET TRANSACTION")
}

fn verify_transaction_modes(
    modes: Vec<TransactionMode>,
) -> Result<Option<TransactionAccessMode>, PlanError> {
    let mut access = None;
    for mode in modes {
        match mode {
            // Although we are only serializable, it's not wrong to accept lower isolation
            // levels because we still meet the required guarantees for those.
            TransactionMode::IsolationLevel(_) => {}
            TransactionMode::AccessMode(mode) => {
                access = Some(mode);
            }
        }
    }
    Ok(access)
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
    Ok(Plan::AbortTransaction)
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
    Ok(Plan::CommitTransaction)
}

fn verify_chain(chain: bool) -> Result<(), PlanError> {
    if chain {
        bail_unsupported!("CHAIN");
    }
    Ok(())
}
