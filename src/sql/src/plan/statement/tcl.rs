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

use crate::ast::display::AstDisplay;
use crate::ast::{
    CommitStatement, RollbackStatement, SetTransactionStatement, StartTransactionStatement,
    TransactionMode,
};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::Plan;

pub fn describe_start_transaction(
    _: &StatementContext,
    _: StartTransactionStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_start_transaction(
    _: &StatementContext,
    StartTransactionStatement { modes }: StartTransactionStatement,
) -> Result<Plan, anyhow::Error> {
    verify_transaction_modes(&modes)?;
    Ok(Plan::StartTransaction)
}

pub fn describe_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<StatementDesc, anyhow::Error> {
    bail_unsupported!("SET TRANSACTION")
}

pub fn plan_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<Plan, anyhow::Error> {
    bail_unsupported!("SET TRANSACTION")
}

fn verify_transaction_modes(modes: &[TransactionMode]) -> Result<(), anyhow::Error> {
    for mode in modes {
        match mode {
            // Although we are only serializable, it's not wrong to accept lower isolation
            // levels because we still meet the required guarantees for those.
            TransactionMode::IsolationLevel(_) => {}
            _ => bail_unsupported!(format!("transaction mode {}", mode.to_ast_string())),
        }
    }
    Ok(())
}

pub fn describe_rollback(
    _: &StatementContext,
    _: RollbackStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_rollback(
    _: &StatementContext,
    RollbackStatement { chain }: RollbackStatement,
) -> Result<Plan, anyhow::Error> {
    verify_chain(chain)?;
    Ok(Plan::AbortTransaction)
}

pub fn describe_commit(
    _: &StatementContext,
    _: CommitStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_commit(
    _: &StatementContext,
    CommitStatement { chain }: CommitStatement,
) -> Result<Plan, anyhow::Error> {
    verify_chain(chain)?;
    Ok(Plan::CommitTransaction)
}

fn verify_chain(chain: bool) -> Result<(), anyhow::Error> {
    if chain {
        bail_unsupported!("CHAIN");
    }
    Ok(())
}
