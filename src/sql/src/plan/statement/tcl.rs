// Copyright Materialize, Inc. All rights reserved.
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
    _: StartTransactionStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::StartTransaction)
}

pub fn describe_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<StatementDesc, anyhow::Error> {
    unsupported!("SET TRANSACTION")
}

pub fn plan_set_transaction(
    _: &StatementContext,
    _: SetTransactionStatement,
) -> Result<Plan, anyhow::Error> {
    unsupported!("SET TRANSACTION")
}

pub fn describe_rollback(
    _: &StatementContext,
    _: RollbackStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_rollback(_: &StatementContext, _: RollbackStatement) -> Result<Plan, anyhow::Error> {
    Ok(Plan::AbortTransaction)
}

pub fn describe_commit(
    _: &StatementContext,
    _: CommitStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_commit(_: &StatementContext, _: CommitStatement) -> Result<Plan, anyhow::Error> {
    Ok(Plan::CommitTransaction)
}
