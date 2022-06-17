// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Session control language (SCL).
//!
//! This module houses the handlers for statements that manipulate the session,
//! like `DISCARD` and `SET`.

use std::collections::HashSet;

use anyhow::{anyhow, bail};
use uncased::UncasedStr;

use mz_repr::adt::interval::Interval;
use mz_repr::{RelationDesc, ScalarType};

use crate::ast::display::AstDisplay;
use crate::ast::{
    CloseStatement, DeallocateStatement, DeclareStatement, DiscardStatement, DiscardTarget,
    ExecuteStatement, FetchOption, FetchOptionName, FetchStatement, PrepareStatement,
    ResetVariableStatement, SetVariableStatement, ShowVariableStatement,
};
use crate::names::{self, Aug};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::with_options::TryFromValue;
use crate::plan::{
    describe, query, ClosePlan, DeallocatePlan, DeclarePlan, ExecutePlan, ExecuteTimeout,
    FetchPlan, Plan, PreparePlan, ResetVariablePlan, SetVariablePlan, ShowVariablePlan,
};

pub fn describe_set_variable(
    _: &StatementContext,
    _: SetVariableStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_set_variable(
    _: &StatementContext,
    SetVariableStatement {
        local,
        variable,
        value,
    }: SetVariableStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::SetVariable(SetVariablePlan {
        name: variable.to_string(),
        value,
        local,
    }))
}

pub fn describe_reset_variable(
    _: &StatementContext,
    _: ResetVariableStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_reset_variable(
    _: &StatementContext,
    ResetVariableStatement { variable }: ResetVariableStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::ResetVariable(ResetVariablePlan {
        name: variable.to_string(),
    }))
}

pub fn describe_show_variable(
    _: &StatementContext,
    ShowVariableStatement { variable, .. }: ShowVariableStatement,
) -> Result<StatementDesc, anyhow::Error> {
    let desc = if variable.as_str() == UncasedStr::new("ALL") {
        RelationDesc::empty()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("setting", ScalarType::String.nullable(false))
            .with_column("description", ScalarType::String.nullable(false))
    } else {
        RelationDesc::empty().with_column(variable.as_str(), ScalarType::String.nullable(false))
    };
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_show_variable(
    _: &StatementContext,
    ShowVariableStatement { variable }: ShowVariableStatement,
) -> Result<Plan, anyhow::Error> {
    if variable.as_str() == UncasedStr::new("ALL") {
        Ok(Plan::ShowAllVariables)
    } else {
        Ok(Plan::ShowVariable(ShowVariablePlan {
            name: variable.to_string(),
        }))
    }
}

pub fn describe_discard(
    _: &StatementContext,
    _: DiscardStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_discard(
    _: &StatementContext,
    DiscardStatement { target }: DiscardStatement,
) -> Result<Plan, anyhow::Error> {
    match target {
        DiscardTarget::All => Ok(Plan::DiscardAll),
        DiscardTarget::Temp => Ok(Plan::DiscardTemp),
        DiscardTarget::Sequences => bail_unsupported!("DISCARD SEQUENCES"),
        DiscardTarget::Plans => bail_unsupported!("DISCARD PLANS"),
    }
}

pub fn describe_declare(
    _: &StatementContext,
    _: DeclareStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_declare(
    _: &StatementContext,
    DeclareStatement { name, stmt }: DeclareStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::Declare(DeclarePlan {
        name: name.to_string(),
        stmt: *stmt,
    }))
}

pub fn describe_fetch(
    _: &StatementContext,
    _: FetchStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(FetchOption, (Timeout, Interval));

pub fn plan_fetch(
    _: &StatementContext,
    FetchStatement {
        name,
        count,
        options,
    }: FetchStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let FetchOptionExtracted { timeout } = options.try_into()?;
    let timeout = match timeout {
        Some(timeout) => {
            // Limit FETCH timeouts to 1 day. If users have a legitimate need it can be
            // bumped. If we do bump it, ensure that the new upper limit is within the
            // bounds of a tokio time future, otherwise it'll panic.
            const SECS_PER_DAY: f64 = 60f64 * 60f64 * 24f64;
            let timeout_secs = timeout.as_epoch_seconds::<f64>();
            if !timeout_secs.is_finite() || timeout_secs < 0f64 || timeout_secs > SECS_PER_DAY {
                bail!("timeout out of range: {:#}", timeout);
            }
            ExecuteTimeout::Seconds(timeout_secs)
        }
        // FETCH defaults to WaitOnce.
        None => ExecuteTimeout::WaitOnce,
    };
    Ok(Plan::Fetch(FetchPlan {
        name: name.to_string(),
        count,
        timeout,
    }))
}

pub fn describe_close(
    _: &StatementContext,
    _: CloseStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_close(
    _: &StatementContext,
    CloseStatement { name }: CloseStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::Close(ClosePlan {
        name: name.to_string(),
    }))
}

pub fn describe_prepare(
    _: &StatementContext,
    _: PrepareStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_prepare(
    scx: &StatementContext,
    PrepareStatement { name, stmt }: PrepareStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    // TODO: PREPARE supports specifying param types.
    let param_types = [];
    let (stmt_resolved, _) = names::resolve(scx.catalog, *stmt.clone())?;
    let desc = describe(scx.pcx()?, scx.catalog, stmt_resolved, &param_types)?;
    Ok(Plan::Prepare(PreparePlan {
        name: name.to_string(),
        stmt: *stmt,
        desc,
    }))
}

pub fn describe_execute(
    scx: &StatementContext,
    stmt: ExecuteStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    // The evaluation of the statement doesn't happen until it gets to coord. That
    // means if the statement is now invalid due to an object having been dropped,
    // describe is unable to notice that. This is currently an existing problem
    // with prepared statements over pgwire as well, so we can leave this for now.
    // See #8397.
    Ok(plan_execute_desc(scx, stmt)?.0.clone())
}

pub fn plan_execute(
    scx: &StatementContext,
    stmt: ExecuteStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    Ok(plan_execute_desc(scx, stmt)?.1)
}

fn plan_execute_desc<'a>(
    scx: &'a StatementContext,
    ExecuteStatement { name, params }: ExecuteStatement<Aug>,
) -> Result<(&'a StatementDesc, Plan), anyhow::Error> {
    let name = name.to_string();
    let desc = match scx.catalog.get_prepared_statement_desc(&name) {
        Some(desc) => desc,
        // TODO(mjibson): use CoordError::UnknownPreparedStatement.
        None => bail!("unknown prepared statement {}", name),
    };
    Ok((
        desc,
        Plan::Execute(ExecutePlan {
            name,
            params: query::plan_params(scx, params, desc)?,
        }),
    ))
}

pub fn describe_deallocate(
    _: &StatementContext,
    _: DeallocateStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_deallocate(
    _: &StatementContext,
    DeallocateStatement { name }: DeallocateStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::Deallocate(DeallocatePlan {
        name: name.map(|name| name.to_string()),
    }))
}
