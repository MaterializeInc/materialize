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

use mz_sql_parser::ast::{SetVariableTo, SetVariableValue, Value};
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
    FetchPlan, Plan, PlanError, PreparePlan, ResetVariablePlan, SetVariablePlan, ShowVariablePlan,
    VariableValue,
};

pub fn describe_set_variable(
    _: &StatementContext,
    _: SetVariableStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_set_variable(
    _: &StatementContext,
    SetVariableStatement {
        local,
        variable,
        to,
    }: SetVariableStatement,
) -> Result<Plan, PlanError> {
    let value = plan_set_variable_to(to)?;
    Ok(Plan::SetVariable(SetVariablePlan {
        name: variable.into_string(),
        value,
        local,
    }))
}

pub fn plan_set_variable_to(to: SetVariableTo) -> Result<VariableValue, PlanError> {
    match to {
        SetVariableTo::Default => Ok(VariableValue::Default),
        SetVariableTo::Values(values) => {
            let mut out = vec![];
            // Postgres flattens multiple SET values to a single string, and
            // cares about the underlying variable's flags (guc_tables.c) when
            // generating that string. We avoid needing to do that by passing a
            // Vec instead. Each SET var is in charge of if it wants to split
            // each member of the Vec or not.
            for value in values {
                out.push(match value {
                    // lit.to_string will quote a Value::String, so get the unquoted version.
                    SetVariableValue::Literal(Value::String(s)) => s,
                    SetVariableValue::Literal(lit) => lit.to_string(),
                    SetVariableValue::Ident(ident) => ident.into_string(),
                });
            }
            Ok(VariableValue::Values(out))
        }
    }
}

pub fn describe_reset_variable(
    _: &StatementContext,
    _: ResetVariableStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_reset_variable(
    _: &StatementContext,
    ResetVariableStatement { variable }: ResetVariableStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::ResetVariable(ResetVariablePlan {
        name: variable.to_string(),
    }))
}

pub fn describe_show_variable(
    _: &StatementContext,
    ShowVariableStatement { variable, .. }: ShowVariableStatement,
) -> Result<StatementDesc, PlanError> {
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
) -> Result<Plan, PlanError> {
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
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_discard(
    _: &StatementContext,
    DiscardStatement { target }: DiscardStatement,
) -> Result<Plan, PlanError> {
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
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_declare(
    _: &StatementContext,
    DeclareStatement { name, stmt }: DeclareStatement<Aug>,
) -> Result<Plan, PlanError> {
    Ok(Plan::Declare(DeclarePlan {
        name: name.to_string(),
        stmt: *stmt,
    }))
}

pub fn describe_fetch(
    _: &StatementContext,
    _: FetchStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
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
) -> Result<Plan, PlanError> {
    let FetchOptionExtracted { timeout, .. } = options.try_into()?;
    let timeout = match timeout {
        Some(timeout) => {
            // Limit FETCH timeouts to 1 day. If users have a legitimate need it can be
            // bumped. If we do bump it, ensure that the new upper limit is within the
            // bounds of a tokio time future, otherwise it'll panic.
            const SECS_PER_DAY: f64 = 60f64 * 60f64 * 24f64;
            let timeout_secs = timeout.as_epoch_seconds::<f64>();
            if !timeout_secs.is_finite() || timeout_secs < 0f64 || timeout_secs > SECS_PER_DAY {
                sql_bail!("timeout out of range: {:#}", timeout);
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

pub fn describe_close(_: &StatementContext, _: CloseStatement) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_close(
    _: &StatementContext,
    CloseStatement { name }: CloseStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::Close(ClosePlan {
        name: name.to_string(),
    }))
}

pub fn describe_prepare(
    _: &StatementContext,
    _: PrepareStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_prepare(
    scx: &StatementContext,
    PrepareStatement { name, stmt }: PrepareStatement<Aug>,
) -> Result<Plan, PlanError> {
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
) -> Result<StatementDesc, PlanError> {
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
) -> Result<Plan, PlanError> {
    Ok(plan_execute_desc(scx, stmt)?.1)
}

fn plan_execute_desc<'a>(
    scx: &'a StatementContext,
    ExecuteStatement { name, params }: ExecuteStatement<Aug>,
) -> Result<(&'a StatementDesc, Plan), PlanError> {
    let name = name.to_string();
    let desc = match scx.catalog.get_prepared_statement_desc(&name) {
        Some(desc) => desc,
        // TODO(mjibson): use CoordError::UnknownPreparedStatement.
        None => sql_bail!("unknown prepared statement {}", name),
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
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_deallocate(
    _: &StatementContext,
    DeallocateStatement { name }: DeallocateStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::Deallocate(DeallocatePlan {
        name: name.map(|name| name.to_string()),
    }))
}
