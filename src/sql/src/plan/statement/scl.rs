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

use mz_repr::{CatalogItemId, RelationDesc, RelationVersionSelector, ScalarType};
use mz_sql_parser::ast::InspectShardStatement;
use std::time::Duration;
use uncased::UncasedStr;

use crate::ast::display::AstDisplay;
use crate::ast::{
    CloseStatement, DeallocateStatement, DeclareStatement, DiscardStatement, DiscardTarget,
    ExecuteStatement, FetchOption, FetchOptionName, FetchStatement, PrepareStatement,
    ResetVariableStatement, SetVariableStatement, SetVariableTo, ShowVariableStatement,
};
use crate::names::{self, Aug};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{
    ClosePlan, DeallocatePlan, DeclarePlan, ExecutePlan, ExecuteTimeout, FetchPlan,
    InspectShardPlan, Params, Plan, PlanError, PreparePlan, ResetVariablePlan, SetVariablePlan,
    ShowVariablePlan, VariableValue, describe, query,
};
use crate::session::vars;
use crate::session::vars::{IsolationLevel, SCHEMA_ALIAS, TRANSACTION_ISOLATION_VAR_NAME};

pub fn describe_set_variable(
    _: &StatementContext,
    _: SetVariableStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_set_variable(
    scx: &StatementContext,
    SetVariableStatement {
        local,
        variable,
        to,
    }: SetVariableStatement,
) -> Result<Plan, PlanError> {
    let value = plan_set_variable_to(to)?;
    let name = variable.into_string();

    if let VariableValue::Values(values) = &value {
        if let Some(value) = values.first() {
            if name.as_str() == TRANSACTION_ISOLATION_VAR_NAME
                && value == IsolationLevel::StrongSessionSerializable.as_str()
            {
                scx.require_feature_flag(&vars::ENABLE_SESSION_TIMELINES)?;
            }
        }
    }

    Ok(Plan::SetVariable(SetVariablePlan { name, value, local }))
}

pub fn plan_set_variable_to(to: SetVariableTo) -> Result<VariableValue, PlanError> {
    match to {
        SetVariableTo::Default => Ok(VariableValue::Default),
        SetVariableTo::Values(values) => {
            // Per PostgreSQL, string literals and identifiers are treated
            // equivalently during `SET`. We retain only the underlying string
            // value of each element in the list. It's our caller's
            // responsibility to figure out how to set the variable to the
            // provided list of values using variable-specific logic.
            let values = values
                .into_iter()
                .map(|v| v.into_unquoted_value())
                .collect();
            Ok(VariableValue::Values(values))
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
        RelationDesc::builder()
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("setting", ScalarType::String.nullable(false))
            .with_column("description", ScalarType::String.nullable(false))
            .finish()
    } else if variable.as_str() == SCHEMA_ALIAS {
        RelationDesc::builder()
            .with_column(variable.as_str(), ScalarType::String.nullable(true))
            .finish()
    } else {
        RelationDesc::builder()
            .with_column(variable.as_str(), ScalarType::String.nullable(false))
            .finish()
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

pub fn describe_inspect_shard(
    _: &StatementContext,
    InspectShardStatement { .. }: InspectShardStatement,
) -> Result<StatementDesc, PlanError> {
    let desc = RelationDesc::builder()
        .with_column("state", ScalarType::Jsonb.nullable(false))
        .finish();
    Ok(StatementDesc::new(Some(desc)))
}

pub fn plan_inspect_shard(
    scx: &StatementContext,
    InspectShardStatement { id }: InspectShardStatement,
) -> Result<Plan, PlanError> {
    let id: CatalogItemId = id.parse().map_err(|_| sql_err!("invalid shard id"))?;
    // Always inspect the shard at the latest GlobalId.
    let gid = scx
        .catalog
        .try_get_item(&id)
        .ok_or_else(|| sql_err!("item doesn't exist"))?
        .at_version(RelationVersionSelector::Latest)
        .global_id();
    Ok(Plan::InspectShard(InspectShardPlan { id: gid }))
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
    scx: &StatementContext,
    DeclareStatement { stmt, .. }: DeclareStatement<Aug>,
    param_types_in: &[Option<ScalarType>],
) -> Result<StatementDesc, PlanError> {
    let (stmt_resolved, _) = names::resolve(scx.catalog, *stmt)?;
    // Get the desc for the inner statement, but only for its parameters. The outer DECLARE doesn't
    // return any rows itself when executed.
    let desc = describe(scx.pcx()?, scx.catalog, stmt_resolved, param_types_in)?;
    // The outer describe fn calls scx.finalize_param_types, so we need to transfer the inner desc's
    // params to this scx.
    for (i, ty) in desc.param_types.into_iter().enumerate() {
        scx.param_types.borrow_mut().insert(i + 1, ty);
    }
    Ok(StatementDesc::new(None))
}

pub fn plan_declare(
    _: &StatementContext,
    DeclareStatement { name, stmt, sql }: DeclareStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    Ok(Plan::Declare(DeclarePlan {
        name: name.to_string(),
        stmt: *stmt,
        sql,
        params: params.clone(),
    }))
}

pub fn describe_fetch(
    _: &StatementContext,
    _: FetchStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    panic!("ggggggggggggggggggggggggggggggggggggg");
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(FetchOption, (Timeout, Duration));

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
            const DAY: Duration = Duration::from_secs(60 * 60 * 24);
            if timeout > DAY {
                sql_bail!("timeout out of range: {}s", timeout.as_secs_f64());
            }
            ExecuteTimeout::Seconds(timeout.as_secs_f64())
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
    PrepareStatement { name, stmt, sql }: PrepareStatement<Aug>,
) -> Result<Plan, PlanError> {
    // TODO: PREPARE supports specifying param types.
    let param_types = [];
    let (stmt_resolved, _) = names::resolve(scx.catalog, *stmt.clone())?;
    let desc = describe(scx.pcx()?, scx.catalog, stmt_resolved, &param_types)?;
    Ok(Plan::Prepare(PreparePlan {
        name: name.to_string(),
        stmt: *stmt,
        desc,
        sql,
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
    // See database-issues#2563.
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
