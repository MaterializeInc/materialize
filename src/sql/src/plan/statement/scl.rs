// Copyright Materialize, Inc. All rights reserved.
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

use std::convert::TryFrom;

use anyhow::bail;

use repr::adt::interval::Interval;
use repr::{RelationDesc, ScalarType};

use crate::ast::{
    CloseStatement, DeclareStatement, DiscardStatement, DiscardTarget, FetchStatement, Raw,
    SetVariableStatement, SetVariableValue, ShowVariableStatement, Value,
};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{ExecuteTimeout, Plan};

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
    if local {
        unsupported!("SET LOCAL");
    }
    Ok(Plan::SetVariable {
        name: variable.to_string(),
        value: match value {
            SetVariableValue::Literal(Value::String(s)) => s,
            SetVariableValue::Literal(lit) => lit.to_string(),
            SetVariableValue::Ident(ident) => ident.into_string(),
        },
    })
}

pub fn describe_show_variable(
    _: &StatementContext,
    ShowVariableStatement { variable, .. }: ShowVariableStatement,
) -> Result<StatementDesc, anyhow::Error> {
    let desc = if variable.as_str() == unicase::Ascii::new("ALL") {
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
    if variable.as_str() == unicase::Ascii::new("ALL") {
        Ok(Plan::ShowAllVariables)
    } else {
        Ok(Plan::ShowVariable(variable.to_string()))
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
        DiscardTarget::Sequences => unsupported!("DISCARD SEQUENCES"),
        DiscardTarget::Plans => unsupported!("DISCARD PLANS"),
    }
}

pub fn describe_declare(
    _: &StatementContext,
    _: DeclareStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_declare(
    _: &StatementContext,
    DeclareStatement { name, stmt }: DeclareStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::Declare {
        name: name.to_string(),
        stmt: *stmt,
    })
}

with_options! {
    struct FetchOptions {
        timeout: Interval,
    }
}

pub fn describe_fetch(
    _: &StatementContext,
    _: FetchStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_fetch(
    _: &StatementContext,
    FetchStatement {
        name,
        count,
        options,
    }: FetchStatement,
) -> Result<Plan, anyhow::Error> {
    let options = FetchOptions::try_from(options)?;
    let timeout = match options.timeout {
        Some(timeout) => {
            // Limit FETCH timeouts to 1 day. If users have a legitimate need it can be
            // bumped. If we do bump it, ensure that the new upper limit is within the
            // bounds of a tokio time future, otherwise it'll panic.
            const SECS_PER_DAY: f64 = 60f64 * 60f64 * 24f64;
            let timeout_secs = timeout.as_seconds();
            if !timeout_secs.is_finite() || timeout_secs < 0f64 || timeout_secs > SECS_PER_DAY {
                bail!("timeout out of range: {:#}", timeout);
            }
            ExecuteTimeout::Seconds(timeout_secs)
        }
        // FETCH defaults to WaitOnce.
        None => ExecuteTimeout::WaitOnce,
    };
    Ok(Plan::Fetch {
        name: name.to_string(),
        count,
        timeout,
    })
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
    Ok(Plan::Close {
        name: name.to_string(),
    })
}
