// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Queries that validate CONNECTION objects.

use crate::ast::ValidateConnectionStatement;
use crate::names::Aug;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{Plan, PlanError, ValidateConnectionPlan};
use crate::session::vars;

pub fn describe_validate_connection(
    _: &StatementContext,
    _: ValidateConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_validate_connection(
    scx: &StatementContext,
    stmt: ValidateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_CONNECTION_VALIDATION_SYNTAX)?;
    let item = scx.get_item_by_resolved_name(&stmt.name)?;

    // Validate the target of the validate statement.
    match item.connection().cloned() {
        Ok(connection) => Ok(Plan::ValidateConnection(ValidateConnectionPlan {
            id: item.id(),
            connection,
        })),
        Err(_) => {
            sql_bail!(
                "cannot validate {} '{}'",
                item.item_type(),
                stmt.name.full_name_str()
            );
        }
    }
}
