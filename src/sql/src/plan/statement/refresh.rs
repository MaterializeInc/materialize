// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Queries that REFRESH catalog objects.

use crate::ast::RefreshSourceReferencesStatement;
use crate::names::Aug;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{Plan, PlanError, RefreshSourceReferencesPlan};
use crate::session::vars;

pub fn describe_refresh_source_references(
    _: &StatementContext,
    _: RefreshSourceReferencesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_refresh_source_references(
    scx: &StatementContext,
    stmt: RefreshSourceReferencesStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_CREATE_TABLE_FROM_SOURCE)?;
    let source_item = scx.get_item_by_resolved_name(&stmt.source)?;

    // Validate the target of the refresh statement.
    match source_item.source_desc() {
        Ok(_) => Ok(Plan::RefreshSourceReferencesPlan(
            RefreshSourceReferencesPlan {
                source_id: source_item.id(),
            },
        )),
        Err(_) => {
            sql_bail!(
                "{} '{}' is not a source",
                source_item.item_type(),
                stmt.source.full_name_str()
            );
        }
    }
}
