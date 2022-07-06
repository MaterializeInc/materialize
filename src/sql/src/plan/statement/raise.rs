// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Queries that send notices/errors to the client
//!
//! This module houses the handlers for the `RAISE` suite of statements, like
//! `RAISE WARNING` and `RAISE INFO`.

use crate::ast::RaiseStatement;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{Plan, PlanError, RaisePlan};

pub fn describe_raise(_: &StatementContext, _: RaiseStatement) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_raise(scx: &StatementContext, r: RaiseStatement) -> Result<Plan, PlanError> {
    scx.require_unsafe_mode("RAISE statement")?;
    Ok(Plan::Raise(RaisePlan {
        severity: r.severity,
    }))
}
