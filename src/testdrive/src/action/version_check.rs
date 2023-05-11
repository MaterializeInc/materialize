// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use tokio_postgres::types::Type;

use crate::action::{ControlFlow, State};

pub async fn run_version_check(
    min_version: i32,
    max_version: i32,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let query = "SELECT mz_version_num()";
    let stmt = state
        .pgclient
        .prepare(query)
        .await
        .context("failed to prepare version-check query")?;
    if stmt.columns().len() != 1 || *stmt.columns()[0].type_() != Type::INT4 {
        bail!(
            "version-check query must return exactly one int column, but is {}",
            *stmt.columns()[0].type_()
        );
    }
    let actual_version: i32 = state
        .pgclient
        .query_one(&stmt, &[])
        .await
        .context("executing version-check query failed")?
        .get(0);
    state.skip_next_command = actual_version < min_version || actual_version > max_version;
    Ok(ControlFlow::Continue)
}
