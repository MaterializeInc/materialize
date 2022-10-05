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
use crate::parser::BuiltinCommand;

pub async fn run_skip_if(
    cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let query = cmd.input.join("\n");
    let stmt = state
        .pgclient
        .prepare(&query)
        .await
        .context("failed to prepare skip-if query")?;

    if stmt.columns().len() != 1 || *stmt.columns()[0].type_() != Type::BOOL {
        bail!("skip-if query must return exactly one boolean column");
    }

    let should_skip: bool = state
        .pgclient
        .query_one(&stmt, &[])
        .await
        .context("executing skip-if query failed")?
        .get(0);

    if should_skip {
        println!("skip-if query returned true; skipping rest of file");
        Ok(ControlFlow::Break)
    } else {
        println!("skip-if query returned false; continuing");
        Ok(ControlFlow::Continue)
    }
}
