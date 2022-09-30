// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Context};

use mysql_async::prelude::Query;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;

    let conn = state
        .mysql_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("MySQL connection '{}' not found", &name))?;

    for query in cmd.input {
        println!(">> {}", query);
        query
            .run(&mut *conn)
            .await
            .context("executing MySQL query")?;
    }

    Ok(ControlFlow::Continue)
}
