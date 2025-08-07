// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, anyhow};
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let split_lines = cmd.args.opt_bool("split-lines")?.unwrap_or(true);
    cmd.args.done()?;

    let client = state
        .sql_server_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("connection {} not found", name.quoted()))?;

    if split_lines {
        for query in cmd.input {
            println!(">> {}", query);
            client
                .simple_query(query)
                .await
                .context("executing SQL Server query")?;
        }
    } else {
        let query = cmd.input.join("\n");
        println!(">> {}", query);
        // execute uses prepared statements, which will fail for CREATE FUNCTION/PROCEDURE etc, see
        // https://github.com/prisma/tiberius/issues/236, so using simple_query instead
        client
            .simple_query(query)
            .await
            .context("executing SQL Server query")?;
    }

    Ok(ControlFlow::Continue)
}
