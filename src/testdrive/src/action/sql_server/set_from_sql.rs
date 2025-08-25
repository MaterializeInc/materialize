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

pub async fn run_set_from_sql(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let var = cmd.args.string("var")?;
    cmd.args.done()?;

    let client = state
        .sql_server_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("connection {} not found", name.quoted()))?;

    let query = cmd.input.join("\n");
    println!(">> {}", query);
    // execute uses prepared statements, which will fail for CREATE FUNCTION/PROCEDURE etc, see
    // https://github.com/prisma/tiberius/issues/236, so using simple_query instead
    let rows = client
        .simple_query(query)
        .await
        .context("executing SQL Server query")?;

    let row = rows
        .into_iter()
        .next()
        .expect("sql-server-set-from-sql query must return exactly one row");

    let value: &str = row.try_get(0)?.expect("deserializing value as string");

    state.cmd_vars.insert(var, value.to_string());

    Ok(ControlFlow::Continue)
}
