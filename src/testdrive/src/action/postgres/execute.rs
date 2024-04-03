// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, bail, Context};
use mz_ore::task;
use tokio_postgres::Client;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

async fn execute_input(cmd: BuiltinCommand, client: &Client) -> Result<(), anyhow::Error> {
    for query in cmd.input {
        println!(">> {}", query);
        client
            .batch_execute(&query)
            .await
            .context("executing postgres query")?;
    }
    Ok(())
}

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let connection = cmd.args.string("connection")?;
    let background = cmd.args.opt_bool("background")?.unwrap_or(false);
    cmd.args.done()?;

    match (connection.starts_with("postgres://"), background) {
        (true, true) => {
            let (client_inner, _) = postgres_client(&connection, state.default_timeout).await?;
            task::spawn(|| "postgres-execute", async move {
                match execute_input(cmd, &client_inner).await {
                    Ok(_) => {}
                    Err(e) => println!("Error in backgrounded postgres-execute query: {e}"),
                }
            });
        }
        (false, true) => bail!("cannot use 'background' arg with referenced connection"),
        (true, false) => {
            let (client_inner, _) = postgres_client(&connection, state.default_timeout).await?;
            execute_input(cmd, &client_inner).await?;
        }
        (false, false) => {
            let client = state
                .postgres_clients
                .get(&connection)
                .ok_or_else(|| anyhow!("connection '{}' not found", &connection))?;
            execute_input(cmd, client).await?;
        }
    }

    Ok(ControlFlow::Continue)
}
