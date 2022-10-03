// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Context};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let connection = cmd.args.string("connection")?;
    cmd.args.done()?;

    let client;
    let client = if connection.starts_with("postgres://") {
        let (client_inner, _) = postgres_client(&connection).await?;
        client = client_inner;
        &client
    } else {
        state
            .postgres_clients
            .get(&connection)
            .ok_or_else(|| anyhow!("connection '{}' not found", &connection))?
    };

    for query in cmd.input {
        println!(">> {}", query);
        client
            .batch_execute(&query)
            .await
            .context("executing postgres query")?;
    }

    Ok(ControlFlow::Continue)
}
