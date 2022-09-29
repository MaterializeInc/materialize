// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Context};
use async_trait::async_trait;

use mz_ore::str::StrExt;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct ExecuteAction {
    name: String,
    queries: Vec<String>,
}

pub fn build_execute(mut cmd: BuiltinCommand) -> Result<ExecuteAction, anyhow::Error> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;
    Ok(ExecuteAction {
        name,
        queries: cmd.input,
    })
}

#[async_trait]
impl Action for ExecuteAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let client = state
            .sql_server_clients
            .get_mut(&self.name)
            .ok_or_else(|| anyhow!("connection {} not found", self.name.quoted()))?;

        for query in &self.queries {
            println!(">> {}", query);
            client
                .execute(query, &[])
                .await
                .context("executing SQL Server query")?;
        }

        Ok(ControlFlow::Continue)
    }
}
