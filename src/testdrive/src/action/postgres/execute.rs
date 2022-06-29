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

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

pub struct ExecuteAction {
    connection: String,
    queries: Vec<String>,
}

pub fn build_execute(mut cmd: BuiltinCommand) -> Result<ExecuteAction, anyhow::Error> {
    let connection = cmd.args.string("connection")?;
    cmd.args.done()?;
    Ok(ExecuteAction {
        connection,
        queries: cmd.input,
    })
}

#[async_trait]
impl Action for ExecuteAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let client;
        let client = if self.connection.starts_with("postgres://") {
            let (client_inner, _) = postgres_client(&self.connection).await?;
            client = client_inner;
            &client
        } else {
            state
                .postgres_clients
                .get(&self.connection)
                .ok_or_else(|| anyhow!("connection '{}' not found", &self.connection))?
        };

        for query in &self.queries {
            println!(">> {}", query);
            client
                .batch_execute(query)
                .await
                .context("executing postgres query")?;
        }

        Ok(ControlFlow::Continue)
    }
}
