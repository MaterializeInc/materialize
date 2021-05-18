// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use tokio_postgres::NoTls;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct ExecuteAction {
    connection: String,
    queries: Vec<String>,
}

pub fn build_execute(mut cmd: BuiltinCommand) -> Result<ExecuteAction, String> {
    let connection = cmd.args.string("connection")?;
    cmd.args.done()?;
    Ok(ExecuteAction {
        connection,
        queries: cmd.input,
    })
}

#[async_trait]
impl Action for ExecuteAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, _: &mut State) -> Result<(), String> {
        let (client, conn) = tokio_postgres::connect(&self.connection, NoTls)
            .await
            .map_err(|e| format!("connecting to postgres: {}", e))?;
        println!(
            "Executing queries against PostgreSQL server at {}...",
            self.connection
        );
        let conn_handle = tokio::spawn(conn);
        for query in &self.queries {
            println!(">> {}", query);
            client
                .batch_execute(query)
                .await
                .map_err(|e| format!("executing postgres query: {}", e))?;
        }
        drop(client);
        conn_handle
            .await
            .unwrap()
            .map_err(|e| format!("postgres connection error: {}", e))
    }
}
