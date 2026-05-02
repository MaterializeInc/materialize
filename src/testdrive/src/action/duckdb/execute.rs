// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, anyhow};

use crate::action::duckdb::get_or_create_connection;
use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;

    let conn = get_or_create_connection(state, name).await?;

    mz_ore::task::spawn_blocking(
        || "duckdb_execute".to_string(),
        move || {
            let conn = conn.lock().map_err(|e| anyhow!("lock poisoned: {}", e))?;
            for query in &cmd.input {
                println!(">> {}", query);
                conn.execute_batch(query)
                    .with_context(|| format!("executing DuckDB query: {}", query))?;
            }
            Ok::<_, anyhow::Error>(())
        },
    )
    .await?;

    Ok(ControlFlow::Continue)
}
