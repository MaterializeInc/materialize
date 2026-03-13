// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{Context, anyhow};
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

/// Check if an error is a SQL Server deadlock (error code 1205).
fn is_deadlock_error(err: &anyhow::Error) -> bool {
    // Use alternate Display format `{:#}` to get the full anyhow error chain,
    // not just the outermost context message.
    let msg = format!("{:#}", err);
    // SQL Server deadlock victim error code 1205 appears in the error chain.
    msg.contains("1205") && msg.contains("deadlock")
}

/// Maximum number of retries for deadlock errors.
const DEADLOCK_MAX_RETRIES: usize = 5;

/// Initial backoff duration between deadlock retries.
const DEADLOCK_INITIAL_BACKOFF: Duration = Duration::from_millis(500);

async fn execute_with_deadlock_retry(
    client: &mut mz_sql_server_util::Client,
    query: &str,
) -> Result<(), anyhow::Error> {
    let mut backoff = DEADLOCK_INITIAL_BACKOFF;
    for attempt in 0..=DEADLOCK_MAX_RETRIES {
        match client
            .simple_query(query.to_string())
            .await
            .context("executing SQL Server query")
        {
            Ok(_) => return Ok(()),
            Err(err) if is_deadlock_error(&err) && attempt < DEADLOCK_MAX_RETRIES => {
                println!(
                    ">> deadlock detected (attempt {}/{}), retrying after {:?}",
                    attempt + 1,
                    DEADLOCK_MAX_RETRIES,
                    backoff,
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!()
}

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let split_lines = cmd.args.opt_bool("split-lines")?.unwrap_or(true);
    // When set, wraps the SQL in a Transaction and then drops it without
    // calling commit or rollback. Used to test that Transaction::drop sends
    // ROLLBACK correctly.
    let abandon_txn = cmd.args.opt_bool("abandon-txn")?.unwrap_or(false);
    cmd.args.done()?;

    let client = state
        .sql_server_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("connection {} not found", name.quoted()))?;

    if abandon_txn {
        let mut txn = client
            .transaction()
            .await
            .context("begin transaction for abandon-txn")?;
        if split_lines {
            for query in &cmd.input {
                println!(">> (abandon-txn) {}", query);
                txn.simple_query(query.to_string())
                    .await
                    .context("executing SQL Server query in transaction")?;
            }
        } else {
            let query = cmd.input.join("\n");
            println!(">> (abandon-txn) {}", query);
            txn.simple_query(query)
                .await
                .context("executing SQL Server query in transaction")?;
        }
        // Transaction dropped here without commit or rollback.
        // If Drop is correct, a ROLLBACK is sent via the channel.
    } else {
        if split_lines {
            for query in &cmd.input {
                println!(">> {}", query);
                execute_with_deadlock_retry(client, query).await?;
            }
        } else {
            let query = cmd.input.join("\n");
            println!(">> {}", query);
            // execute uses prepared statements, which will fail for CREATE FUNCTION/PROCEDURE etc, see
            // https://github.com/prisma/tiberius/issues/236, so using simple_query instead
            execute_with_deadlock_retry(client, &query).await?;
        }
    }

    Ok(ControlFlow::Continue)
}
