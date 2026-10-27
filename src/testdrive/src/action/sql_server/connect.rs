// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use mz_ore::retry::{Retry, RetryResult};
use mz_sql_server_util::{Client, Config};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

/// Whether a connection error is transient and worth retrying.
///
/// Azure SQL Database returns error 40613 ("Database ... is not currently
/// available") while a serverless database is resuming or being scaled, which
/// can take tens of seconds. The connection succeeds once the database is
/// online, so we retry rather than fail the test.
fn is_retryable_connect_error(err: &anyhow::Error) -> bool {
    // Use alternate Display format `{:#}` to get the full anyhow error chain,
    // not just the outermost context message.
    let msg = format!("{:#}", err);
    msg.contains("40613") || msg.contains("is not currently available")
}

pub async fn run_connect(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    // Retry policy for transient connection errors, all in seconds. Defaults
    // are generous because an auto-paused Azure SQL Database can take tens of
    // seconds to resume.
    let initial_backoff = cmd
        .args
        .opt_parse::<f64>("retry-initial-backoff")?
        .map(Duration::from_secs_f64)
        .unwrap_or(Duration::from_millis(500));
    let clamp_backoff = cmd
        .args
        .opt_parse::<f64>("retry-clamp-backoff")?
        .map(Duration::from_secs_f64)
        .unwrap_or(Duration::from_secs(5));
    let max_duration = cmd
        .args
        .opt_parse::<f64>("retry-max-duration")?
        .map(Duration::from_secs_f64)
        .unwrap_or(Duration::from_secs(90));
    cmd.args.done()?;

    let ado_string = cmd.input.join("\n");

    let config = Config::from_ado_string(&ado_string).context("parsing ADO string")?;

    let client = Retry::default()
        .initial_backoff(initial_backoff)
        .clamp_backoff(clamp_backoff)
        .max_duration(max_duration)
        .retry_async(|_| async {
            match Client::connect(config.clone())
                .await
                .context("connecting to SQL server")
            {
                Ok(client) => RetryResult::Ok(client),
                Err(err) if is_retryable_connect_error(&err) => {
                    println!(">> transient connect error, retrying: {:#}", err);
                    RetryResult::RetryableErr(err)
                }
                Err(err) => RetryResult::FatalErr(err),
            }
        })
        .await?;
    state.sql_server_clients.insert(name.clone(), client);

    Ok(ControlFlow::Continue)
}
