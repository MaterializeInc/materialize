// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn stop_capture_jobs(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;

    let client = state
        .sql_server_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("connection {} not found", name.quoted()))?;

    static STOP_CDC_JOBS: &str = "EXEC sys.sp_cdc_stop_job @job_type = N'capture';";
    let max_duration = std::cmp::max(state.default_timeout, Duration::from_secs(10));
    let timer = Instant::now();

    loop {
        let Err(error) = client.execute(STOP_CDC_JOBS, &[]).await else {
            break;
        };
        if timer.elapsed() < max_duration {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            return Err(anyhow!("ERROR: sp_cdc_stop_job failed {error}"));
        }
    }

    Ok(ControlFlow::Continue)
}

pub async fn run_cdc_scan(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    cmd.args.done()?;

    let client = state
        .sql_server_clients
        .get_mut(&name)
        .ok_or_else(|| anyhow!("connection {} not found", name.quoted()))?;

    let max_duration = std::cmp::max(state.default_timeout, Duration::from_secs(10));
    let timer = Instant::now();
    static CDC_SCAN: &str = "EXEC sys.sp_cdc_scan @continuous = 0, @maxscans = 1;";
    loop {
        let Err(error) = client.execute(CDC_SCAN, &[]).await else {
            break;
        };
        if timer.elapsed() < max_duration {
            tokio::time::sleep(Duration::from_millis(100)).await;
        } else {
            return Err(anyhow!("ERROR: sp_cdc_scan failed {error}"));
        }
    }
    // now we wait until it's done

    Ok(ControlFlow::Continue)
}
