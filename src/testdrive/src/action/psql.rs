// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use tokio::process::Command;

use mz_ore::option::OptionExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::text;

pub async fn run_execute(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let command = cmd.args.string("command")?;
    cmd.args.done()?;

    let expected_output = cmd.input.join("\n");
    let output = Command::new("psql")
        .args(&[
            "--pset",
            "footer=off",
            "--command",
            &command,
            &format!(
                "postgres://{}@{}",
                state.materialize_user, state.materialize_sql_addr
            ),
        ])
        .output()
        .await
        .context("execution of `psql` failed")?;
    if !output.status.success() {
        bail!(
            "psql reported failure with exit code {}: {}",
            output.status.code().display_or("unknown"),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let stdout = text::trim_trailing_space(&String::from_utf8_lossy(&output.stdout));
    if expected_output != stdout {
        text::print_diff(&expected_output, &*stdout);
        bail!("psql returned unexpected output (diff above)");
    }
    Ok(ControlFlow::Continue)
}
