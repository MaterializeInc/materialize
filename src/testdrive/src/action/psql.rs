// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use tokio::process::Command;

use ore::option::OptionExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use crate::util::text;

pub struct ExecuteAction {
    command: String,
    expected_output: String,
}

pub fn build_execute(mut cmd: BuiltinCommand) -> Result<ExecuteAction, String> {
    let command = cmd.args.string("command")?;
    Ok(ExecuteAction {
        command,
        expected_output: cmd.input.join("\n"),
    })
}

#[async_trait]
impl Action for ExecuteAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let output = Command::new("psql")
            .args(&[
                "--pset",
                "footer=off",
                "--command",
                &self.command,
                &format!(
                    "postgres://{}@{}",
                    state.materialized_user, state.materialized_addr
                ),
            ])
            .output()
            .await
            .map_err(|e| format!("execution of `psql` failed: {}", e))?;
        if !output.status.success() {
            return Err(format!(
                "psql reported failure with exit code {}: {}",
                output.status.code().display_or("unknown"),
                String::from_utf8_lossy(&output.stderr),
            ));
        }
        let stdout = text::trim_trailing_space(&String::from_utf8_lossy(&output.stdout));
        if self.expected_output != stdout {
            text::print_diff(&self.expected_output, &*stdout);
            return Err("psql returned unexpected output (diff above)".into());
        }
        Ok(())
    }
}
