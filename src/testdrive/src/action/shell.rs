// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

use tokio::process::Command;

pub struct ExecuteAction {
    command: String,
}

pub fn build_execute(cmd: BuiltinCommand) -> Result<ExecuteAction, String> {
    Ok(ExecuteAction {
        command: cmd.input.join("\n"),
    })
}

#[async_trait]
impl Action for ExecuteAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, _: &mut State) -> Result<(), String> {
        println!("$ shell-execute\n{}", self.command);

        let status = Command::new("bash")
            .arg("-c")
            .arg(&self.command)
            .spawn()
            .map_err(|e| e.to_string())?
            .wait()
            .await
            .map_err(|e| e.to_string())?;

        // Make sure the output of the shell command is separated from the output of the
        // subsequent testdrive command
        println!("");

        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "shell command returned non-zero exit code: {}",
                status.code().unwrap()
            ))
        }
    }
}
