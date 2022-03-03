// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::iter;
use std::path::{self, PathBuf};

use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio::process::Command;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct CompileDescriptorsAction {
    inputs: Vec<String>,
    output: String,
}

pub fn build_compile_descriptors(
    mut cmd: BuiltinCommand,
) -> Result<CompileDescriptorsAction, anyhow::Error> {
    let inputs: Vec<String> = cmd
        .args
        .string("inputs")?
        .split(',')
        .map(|s| s.into())
        .collect();
    let output = cmd.args.string("output")?;
    for path in inputs.iter().chain(iter::once(&output)) {
        if path.contains(path::MAIN_SEPARATOR) {
            // The goal isn't security, but preventing mistakes.
            bail!("separators in paths are forbidden");
        }
    }
    Ok(CompileDescriptorsAction { inputs, output })
}

#[async_trait]
impl Action for CompileDescriptorsAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let protoc = match env::var_os("PROTOC") {
            None => protobuf_src::protoc(),
            Some(protoc) => PathBuf::from(protoc),
        };
        let status = Command::new(protoc)
            .arg("--include_imports")
            .arg("-I")
            .arg(&state.temp_path)
            .arg("--descriptor_set_out")
            .arg(state.temp_path.join(&self.output))
            .args(&self.inputs)
            .status()
            .await
            .context("invoking protoc failed")?;
        if !status.success() {
            bail!("protoc exited unsuccessfully");
        }
        Ok(ControlFlow::Continue)
    }
}
