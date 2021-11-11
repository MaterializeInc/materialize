// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;
use std::path;

use async_trait::async_trait;
use protobuf::Message;
use tokio::fs;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CompileDescriptorsAction {
    inputs: Vec<String>,
    output: String,
}

pub fn build_compile_descriptors(
    mut cmd: BuiltinCommand,
) -> Result<CompileDescriptorsAction, String> {
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
            return Err("separators in paths are forbidden".into());
        }
    }
    Ok(CompileDescriptorsAction { inputs, output })
}

#[async_trait]
impl Action for CompileDescriptorsAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let mut protoc = mz_protoc::Protoc::new();
        protoc.include(&state.temp_path);
        for input in &self.inputs {
            protoc.input(state.temp_path.join(input));
        }
        let fds = protoc
            .parse()
            .map_err(|e| format!("compiling protobuf descriptors: {}", e))?
            .write_to_bytes()
            .map_err(|e| format!("compiling protobuf descriptors: {}", e))?;
        fs::write(state.temp_path.join(&self.output), fds)
            .await
            .map_err(|e| format!("writing protobuf descriptors: {}", e))?;
        Ok(())
    }
}
