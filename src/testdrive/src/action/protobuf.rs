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
use tokio::process::Command;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_compile_descriptors(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
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
    let protoc = match env::var_os("PROTOC") {
        None => protobuf_src::protoc(),
        Some(protoc) => PathBuf::from(protoc),
    };
    let status = Command::new(protoc)
        .arg("--include_imports")
        .arg("-I")
        .arg(&state.temp_path)
        .arg("--descriptor_set_out")
        .arg(state.temp_path.join(&output))
        .args(&inputs)
        .status()
        .await
        .context("invoking protoc failed")?;
    if !status.success() {
        bail!("protoc exited unsuccessfully");
    }
    Ok(ControlFlow::Continue)
}
