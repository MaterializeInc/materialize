// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path;

use async_trait::async_trait;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct AppendAction {
    path: String,
    contents: String,
}

fn build_path(cmd: &mut BuiltinCommand) -> Result<String, String> {
    let path = cmd.args.string("path")?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        Err("separators in paths are forbidden".into())
    } else {
        Ok(path)
    }
}

pub fn build_append(mut cmd: BuiltinCommand) -> Result<AppendAction, String> {
    let path = build_path(&mut cmd)?;
    let contents = cmd.input.join("\n") + "\n";
    cmd.args.done()?;
    Ok(AppendAction { path, contents })
}

#[async_trait]
impl Action for AppendAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Appending to file {}", path.display());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| e.to_string())?;
        file.write_all(self.contents.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub struct DeleteAction {
    path: String,
}

pub fn build_delete(mut cmd: BuiltinCommand) -> Result<DeleteAction, String> {
    let path = build_path(&mut cmd)?;
    cmd.args.done()?;
    Ok(DeleteAction { path })
}

#[async_trait]
impl Action for DeleteAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("Deleting file {}", path.display());
        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| e.to_string())
    }
}
