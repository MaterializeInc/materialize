// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::path;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct WriteAction {
    path: String,
    contents: String,
}

pub fn build_write(mut cmd: BuiltinCommand) -> Result<WriteAction, String> {
    let path = cmd.args.string("path")?;
    let contents = cmd.input.join("\n");
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        return Err("separators in paths are forbidden".into());
    }
    Ok(WriteAction { path, contents })
}

impl Action for WriteAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        fs::write(state.temp_dir.path().join(&self.path), &self.contents)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
