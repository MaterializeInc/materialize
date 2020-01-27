// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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
