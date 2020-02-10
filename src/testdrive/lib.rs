// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration test driver for Materialize.

use std::fs::File;
use std::io::{self, Read};

use self::error::{Error, InputError, ResultExt};
use self::parser::LineReader;

mod action;
mod parser;
mod protobuf;

pub mod error;

pub use self::action::Config;

/// Runs a testdrive script stored in a file.
pub fn run_file(config: &Config, filename: &str) -> Result<(), Error> {
    let mut file = File::open(&filename).err_ctx(format!("opening {}", filename))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .err_ctx(format!("reading {}", filename))?;
    run_string(config, filename, &contents)
}

/// Runs a testdrive script from the standard input.
pub fn run_stdin(config: &Config) -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .err_ctx("reading <stdin>".into())?;
    run_string(config, "<stdin>", &contents)
}

/// Runs a testdrive script stored in a string.
///
/// The script in `contents` is used verbatim. The provided `filename` is used
/// only as output in error messages and such. No attempt is made to read
/// `filename`.
pub fn run_string(config: &Config, filename: &str, contents: &str) -> Result<(), Error> {
    println!("==> {}", filename);
    let mut line_reader = LineReader::new(contents);
    // TODO(benesch): when `try` blocks land, use one here.
    run_line_reader(config, &mut line_reader)
        .map_err(|e| e.with_input_details(&filename, &contents, &line_reader))
}

fn run_line_reader(config: &Config, line_reader: &mut LineReader) -> Result<(), Error> {
    let cmds = parser::parse(line_reader)?;
    // TODO(benesch): consider sharing state between files, to avoid
    // reconnections for every file. For now it's nice to not open any
    // connections until after parsing.
    let mut state = action::create_state(config)?;
    state.reset_materialized()?;
    let actions = action::build(cmds, &state)?;
    for a in actions.iter().rev() {
        a.action
            .undo(&mut state)
            .map_err(|e| InputError { msg: e, pos: a.pos })?;
    }
    for a in &actions {
        a.action
            .redo(&mut state)
            .map_err(|e| InputError { msg: e, pos: a.pos })?;
    }
    Ok(())
}
