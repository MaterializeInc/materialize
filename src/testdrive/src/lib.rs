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
mod format;
mod parser;

pub mod error;
pub mod util;

pub use self::action::Config;

/// Runs a testdrive script stored in a file.
pub async fn run_file(config: &Config, filename: &str) -> Result<(), Error> {
    let mut file = File::open(&filename).err_ctx(format!("opening {}", filename))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .err_ctx(format!("reading {}", filename))?;
    run_string(config, filename, &contents).await
}

/// Runs a testdrive script from the standard input.
pub async fn run_stdin(config: &Config) -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .err_ctx("reading <stdin>".into())?;
    run_string(config, "<stdin>", &contents).await
}

/// Runs a testdrive script stored in a string.
///
/// The script in `contents` is used verbatim. The provided `filename` is used
/// only as output in error messages and such. No attempt is made to read
/// `filename`.
pub async fn run_string(config: &Config, filename: &str, contents: &str) -> Result<(), Error> {
    println!("==> {}", filename);
    let mut line_reader = LineReader::new(contents);
    run_line_reader(config, &mut line_reader)
        .await
        .map_err(|e| e.with_input_details(&filename, &contents, &line_reader))
}

async fn run_line_reader(config: &Config, line_reader: &mut LineReader<'_>) -> Result<(), Error> {
    // TODO(benesch): consider sharing state between files, to avoid
    // reconnections for every file. For now it's nice to not open any
    // connections until after parsing.
    let cmds = parser::parse(line_reader)?;
    let mut cmds_exec = cmds.clone();
    // Extract number of executions
    let mut execution_count = 1;
    if let Some(command) = cmds_exec.iter_mut().find(|el| {
        if let parser::Command::Builtin(c) = &el.command {
            if c.name == "set-execution-count" {
                return true;
            }
        }
        false
    }) {
        if let parser::Command::Builtin(c) = &mut command.command {
            let count = c.args.string("count").unwrap_or_default();
            execution_count = count.parse::<u32>().unwrap_or(1);
        }
    };
    println!("Running test {} time(s) ... ", execution_count);
    for _ in 1..execution_count {
        println!("Run {} ...", execution_count);
        cmds_exec = cmds.clone();
        let (mut state, state_cleanup) = action::create_state(config).await?;
        state.reset_materialized().await?;
        // The `tokio::spawn` allows using `block_in_place` to run sync code within
        // the spawned task. The spawn will one day not be necessary.
        // See: https://github.com/tokio-rs/tokio/issues/1838.
        tokio::spawn(async move {
            let actions = action::build(cmds_exec, &state)?;
            for a in actions.iter().rev() {
                let undo = a.action.undo(&mut state);
                undo.await.map_err(|e| InputError { msg: e, pos: a.pos })?;
            }
            for a in &actions {
                let redo = a.action.redo(&mut state);
                redo.await.map_err(|e| InputError { msg: e, pos: a.pos })?;
            }
            drop(state);
            state_cleanup.await
        })
        .await
        .expect("action task unexpectedly canceled")?
    }
    Ok(())
}
