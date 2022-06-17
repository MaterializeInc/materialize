// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration test driver for Materialize.

#![warn(missing_docs)]

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use anyhow::{anyhow, Context};
use itertools::Itertools;

use mz_ore::display::DisplayExt;

use self::action::ControlFlow;
use self::error::{ErrorLocation, PosError};
use self::parser::LineReader;

mod action;
mod error;
mod format;
mod parser;
mod util;

pub use self::action::Config;
pub use self::error::Error;

/// Runs a testdrive script stored in a file.
pub async fn run_file(config: &Config, filename: &Path) -> Result<(), Error> {
    let mut file =
        File::open(filename).with_context(|| format!("opening {}", filename.display()))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("reading {}", filename.display()))?;
    run_string(config, filename, &contents).await
}

/// Runs a testdrive script from the standard input.
pub async fn run_stdin(config: &Config) -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .context("reading <stdin>")?;
    run_string(config, Path::new("<stdin>"), &contents).await
}

/// Runs a testdrive script stored in a string.
///
/// The script in `contents` is used verbatim. The provided `filename` is used
/// only as output in error messages and such. No attempt is made to read
/// `filename`.
pub async fn run_string(config: &Config, filename: &Path, contents: &str) -> Result<(), Error> {
    println!("--- {}", filename.display());

    let mut line_reader = LineReader::new(contents);
    run_line_reader(config, &mut line_reader)
        .await
        .map_err(|e| {
            let location = e.pos.map(|pos| {
                let (line, col) = line_reader.line_col(pos);
                ErrorLocation::new(filename, contents, line, col)
            });
            Error::new(e.source, location)
        })
}

async fn run_line_reader(
    config: &Config,
    line_reader: &mut LineReader<'_>,
) -> Result<(), PosError> {
    // TODO(benesch): consider sharing state between files, to avoid
    // reconnections for every file. For now it's nice to not open any
    // connections until after parsing.
    let cmds = parser::parse(line_reader)?;
    let (mut state, state_cleanup) = action::create_state(config).await?;
    let actions = action::build(cmds, &state).await?;

    if config.reset {
        state.reset_materialize().await?;

        for a in actions.iter().rev() {
            let undo = a.action.undo(&mut state);
            undo.await.map_err(|e| PosError::new(e, a.pos))?
        }
    }

    for a in &actions {
        let redo = a.action.redo(&mut state);
        match redo.await.map_err(|e| PosError::new(e, a.pos))? {
            ControlFlow::Continue => (),
            ControlFlow::Break => break,
        }
    }

    if config.reset {
        let mut errors = Vec::new();

        if let Err(e) = state.reset_s3().await {
            errors.push(e);
        }

        if let Err(e) = state.reset_sqs().await {
            errors.push(e);
        }

        if let Err(e) = state.reset_kinesis().await {
            errors.push(e);
        }

        drop(state);
        if let Err(e) = state_cleanup.await {
            errors.push(e);
        }

        if !errors.is_empty() {
            return Err(anyhow!(
                "cleanup failed: {} errors: {}",
                errors.len(),
                errors.into_iter().map(|e| e.to_string_alt()).join("\n"),
            )
            .into());
        }
    }
    Ok(())
}
