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
use std::io::{self, Read, Write};
use std::path::Path;

use action::Run;
use anyhow::{Context, anyhow};
use mz_ore::error::ErrorExt;
use tempfile::NamedTempFile;
use tracing::debug;

use crate::action::ControlFlow;
use crate::error::{ErrorLocation, PosError};
use crate::parser::{BuiltinCommand, Command, LineReader};

mod action;
mod error;
mod format;
mod parser;
mod util;

pub use crate::action::consistency::Level as ConsistencyCheckLevel;
pub use crate::action::{CatalogConfig, Config};
pub use crate::error::Error;

/// Runs a testdrive script stored in a file.
pub async fn run_file(config: &Config, filename: &Path) -> Result<(), Error> {
    let mut file =
        File::open(filename).with_context(|| format!("opening {}", filename.display()))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("reading {}", filename.display()))?;
    run_string(config, Some(filename), &contents).await
}

/// Runs a testdrive script from the standard input.
pub async fn run_stdin(config: &Config) -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .context("reading <stdin>")?;
    run_string(config, None, &contents).await
}

/// Runs a testdrive script stored in a string.
///
/// The script in `contents` is used verbatim. The provided `filename` is used
/// only as output in error messages and such. No attempt is made to read
/// `filename`.
pub async fn run_string(
    config: &Config,
    filename: Option<&Path>,
    contents: &str,
) -> Result<(), Error> {
    if let Some(f) = filename {
        println!("--- {}", f.display());
    }

    let mut line_reader = LineReader::new(contents);
    run_line_reader(config, &mut line_reader, contents, filename)
        .await
        .map_err(|e| {
            let location = e.pos.map(|pos| {
                let (line, col) = line_reader.line_col(pos);
                ErrorLocation::new(filename, contents, line, col)
            });
            Error::new(e.source, location)
        })
}

pub(crate) async fn run_line_reader(
    config: &Config,
    line_reader: &mut LineReader<'_>,
    contents: &str,
    filename: Option<&Path>,
) -> Result<(), PosError> {
    // TODO(benesch): consider sharing state between files, to avoid
    // reconnections for every file. For now it's nice to not open any
    // connections until after parsing.
    let cmds = parser::parse(line_reader)?;

    if cmds.is_empty() {
        return Err(PosError::from(anyhow!("No input provided!")));
    } else {
        debug!("Received {} commands to run", cmds.len());
    }

    let has_kafka_cmd = cmds.iter().any(|cmd| {
        matches!(
            &cmd.command,
            Command::Builtin(BuiltinCommand { name, .. }, _) if name.starts_with("kafka-"),
        )
    });

    let (mut state, state_cleanup) = action::create_state(config).await?;

    if config.reset {
        // Delete any existing Materialize and Kafka state *before* the test
        // script starts. We don't clean up Materialize or Kafka state at the
        // end of the script because it's useful to leave the state around,
        // e.g., for debugging, or when using a testdrive script to set up
        // Materialize for further tinkering.

        state.reset_materialize().await?;

        // Only try to clean up Kafka state if the test script uses a Kafka
        // action. Tests that don't use Kafka likely don't have a Kafka
        // broker available.
        if has_kafka_cmd {
            state.reset_kafka().await?;
        }
    }

    let mut errors = Vec::new();

    let mut skipping = false;

    for cmd in cmds {
        if skipping {
            if let Command::Builtin(builtin, _) = cmd.command {
                if builtin.name == "skip-end" {
                    println!("skip-end reached");
                    skipping = false;
                } else if builtin.name == "skip-if" {
                    errors.push(PosError {
                        source: anyhow!("nested skip-if not allowed"),
                        pos: Some(cmd.pos),
                    });
                    break;
                }
            }
            continue;
        }

        match cmd.run(&mut state).await {
            Ok(ControlFlow::Continue) => (),
            Ok(ControlFlow::SkipBegin) => {
                skipping = true;
                ()
            }
            // ignore, already handled above
            Ok(ControlFlow::SkipEnd) => (),
            Err(e) => {
                errors.push(e);
                break;
            }
        }
    }
    let mut consistency_checks_succeeded = true;
    if config.consistency_checks == action::consistency::Level::File {
        if let Err(e) = action::consistency::run_consistency_checks(&state).await {
            consistency_checks_succeeded = false;
            errors.push(e.into());
        }
    }
    state.clear_skip_consistency_checks();

    if config.rewrite_results && consistency_checks_succeeded {
        let mut f = NamedTempFile::new_in(filename.unwrap().parent().unwrap()).unwrap();
        let mut pos = 0;
        for rewrite in &state.rewrites {
            write!(f, "{}", &contents[pos..rewrite.start]).expect("rewriting results");
            write!(f, "{}", rewrite.content).expect("rewriting results");
            pos = rewrite.end;
        }
        write!(f, "{}", &contents[pos..]).expect("rewriting results");
        f.persist(filename.unwrap()).expect("rewriting results");
    }

    if config.reset {
        drop(state);
        if let Err(e) = state_cleanup.await {
            errors.push(anyhow!("cleanup failed: error: {}", e.to_string_with_causes()).into());
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        // Only surface the first error encountered for sake of simplicity
        Err(errors.remove(0))
    }
}
