// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Clean command — delete the project's `target/` build directory.

use crate::cli::CliError;
use crate::config::Settings;
use crate::log;
use crate::types::BUILD_DIR;
use std::fmt;
use std::path::PathBuf;

#[derive(serde::Serialize)]
struct CleanResult {
    path: PathBuf,
    removed: bool,
}

impl fmt::Display for CleanResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.removed {
            write!(f, "  \u{2713} Removed {}", self.path.display())
        } else {
            write!(
                f,
                "  \u{2713} Nothing to clean ({} not present)",
                self.path.display()
            )
        }
    }
}

/// Delete the project's `target/` directory.
///
/// Idempotent: succeeds (with a "nothing to clean" message) when `target/`
/// does not exist.
pub fn run(settings: &Settings) -> Result<(), CliError> {
    let target = settings.directory.join(BUILD_DIR);
    let removed = match std::fs::remove_dir_all(&target) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => return Err(CliError::Io(e)),
    };
    log::output(&CleanResult {
        path: target,
        removed,
    });
    Ok(())
}
