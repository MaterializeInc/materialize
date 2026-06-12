// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sql command - launch an interactive psql session using the active profile.

use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use crate::cli::CliError;
use crate::client::{build_options_string, default_sslmode};
use crate::config::Settings;

const APPLICATION_NAME: &str = "mz-deploy-sql";

/// Launch an interactive `psql` session connected via the active profile.
///
/// Translates the profile into `PG*` environment variables, then replaces the
/// current process with `psql`. Trailing arguments are forwarded to `psql`
/// unchanged, so callers can pass flags like `-c "SELECT 1"` or `-f script.sql`.
///
/// Unlike the rest of the CLI, this command does **not** pin the session to
/// `_mz_deploy_server`: interactive shells should use whatever cluster the
/// profile (or server default) selects.
pub fn run(settings: &Settings, psql_args: Vec<String>) -> Result<(), CliError> {
    let profile = settings.connection();
    let host = profile.require_host()?;

    let mut cmd = Command::new("psql");
    cmd.env("PGHOST", host);
    cmd.env("PGPORT", profile.port.to_string());
    cmd.env("PGUSER", &profile.username);
    cmd.env("PGDATABASE", "materialize");
    if let Some(password) = &profile.password {
        cmd.env("PGPASSWORD", password);
    }

    let mode = profile.sslmode.unwrap_or_else(|| default_sslmode(host));
    cmd.env("PGSSLMODE", mode.libpq_name());
    if let Some(cert) = &profile.sslrootcert {
        cmd.env("PGSSLROOTCERT", cert);
    }

    if let Some(options) = build_options_string(&profile.options) {
        cmd.env("PGOPTIONS", options);
    }

    cmd.env("PGAPPNAME", APPLICATION_NAME);
    cmd.args(psql_args);

    // `exec` replaces the current process on success and only returns on
    // failure. Map ENOENT to an install hint, since a missing `psql` binary
    // is the most common error here.
    let err = cmd.exec();
    let msg = if err.kind() == io::ErrorKind::NotFound {
        "failed to launch psql: binary not found on PATH. \
         Install it with `brew install libpq` (macOS) or \
         `apt install postgresql-client` (Debian/Ubuntu)."
            .to_string()
    } else {
        format!("failed to launch psql: {err}")
    };
    Err(CliError::Message(msg))
}
