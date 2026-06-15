// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `mz-deploy profile {list,set,current}` — manage the project's default profile.
//!
//! Modeled on `kubectl config` (contexts). The default profile is recorded
//! per-project and per-developer, so team members can each pick their own
//! default without touching shared configuration. Resolution order for any
//! command: `--profile` flag, then `MZ_DEPLOY_PROFILE`, then the recorded
//! project default, then error.
//!
//! Subcommands:
//!
//! - [`list`] — every profile defined in `profiles.toml`, with the currently
//!   resolved profile marked `(active)`.
//! - [`set`] — records `<name>` as the project default after validating that
//!   the profile exists in `profiles.toml`.
//! - [`current`] — prints the resolved profile and where it came from (flag,
//!   env var, or project default), or reports that no profile has been
//!   selected.

use crate::cli::CliError;
use crate::config::{ProfilesConfig, read_mzprofile, write_mzprofile};
use crate::{info, log};
use owo_colors::{OwoColorize, Stream, Style};
use serde::Serialize;
use std::fmt;
use std::path::Path;

/// Renderable result for `profile list`.
#[derive(Serialize)]
struct ProfileListing {
    profiles: Vec<ProfileEntry>,

    source: String,
}

#[derive(Serialize)]
struct ProfileEntry {
    name: String,
    active: bool,
}

impl fmt::Display for ProfileListing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.profiles.is_empty() {
            return write!(f, "No profiles found in {}", self.source);
        }
        let mut first = true;
        for entry in &self.profiles {
            if !first {
                writeln!(f)?;
            }
            first = false;
            if entry.active {
                write!(
                    f,
                    "  {}  {}",
                    entry.name.if_supports_color(Stream::Stderr, |t| t.green()),
                    "(active)".if_supports_color(Stream::Stderr, |t| t.dimmed())
                )?;
            } else {
                write!(f, "  {}", entry.name)?;
            }
        }
        Ok(())
    }
}

/// List every profile defined in `profiles.toml` and mark the active one.
pub fn list(
    directory: &Path,
    cli_profile: Option<&str>,
    profiles_dir: Option<&Path>,
) -> Result<(), CliError> {
    let profiles_config = ProfilesConfig::load(profiles_dir)?;
    let active = resolve_active(directory, cli_profile)?;
    let names = profiles_config.profile_names();

    let profiles = names
        .iter()
        .map(|name| ProfileEntry {
            name: (*name).to_string(),
            active: active.as_deref() == Some(*name),
        })
        .collect();

    log::output(&ProfileListing {
        profiles,
        source: profiles_config.source_path().display().to_string(),
    });
    Ok(())
}

/// Record `name` as the project default.
///
/// Validates that the profile exists in `profiles.toml` so typos fail at
/// `set` time rather than at the next command invocation.
pub fn set(directory: &Path, profiles_dir: Option<&Path>, name: &str) -> Result<(), CliError> {
    let profiles_config = ProfilesConfig::load(profiles_dir)?;
    // `get_profile` returns ConfigError::ProfileNotFound if missing.
    let _ = profiles_config.get_profile(name)?;

    write_mzprofile(directory, name)?;

    let check_style = Style::new().green().bold();
    info!(
        "  {} default profile set to {}",
        "✓".if_supports_color(Stream::Stderr, |t| check_style.style(t)),
        name.if_supports_color(Stream::Stderr, |t| t.green()),
    );
    Ok(())
}

/// Print the resolved profile and the source it came from.
pub fn current(directory: &Path, cli_profile: Option<&str>) -> Result<(), CliError> {
    if let Some(name) = cli_profile {
        // Clap surfaces `--profile` and `MZ_DEPLOY_PROFILE` through the same
        // `cli_profile` handle; we can't distinguish which one was set without
        // querying the env directly.
        let source = if std::env::var_os("MZ_DEPLOY_PROFILE").is_some() {
            "MZ_DEPLOY_PROFILE env var"
        } else {
            "--profile flag"
        };
        info!(
            "  {} ({})",
            name.if_supports_color(Stream::Stderr, |t| t.green()),
            source.if_supports_color(Stream::Stderr, |t| t.dimmed())
        );
        return Ok(());
    }

    match read_mzprofile(directory)? {
        Some(name) => {
            info!(
                "  {} ({})",
                name.if_supports_color(Stream::Stderr, |t| t.green()),
                "project default".if_supports_color(Stream::Stderr, |t| t.dimmed()),
            );
        }
        None => {
            info!(
                "  {} no profile selected — run {} to set one",
                "⚠".if_supports_color(Stream::Stderr, |t| t.yellow()),
                "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan()),
            );
        }
    }
    Ok(())
}

fn resolve_active(directory: &Path, cli_profile: Option<&str>) -> Result<Option<String>, CliError> {
    if let Some(p) = cli_profile {
        return Ok(Some(p.to_string()));
    }
    Ok(read_mzprofile(directory)?)
}
