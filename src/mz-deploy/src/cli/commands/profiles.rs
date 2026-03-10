//! Profiles command - list available connection profiles.

use crate::cli::CliError;
use crate::config::{ProfilesConfig, ProjectSettings};
use crate::humanln;
use owo_colors::OwoColorize;
use std::path::Path;

/// List available connection profiles and indicate the active one.
///
/// # Arguments
/// * `directory` - Project directory to search for configuration files.
/// * `cli_profile` - Optional profile name from the `--profile` CLI flag.
/// * `profiles_dir` - Optional directory containing `profiles.toml`.
pub fn run(
    directory: &Path,
    cli_profile: Option<&str>,
    profiles_dir: Option<&Path>,
) -> Result<(), CliError> {
    let profiles_config = ProfilesConfig::load(profiles_dir).map_err(CliError::Config)?;

    let default_profile = ProjectSettings::load(directory).ok().map(|s| s.profile);

    let active = cli_profile.map(|s| s.to_string()).or(default_profile);

    let names = profiles_config.profile_names();

    if names.is_empty() {
        humanln!(
            "No profiles found in {}",
            profiles_config.source_path().display()
        );
        return Ok(());
    }

    for name in &names {
        if active.as_deref() == Some(*name) {
            humanln!("  {}  {}", name.green(), "(active)".dimmed());
        } else {
            humanln!("  {name}");
        }
    }

    Ok(())
}
