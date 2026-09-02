// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scaffold a new mz-deploy project directory.
//!
//! Creates the standard directory layout (`models/`, `clusters/`, `roles/`),
//! writes starter `project.toml` and `profiles.toml` files, and optionally
//! initializes a git repository.

use crate::cli::CliError;
use crate::cli::progress;
use crate::config::{ProfilesConfig, write_mzprofile};
use crate::{info, info_nonl, log};
use owo_colors::{OwoColorize, Stream, Style};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::process::Command;

const GITIGNORE: &str = include_str!("../scaffold/gitignore");
const PROJECT_TOML: &str = include_str!("../scaffold/project.toml");
const README_MD: &str = include_str!("../scaffold/README.md");
const VSCODE_EXTENSIONS_JSON: &str = include_str!("../scaffold/vscode-extensions.json");

/// Shared options for project scaffolding.
pub struct ScaffoldOpts {
    pub init_git: bool,
}

/// `mz-deploy new <name>` — create a new directory and scaffold into it.
pub fn run(name: &str, opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(name);

    if project_dir.exists() {
        return Err(CliError::Message(format!(
            "destination `{}` already exists",
            name
        )));
    }

    fs::create_dir_all(project_dir)
        .map_err(|e| CliError::Message(format!("failed to create directory: {}", e)))?;

    scaffold(project_dir, name, &opts)?;
    progress::success(&format!("Created project `{}`", name));
    prompt_default_profile(project_dir)?;
    print_skill_hint();
    Ok(())
}

/// `mz-deploy init` — scaffold the current directory as an mz-deploy project.
pub fn init(opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(".");
    let name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "my-project".to_string());

    scaffold(project_dir, &name, &opts)?;
    progress::success(&format!("Initialized project `{}`", name));
    prompt_default_profile(project_dir)?;
    print_skill_hint();
    Ok(())
}

/// Interactive nudge to pick a default profile right after scaffolding.
///
/// Skipped silently in non-interactive contexts (JSON output, `--quiet`, or a
/// non-TTY stdin) so this never blocks CI. When skipped, the user can still
/// run `mz-deploy profile set <name>` later — we don't want to re-explain
/// that on every script invocation.
fn prompt_default_profile(project_dir: &Path) -> Result<(), CliError> {
    if log::json_output_enabled() || log::quiet_enabled() || !io::stdin().is_terminal() {
        return Ok(());
    }

    info!("");

    let profiles_config = match ProfilesConfig::load(None) {
        Ok(c) => c,
        Err(_) => {
            info!("No profiles configured yet.");
            info!("  Add one to ~/.mz/profiles.toml, then run:");
            info!(
                "    {}",
                "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan())
            );
            print_profile_help_hint();
            return Ok(());
        }
    };

    let names = profiles_config.profile_names();
    if names.is_empty() {
        info!("No profiles configured yet.");
        info!(
            "  Add one to {}, then run:",
            profiles_config.source_path().display()
        );
        info!(
            "    {}",
            "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan())
        );
        print_profile_help_hint();
        return Ok(());
    }

    info!("Pick a default profile for this project:");
    for (i, name) in names.iter().enumerate() {
        info!("  {}. {}", i + 1, name);
    }
    info_nonl!("Enter a number, or press Enter to skip: ");
    io::stderr().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        info!(
            "Skipped. Set a default later with {}",
            "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan())
        );
        print_profile_help_hint();
        return Ok(());
    }

    let choice: usize = match trimmed.parse() {
        Ok(n) if (1..=names.len()).contains(&n) => n,
        _ => {
            info!(
                "Invalid choice. Skipped — set a default later with {}",
                "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan())
            );
            print_profile_help_hint();
            return Ok(());
        }
    };

    let name = names[choice - 1];
    write_mzprofile(project_dir, name)?;

    let check_style = Style::new().green().bold();
    info!(
        "  {} default profile set to {}. Update with {}",
        "✓".if_supports_color(Stream::Stderr, |t| check_style.style(t)),
        name.if_supports_color(Stream::Stderr, |t| t.green()),
        "mz-deploy profile set <name>".if_supports_color(Stream::Stderr, |t| t.cyan()),
    );
    print_profile_help_hint();
    Ok(())
}

/// One-line nudge to the help page; appended to every profile-prompt outcome.
fn print_profile_help_hint() {
    info!(
        "  Learn more: {}",
        "mz-deploy help profile".if_supports_color(Stream::Stderr, |t| t.cyan())
    );
}

/// Nudge users toward installing the optional Materialize agent skill.
/// Mirrors the `## Agent skills` section of the scaffolded `README.md`.
fn print_skill_hint() {
    info!("");
    info!("Tip: install the Materialize agent skill for AI coding agents:");
    info!("  npx -y skills add MaterializeInc/agent-skills -a universal -a claude-code --project");
}

/// Common scaffolding logic shared by `new` and `init`.
///
/// Idempotent: files that already exist are left untouched, and the git commit
/// is skipped when there is nothing staged. The commit sets an explicit author
/// and committer so it does not depend on the user's git identity.
fn scaffold(project_dir: &Path, name: &str, opts: &ScaffoldOpts) -> Result<(), CliError> {
    create_dir(project_dir, "models/materialize/public")?;
    create_dir(project_dir, "clusters")?;
    create_dir(project_dir, "roles")?;
    create_dir(project_dir, "network-policies")?;
    create_dir(project_dir, ".vscode")?;
    add_file(project_dir, "models/materialize/public/.gitkeep", "")?;
    add_file(project_dir, "clusters/.gitkeep", "")?;
    add_file(project_dir, "roles/.gitkeep", "")?;
    add_file(project_dir, "network-policies/.gitkeep", "")?;
    add_file(project_dir, ".gitignore", GITIGNORE)?;
    add_file(project_dir, "project.toml", PROJECT_TOML)?;
    add_file(
        project_dir,
        ".vscode/extensions.json",
        VSCODE_EXTENSIONS_JSON,
    )?;
    add_file(
        project_dir,
        "README.md",
        &README_MD.replace("{{name}}", name),
    )?;

    if opts.init_git {
        let dir_arg = project_dir.as_os_str();
        let status = Command::new("git")
            .arg("init")
            .arg(dir_arg)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git init: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git init failed".to_string()));
        }

        let status = Command::new("git")
            .args(["add", "."])
            .current_dir(project_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git add: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git add failed".to_string()));
        }

        let nothing_staged = Command::new("git")
            .args(["diff", "--cached", "--quiet"])
            .current_dir(project_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git diff: {}", e)))?
            .success();

        if !nothing_staged {
            let status = Command::new("git")
                .args([
                    "commit",
                    "--author",
                    "Materialize Inc <noreply@materialize.com>",
                    "-m",
                    "Initial commit",
                ])
                .env("GIT_COMMITTER_NAME", "Materialize Inc")
                .env("GIT_COMMITTER_EMAIL", "noreply@materialize.com")
                .current_dir(project_dir)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map_err(|e| CliError::Message(format!("failed to run git commit: {}", e)))?;

            if !status.success() {
                return Err(CliError::Message("git commit failed".to_string()));
            }
        }
    }

    Ok(())
}

/// Writes a scaffold file, leaving any file that already exists untouched.
fn add_file(project_dir: &Path, file: &str, content: &str) -> Result<(), CliError> {
    let path = project_dir.join(file);
    if path.exists() {
        return Ok(());
    }
    fs::write(&path, content)
        .map_err(|e| CliError::Message(format!("failed to write {}: {}", file, e)))
}

fn create_dir(project_dir: &Path, path: &str) -> Result<(), CliError> {
    fs::create_dir_all(project_dir.join(path))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scaffolding the same directory twice succeeds — including the git path,
    /// where the second run has nothing to commit. Regression for `init`
    /// failing with "git commit failed" on an already-committed project.
    #[cfg_attr(miri, ignore)] // spawns the `git` binary
    #[mz_ore::test]
    fn scaffold_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let opts = ScaffoldOpts { init_git: true };
        scaffold(dir.path(), "proj", &opts).expect("first scaffold should succeed");
        scaffold(dir.path(), "proj", &opts).expect("second scaffold should be idempotent");
        assert!(dir.path().join("project.toml").exists());
    }

    /// Re-scaffolding never overwrites a file the user already has.
    #[cfg_attr(miri, ignore)] // touches the filesystem
    #[mz_ore::test]
    fn scaffold_preserves_existing_files() {
        let dir = tempfile::tempdir().unwrap();
        let custom = "mz_version = \"cloud\"\ndependencies = [\"app.public.foo\"]\n";
        fs::write(dir.path().join("project.toml"), custom).unwrap();

        scaffold(dir.path(), "proj", &ScaffoldOpts { init_git: false })
            .expect("scaffold should succeed over an existing project.toml");

        let contents = fs::read_to_string(dir.path().join("project.toml")).unwrap();
        assert_eq!(contents, custom, "existing project.toml must be preserved");
    }
}
