//! Scaffold a new mz-deploy project directory.
//!
//! Creates the standard directory layout (`models/`, `clusters/`, `roles/`),
//! writes starter `project.toml` and `profiles.toml` files, and optionally
//! initializes a git repository.

use crate::cli::CliError;
use crate::utils::progress;
use std::fs;
use std::path::Path;
use std::process::Command;

pub async fn run(name: &str, init_git: bool) -> Result<(), CliError> {
    let project_dir = Path::new(name);

    if project_dir.exists() {
        return Err(CliError::Message(format!(
            "destination `{}` already exists",
            name
        )));
    }

    fs::create_dir_all(project_dir.join("models/materialize/public"))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))?;
    fs::create_dir_all(project_dir.join("clusters"))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))?;
    fs::create_dir_all(project_dir.join("roles"))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))?;

    for path in &[
        "models/materialize/public/.gitkeep",
        "clusters/.gitkeep",
        "roles/.gitkeep",
    ] {
        fs::write(project_dir.join(path), "")
            .map_err(|e| CliError::Message(format!("failed to write {}: {}", path, e)))?;
    }

    // Write .gitignore
    fs::write(project_dir.join(".gitignore"), ".mz-deploy/\n")
        .map_err(|e| CliError::Message(format!("failed to write .gitignore: {}", e)))?;

    // Write project.toml
    fs::write(
        project_dir.join("project.toml"),
        "profile = \"default\"\nmz_version = \"cloud\"\n",
    )
    .map_err(|e| CliError::Message(format!("failed to write project.toml: {}", e)))?;

    fs::write(
        project_dir.join("README.md"),
        format!(
            "# {}\n\n\
             A [Materialize](https://materialize.com) project managed by mz-deploy.\n\n\
             ## Project structure\n\n\
             - `models/` — SQL model definitions organized by database and schema\n\
             - `clusters/` — Cluster definitions\n\
             - `roles/` — Role definitions\n\
             - `project.toml` — Project configuration\n",
            name
        ),
    )
    .map_err(|e| CliError::Message(format!("failed to write README.md: {}", e)))?;

    if init_git {
        let status = Command::new("git")
            .args(["init", name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git init: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git init failed".to_string()));
        }
    }

    progress::success(&format!("Created project `{}`", name));
    Ok(())
}
