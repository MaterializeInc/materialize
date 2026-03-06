//! Scaffold a new mz-deploy project directory.
//!
//! Creates the standard directory layout (`models/`, `clusters/`, `roles/`),
//! writes starter `project.toml` and `profiles.toml` files, and optionally
//! initializes a git repository.

use crate::cli::CliError;
use crate::cli::progress;
use std::fs;
use std::path::Path;
use std::process::Command;

const GITIGNORE: &str = include_str!("../scaffold/gitignore");
const PROJECT_TOML: &str = include_str!("../scaffold/project.toml");
const README_MD: &str = include_str!("../scaffold/README.md");
const SKILL_MD: &str = include_str!("../scaffold/skill/skill.md");
const AGENT_MD: &str = include_str!("../scaffold/AGENT.md");
const UNIT_TEST_REFERENCE_MD: &str = include_str!("../scaffold/skill/references/unit-tests.md");

pub async fn run(name: &str, init_git: bool) -> Result<(), CliError> {
    let project_dir = Path::new(name);

    if project_dir.exists() {
        return Err(CliError::Message(format!(
            "destination `{}` already exists",
            name
        )));
    }

    create_dir(project_dir, "models/materialize/public")?;
    create_dir(project_dir, "clusters")?;
    create_dir(project_dir, "roles")?;
    create_dir(project_dir, ".agents/skills/mz-deploy/references")?;
    create_dir(project_dir, ".claude/skills")?;
    add_file(project_dir, "models/materialize/public/.gitkeep", "")?;
    add_file(project_dir, "clusters/.gitkeep", "")?;
    add_file(project_dir, "roles/.gitkeep", "")?;
    add_file(project_dir, ".gitignore", GITIGNORE)?;
    add_file(project_dir, "project.toml", PROJECT_TOML)?;
    add_file(
        project_dir,
        "README.md",
        &README_MD.replace("{{name}}", name),
    )?;
    add_file(project_dir, "AGENT.md", AGENT_MD)?;
    add_file(project_dir, "CLAUDE.md", AGENT_MD)?;
    add_file(project_dir, ".agents/skills/mz-deploy/SKILL.md", SKILL_MD)?;
    add_file(
        project_dir,
        ".agents/skills/mz-deploy/references/unit-tests.md",
        UNIT_TEST_REFERENCE_MD,
    )?;

    std::os::unix::fs::symlink(
        "../../.agents/skills/mz-deploy",
        project_dir.join(".claude/skills/mz-deploy"),
    )
    .map_err(|e| CliError::Message(format!("failed to create .claude/skills symlink: {}", e)))?;

    install_agent_skills(project_dir)?;

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

        let status = Command::new("git")
            .args([
                "commit",
                "-m",
                "Initial commit\n\nCo-Authored-By: Materialize Inc <noreply@materialize.com>",
            ])
            .current_dir(project_dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| CliError::Message(format!("failed to run git commit: {}", e)))?;

        if !status.success() {
            return Err(CliError::Message("git commit failed".to_string()));
        }
    }

    progress::success(&format!("Created project `{}`", name));
    Ok(())
}

fn add_file(project_dir: &Path, file: &str, content: &str) -> Result<(), CliError> {
    fs::write(project_dir.join(file), content)
        .map_err(|e| CliError::Message(format!("failed to write {}: {}", file, e)))
}

fn create_dir(project_dir: &Path, path: &str) -> Result<(), CliError> {
    fs::create_dir_all(project_dir.join(path))
        .map_err(|e| CliError::Message(format!("failed to create directories: {}", e)))
}

/// Runs `npx skills add` to install community agent skills.
/// Warns and continues if `npx` is not available or the command fails.
fn install_agent_skills(project_dir: &Path) -> Result<(), CliError> {
    let status = Command::new("npx")
        .args([
            "-y",
            "skills",
            "add",
            "MaterializeInc/agent-skills",
            "-a",
            "universal",
            "-a",
            "claude",
        ])
        .current_dir(project_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(s) if s.success() => {}
        Ok(_) => {
            progress::warn("npx skills add failed; skipping community agent skills");
            return Ok(());
        }
        Err(e) => {
            progress::warn(&format!(
                "failed to run npx skills add: {}; skipping community agent skills",
                e
            ));
            return Ok(());
        }
    }

    Ok(())
}
