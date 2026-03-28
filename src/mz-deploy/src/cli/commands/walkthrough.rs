//! Scaffold a new mz-deploy project with an interactive walkthrough skill.
//!
//! Creates the same project structure as [`super::new_project`], then layers
//! on a walkthrough skill and updated README in a second commit.

use crate::cli::CliError;
use crate::cli::commands::new_project::{self, ScaffoldOpts};
use crate::cli::progress;
use crate::info;
use std::fs;
use std::path::Path;
use std::process::Command;

const WALKTHROUGH_SKILL_MD: &str = include_str!("../scaffold/walkthrough/SKILL.md");
const WALKTHROUGH_DOCKER_COMPOSE: &str =
    include_str!("../scaffold/walkthrough/references/docker-compose.yml");
const WALKTHROUGH_POSTGRES_INIT: &str =
    include_str!("../scaffold/walkthrough/references/postgres-init.sql");
const WALKTHROUGH_SETUP_SH: &str = include_str!("../scaffold/walkthrough/references/setup.sh");
const WALKTHROUGH_PROFILES_TOML: &str = "\
[default]
host = \"localhost\"
port = 6875
username = \"materialize\"
";
const WALKTHROUGH_README: &str = include_str!("../scaffold/walkthrough-README.md");

pub fn run(name: &str, init_git: bool) -> Result<(), CliError> {
    // Step 1: scaffold the standard project (with first commit if git)
    new_project::run(
        name,
        ScaffoldOpts {
            init_git,
            install_skill: true,
        },
    )?;

    let project_dir = Path::new(name);

    // Step 2: add walkthrough skill files
    progress::info("Inscribing the walkthrough scrolls");

    create_dir(project_dir, ".agents/skills/walkthrough/references")?;

    add_file(
        project_dir,
        ".agents/skills/walkthrough/SKILL.md",
        WALKTHROUGH_SKILL_MD,
    )?;
    add_file(
        project_dir,
        ".agents/skills/walkthrough/references/docker-compose.yml",
        WALKTHROUGH_DOCKER_COMPOSE,
    )?;
    add_file(
        project_dir,
        ".agents/skills/walkthrough/references/postgres-init.sql",
        WALKTHROUGH_POSTGRES_INIT,
    )?;
    add_file(
        project_dir,
        ".agents/skills/walkthrough/references/setup.sh",
        WALKTHROUGH_SETUP_SH,
    )?;

    // Make setup.sh executable
    use std::os::unix::fs::PermissionsExt;
    let setup_path = project_dir.join(".agents/skills/walkthrough/references/setup.sh");
    fs::set_permissions(&setup_path, fs::Permissions::from_mode(0o755))
        .map_err(|e| CliError::Message(format!("failed to set setup.sh permissions: {}", e)))?;

    std::os::unix::fs::symlink(
        "../../.agents/skills/walkthrough",
        project_dir.join(".claude/skills/walkthrough"),
    )
    .map_err(|e| {
        CliError::Message(format!(
            "failed to create .claude/skills/walkthrough symlink: {}",
            e
        ))
    })?;

    // Step 3: add profiles.toml for local Docker connection
    add_file(project_dir, "profiles.toml", WALKTHROUGH_PROFILES_TOML)?;

    // Step 4: overwrite README with walkthrough version
    add_file(
        project_dir,
        "README.md",
        &WALKTHROUGH_README.replace("{{name}}", name),
    )?;

    // Step 5: second commit
    if init_git {
        progress::info("Sealing the scrolls into the archive");

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
                "--author",
                "Materialize Inc <noreply@materialize.com>",
                "-m",
                "Add walkthrough skill",
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

    info!();
    progress::success(&format!(
        "The realm of `{}` is prepared. Your quest awaits, Lorekeeper.",
        name
    ));
    info!();
    progress::info("To begin the walkthrough, open the project in Claude Code:");
    info!("      cd {name}");
    info!("      claude");
    info!("    Then type: /walkthrough");

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
