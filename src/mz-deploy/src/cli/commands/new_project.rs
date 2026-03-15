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
const SKILL_MD: &str = include_str!("../scaffold/skill/SKILL.md");
const AGENT_MD: &str = include_str!("../scaffold/AGENT.md");
const STABLE_API_REFERENCE_MD: &str = include_str!("../scaffold/skill/references/stable-api.md");
const WORKFLOW_TEST: &str = include_str!("../scaffold/workflows/test.yml");
const WORKFLOW_STAGE: &str = include_str!("../scaffold/workflows/stage.yml");
const WORKFLOW_HYDRATION: &str = include_str!("../scaffold/workflows/hydration.yml");
const WORKFLOW_DEPLOY: &str = include_str!("../scaffold/workflows/deploy.yml");
const WORKFLOW_CLEANUP: &str = include_str!("../scaffold/workflows/cleanup.yml");
const WORKFLOW_README: &str = include_str!("../scaffold/workflows/README.md");

/// Shared options for project scaffolding.
pub struct ScaffoldOpts {
    pub init_git: bool,
    pub install_skill: bool,
}

/// `mz-deploy new <name>` — create a new directory and scaffold into it.
pub fn run(name: &str, opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(name);

    progress::info(&format!("Creating project {name}..."));
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
    Ok(())
}

/// `mz-deploy init` — scaffold the current directory as an mz-deploy project.
pub fn init(opts: ScaffoldOpts) -> Result<(), CliError> {
    let project_dir = Path::new(".");
    let name = std::env::current_dir()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "my-project".to_string());

    progress::info("Initializing project in current directory...");
    scaffold(project_dir, &name, &opts)?;
    progress::success(&format!("Initialized project `{}`", name));
    Ok(())
}

/// Common scaffolding logic shared by `new` and `init`.
fn scaffold(project_dir: &Path, name: &str, opts: &ScaffoldOpts) -> Result<(), CliError> {
    create_dir(project_dir, "models/materialize/public")?;
    create_dir(project_dir, "clusters")?;
    create_dir(project_dir, "roles")?;
    create_dir(project_dir, "network-policies")?;
    create_dir(project_dir, ".agents/skills/mz-deploy/references")?;
    create_dir(project_dir, ".claude/skills")?;
    create_dir(project_dir, ".github/workflows")?;
    add_file(project_dir, "models/materialize/public/.gitkeep", "")?;
    add_file(project_dir, "clusters/.gitkeep", "")?;
    add_file(project_dir, "roles/.gitkeep", "")?;
    add_file(project_dir, "network-policies/.gitkeep", "")?;
    add_file(project_dir, ".gitignore", GITIGNORE)?;
    add_file(project_dir, "project.toml", PROJECT_TOML)?;
    add_file(
        project_dir,
        "README.md",
        &README_MD.replace("{{name}}", name),
    )?;
    add_file(project_dir, "AGENT.md", AGENT_MD)?;
    add_file(project_dir, "CLAUDE.md", AGENT_MD)?;
    add_file(project_dir, ".github/workflows/test.yml", WORKFLOW_TEST)?;
    add_file(project_dir, ".github/workflows/stage.yml", WORKFLOW_STAGE)?;
    add_file(
        project_dir,
        ".github/workflows/hydration.yml",
        WORKFLOW_HYDRATION,
    )?;
    add_file(project_dir, ".github/workflows/deploy.yml", WORKFLOW_DEPLOY)?;
    add_file(
        project_dir,
        ".github/workflows/cleanup.yml",
        WORKFLOW_CLEANUP,
    )?;
    add_file(project_dir, ".github/workflows/README.md", WORKFLOW_README)?;
    add_file(project_dir, ".agents/skills/mz-deploy/SKILL.md", SKILL_MD)?;
    add_file(
        project_dir,
        ".agents/skills/mz-deploy/references/stable-api.md",
        STABLE_API_REFERENCE_MD,
    )?;

    std::os::unix::fs::symlink(
        "../../.agents/skills/mz-deploy",
        project_dir.join(".claude/skills/mz-deploy"),
    )
    .map_err(|e| CliError::Message(format!("failed to create .claude/skills symlink: {}", e)))?;

    if opts.install_skill {
        install_agent_skills(project_dir)?;
    }

    if opts.init_git {
        progress::info("Initializing git repository");
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

        let status = Command::new("git")
            .args([
                "commit",
                "--author",
                "Materialize Inc <noreply@materialize.com>",
                "-m",
                "Initial commit",
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
    progress::info("Installing Materialize agent skill");
    let status = Command::new("npx")
        .args([
            "-y",
            "skills",
            "add",
            "MaterializeInc/agent-skills",
            "-a",
            "universal",
            "-a",
            "claude-code",
            "--project",
            "--yes",
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
