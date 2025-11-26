use std::process::Command;

/// Get the current git commit hash of the project directory.
///
/// Returns Some(commit_hash) if the directory is a git repository with a valid HEAD,
/// otherwise returns None.
///
/// # Arguments
/// * `directory` - Path to the project directory
pub fn get_git_commit(directory: &std::path::Path) -> Option<String> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(directory)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let commit = String::from_utf8(output.stdout).ok()?;
    Some(commit.trim().to_string())
}

/// Returns `true` if the repo contains any uncommitted or unstaged changes.
///
/// This uses `git status --porcelain`, which outputs a line for each changed file.
/// Any non-empty output means the repo is dirty.
///
/// # Arguments
/// * `directory` - Path to the project directory
pub fn is_dirty(directory: &std::path::Path) -> bool {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(directory)
        .output();

    let out = match output {
        Ok(out) => out,
        Err(_) => return false,
    };

    !String::from_utf8_lossy(&out.stdout).trim().is_empty()
}
