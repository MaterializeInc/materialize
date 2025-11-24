/// Get the current git commit hash of the project directory.
///
/// Returns Some(commit_hash) if the directory is a git repository with a valid HEAD,
/// otherwise returns None.
///
/// # Arguments
/// * `directory` - Path to the project directory
pub fn get_git_commit(directory: &std::path::Path) -> Option<String> {
    use std::process::Command;

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
