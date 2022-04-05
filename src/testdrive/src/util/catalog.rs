use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Creates a temporary copy of Materialize's catalog
///
/// This is useful because running validations against the catalog can conflict with the running
/// Materialize instance. Therefore it's better to run validations on a copy of the catalog.
pub fn catalog_copy(catalog_path: &PathBuf) -> Result<TempDir, anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    fs::copy(
        catalog_path.join("catalog"),
        temp_dir.path().join("catalog"),
    )?;
    Ok(temp_dir)
}
