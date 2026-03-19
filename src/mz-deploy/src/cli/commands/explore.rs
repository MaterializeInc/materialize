//! Generate interactive project documentation as a self-contained HTML file.
//!
//! Compiles the project (no database connection needed), extracts all metadata
//! into a [`DocsManifest`](manifest::DocsManifest), and writes a single
//! `index.html` that includes an interactive DAG, object detail pages, and
//! search.

pub mod manifest;

use crate::cli::CliError;
use crate::config::Settings;
use crate::{project, verbose};
use manifest::build_manifest;
use std::path::PathBuf;

/// The compiled HTML template with inlined CSS and JS.
///
/// Built by `build.rs` from the separate source files in `frontend/`.
const DOCS_TEMPLATE: &str = include_str!(concat!(env!("OUT_DIR"), "/docs_template.html"));

/// Generate documentation and write to the output directory.
///
/// # Arguments
/// * `settings` - Project settings (directory, profile, etc.)
/// * `output_dir` - Directory for generated docs (default: `target/docs/`)
/// * `open` - Whether to open the generated HTML in a browser (default: true)
pub async fn run(
    settings: &Settings,
    output_dir: Option<PathBuf>,
    open: bool,
) -> Result<(), CliError> {
    let directory = &settings.directory;

    verbose!("Compiling project for documentation...");

    let planned_project = project::plan(
        directory.clone(),
        settings.profile_name.clone(),
        settings.profile_suffix().map(|s| s.to_owned()),
        settings.variables().clone(),
    )
    .await?;

    // Load types if available
    let types_lock = crate::types::load_types_lock(directory).ok();
    let types_cache = crate::types::load_types_cache(directory).ok();
    let merged_types = match (types_lock, types_cache) {
        (Some(mut lock), Some(cache)) => {
            lock.merge(&cache);
            Some(lock)
        }
        (Some(lock), None) => Some(lock),
        (None, Some(cache)) => Some(cache),
        (None, None) => None,
    };

    // Derive project name from directory
    let project_name = directory
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "mz-deploy project".to_string());

    // Load cluster definitions (best-effort — missing clusters dir is fine)
    let cluster_defs = crate::project::clusters::load_clusters(
        directory,
        &settings.profile_name,
        settings.profile_suffix(),
        settings.variables(),
    )
    .unwrap_or_default();

    // Load test results (best-effort — missing file is fine)
    let test_results: Option<manifest::DocsTestResults> = {
        let path = directory.join("target").join("test-results.json");
        std::fs::File::open(&path)
            .ok()
            .and_then(|f| serde_json::from_reader(f).ok())
    };

    let manifest = build_manifest(
        &planned_project,
        merged_types.as_ref(),
        &project_name,
        &cluster_defs,
        test_results,
    );

    let json = serde_json::to_string(&manifest)
        .map_err(|e| CliError::Message(format!("failed to serialize manifest: {}", e)))?;

    let html = DOCS_TEMPLATE.replace("__MANIFEST_JSON__", &json);

    // Write output
    let out_dir = output_dir.unwrap_or_else(|| directory.join("target").join("explore"));
    std::fs::create_dir_all(&out_dir).map_err(|e| {
        CliError::Message(format!(
            "failed to create output directory {}: {}",
            out_dir.display(),
            e
        ))
    })?;

    let out_path = out_dir.join("index.html");
    std::fs::write(&out_path, html)
        .map_err(|e| CliError::Message(format!("failed to write {}: {}", out_path.display(), e)))?;

    crate::cli::progress::info(&format!("Documentation written to {}", out_path.display()));

    if open {
        open_in_browser(&out_path);
    }

    Ok(())
}

/// Open a file in the default browser.
fn open_in_browser(path: &std::path::Path) {
    let result = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(path).spawn()
    } else if cfg!(target_os = "linux") {
        std::process::Command::new("xdg-open").arg(path).spawn()
    } else if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/C", "start"])
            .arg(path)
            .spawn()
    } else {
        return;
    };

    if let Err(e) = result {
        verbose!("Could not open browser: {}", e);
    }
}
