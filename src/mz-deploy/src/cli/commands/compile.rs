// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile command — validate project and show deployment plan.
//!
//! Compiles the project through a multi-stage pipeline:
//!
//! 1. **Parse** — Load and parse SQL files from the project directory.
//! 2. **Validate** — Check project structure and dependencies.
//! 3. **Build graph** — Assemble the dependency-aware project graph.
//! 4. **Typecheck** — Incrementally validate SQL against Materialize. Only
//!    objects whose definitions changed since the last build are re-validated;
//!    unchanged builds skip typechecking entirely.
//! 5. **Display** — Print the deployment plan with dependencies and SQL.

use crate::cli::CliError;
use crate::cli::progress;
use crate::config::Settings;
use crate::project::ir::graph::Project;
use crate::{project, verbose};
use std::time::Instant;

/// Compile and validate the project, showing the deployment plan.
///
/// This command:
/// - Loads and parses SQL files from the project directory
/// - Validates the project structure and dependencies
/// - Type-checks SQL statements (incremental when possible)
/// - Displays the deployment plan including dependencies and SQL statements
///
/// Type checking uses compiler-owned incremental artifacts to identify dirty
/// runtime objects. Dependencies are restored lazily, and unchanged builds
/// skip type checking entirely.
///
/// # Arguments
/// * `settings` - Resolved project and profile configuration
/// * `show_progress` - If true, displays progress indicators during compilation
///
/// # Returns
/// Compiled planned project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
pub async fn run(settings: &Settings, show_progress: bool) -> Result<Project, CliError> {
    run_with_fs(settings, show_progress, crate::fs::FileSystem::new()).await
}

/// Like [`run`] but uses the provided [`crate::fs::FileSystem`] (typically an
/// overlay built from unsaved editor buffers) instead of constructing a
/// disk-only one.
pub(crate) async fn run_with_fs(
    settings: &Settings,
    show_progress: bool,
    fs: crate::fs::FileSystem,
) -> Result<Project, CliError> {
    let settings = settings.clone();
    mz_ore::task::spawn_blocking(
        || "compile-run",
        move || run_inner(&settings, show_progress, false, fs),
    )
    .await
}

/// Compile the project without type checking.
///
/// Used by `apply` commands which create infrastructure objects that don't
/// exist yet in the database — type checking would fail because it validates
/// views against the live catalog, but the tables they reference haven't
/// been created yet.
pub async fn run_without_typecheck(
    settings: &Settings,
    show_progress: bool,
) -> Result<Project, CliError> {
    let settings = settings.clone();
    mz_ore::task::spawn_blocking(
        || "compile-run",
        move || run_inner(&settings, show_progress, true, crate::fs::FileSystem::new()),
    )
    .await
}

fn run_inner(
    settings: &Settings,
    show_progress: bool,
    skip_typecheck: bool,
    fs: crate::fs::FileSystem,
) -> Result<Project, CliError> {
    let start_time = Instant::now();
    let directory = &settings.directory;

    if show_progress {
        let canonical = directory.canonicalize();
        let shown = canonical.as_deref().unwrap_or(directory);
        progress::action("Compiling", &shown.display().to_string());
    }

    let planned_project = project::plan_sync(
        &fs,
        directory.clone(),
        settings.profile_name(),
        settings.profile_suffix(),
        settings.variables(),
    )?;

    let validation = project::analysis::deps::validate_dependencies(
        &settings.dependencies,
        &planned_project.external_dependencies,
    );

    if !validation.unused.is_empty() {
        let mut unused: Vec<_> = validation.unused.iter().collect();
        unused.sort();
        for dep in unused {
            progress::warn(&format!(
                "unused dependency: \"{}\" is declared in project.toml but not referenced",
                dep
            ));
        }
    }

    if !validation.undeclared.is_empty() {
        let mut undeclared: Vec<_> = validation.undeclared.into_iter().collect();
        undeclared.sort();
        return Err(CliError::UndeclaredDependencies { undeclared });
    }

    if !skip_typecheck {
        typecheck_project(settings, &planned_project)?;
    }

    if show_progress && crate::log::verbose_enabled() {
        print_verbose_details(&planned_project);
    }

    if show_progress {
        let total_duration = start_time.elapsed();
        progress::finished("compile", total_duration);
    }

    Ok(planned_project)
}

/// Perform type checking using the in-process catalog backend.
fn typecheck_project(settings: &Settings, planned_project: &Project) -> Result<(), CliError> {
    let directory = &settings.directory;
    use crate::project::compiler::typecheck;

    let external_types = crate::types::load_types_lock(directory).unwrap_or_default();

    let (_, stats) = typecheck::run(
        directory,
        settings.profile_name().unwrap_or(""),
        settings.profile_suffix(),
        settings.variables(),
        planned_project,
        external_types,
    )?;
    crate::verbose!(
        "typecheck: ran={} skipped={} schema_stable={} schema_changed={}",
        stats.ran,
        stats.skipped,
        stats.schema_stable,
        stats.schema_changed,
    );

    Ok(())
}

/// Print verbose details about the project (only shown with VERBOSE env var)
fn print_verbose_details(planned_project: &Project) {
    print_external_dependencies(planned_project);
    print_cluster_dependencies(planned_project);
    print_dependency_graph(planned_project);
}

/// Prints dependencies that are referenced but not declared in this project tree.
///
/// These are the objects operators must provision externally before deployment.
fn print_external_dependencies(planned_project: &Project) {
    if planned_project.external_dependencies.is_empty() {
        return;
    }
    verbose!("\nExternal Dependencies (not defined in this project):");
    let mut external: Vec<_> = planned_project.external_dependencies.iter().collect();
    external.sort();
    for dep in external {
        verbose!("  - {}", dep);
    }
}

/// Prints cluster prerequisites inferred from object and index definitions.
fn print_cluster_dependencies(planned_project: &Project) {
    if planned_project.cluster_dependencies.is_empty() {
        return;
    }
    verbose!("\nCluster Dependencies:");
    let mut clusters: Vec<_> = planned_project.cluster_dependencies.iter().collect();
    clusters.sort_by_key(|c| &c.name);
    for cluster in clusters {
        verbose!("  - {}", cluster.name);
    }
}

/// Prints per-object dependency edges for troubleshooting deployment ordering.
///
/// External dependencies are annotated inline to separate project-internal edges
/// from dependencies that are expected to pre-exist.
fn print_dependency_graph(planned_project: &Project) {
    verbose!("\nDependency Graph:");
    for (object_id, deps) in &planned_project.dependency_graph {
        if deps.is_empty() {
            continue;
        }
        verbose!("  {} depends on:", object_id);
        for dep in deps {
            if planned_project.external_dependencies.contains(dep) {
                verbose!("    - {} (external)", dep);
            } else {
                verbose!("    - {}", dep);
            }
        }
    }
}
