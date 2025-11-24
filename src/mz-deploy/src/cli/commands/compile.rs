//! Compile command - validate project and show deployment plan.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::client::config::ConfigError;
use crate::{project, verbose};
use std::path::Path;
use std::time::SystemTime;

/// Compile and validate the project, showing the deployment plan.
///
/// This command:
/// - Loads and parses SQL files from the project directory
/// - Validates the project structure and dependencies
/// - (Unless --offline) Connects to the database to verify external dependencies
/// - Writes types.lock file with external type information
/// - Displays the deployment plan including dependencies and SQL statements
///
/// # Arguments
/// * `profile` - Optional database profile (None if offline)
/// * `offline` - Skip database connection and dependency verification
/// * `directory` - Project root directory
///
/// # Returns
/// Compiled MIR project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
/// Returns `CliError::Connection` if database connection fails (when not offline)
pub async fn run(
    profile: Option<&Profile>,
    offline: bool,
    directory: &Path,
) -> Result<project::mir::Project, CliError> {
    let now = SystemTime::now();
    let client = if !offline {
        if let Some(p) = profile {
            Some(Client::connect_with_profile(p.clone()).await?)
        } else {
            return Err(CliError::Connection(
                crate::client::ConnectionError::Config(
                    ConfigError::ProfilesNotFound {
                        project_path: ".mz/profiles.toml".to_string(),
                        global_path: "~/.mz/profiles.toml".to_string(),
                    }
                )
            ));
        }
    } else {
        None
    };

    // Load and plan the project
    let mir_project = project::plan(directory)?;

    println!("Loading project from: {}", directory.display());

    // Validate against database if connected
    if let Some(mut client) = client {
        client
            .validate_project(&mir_project, directory)
            .await?;

        // Query external types and write types.lock
        let types = client.query_external_types(&mir_project).await?;
        types.write_types_lock(directory)?;
    }

    // Display external dependencies
    if !mir_project.external_dependencies.is_empty() {
        verbose!("External Dependencies (not defined in this project):");
        let mut external: Vec<_> = mir_project.external_dependencies.iter().collect();
        external.sort();
        for dep in external {
            verbose!("  - {}", dep);
        }
        verbose!();
    }

    // Display cluster dependencies
    if !mir_project.cluster_dependencies.is_empty() {
        verbose!("Cluster Dependencies:");
        let mut clusters: Vec<_> = mir_project.cluster_dependencies.iter().collect();
        clusters.sort_by_key(|c| &c.name);
        for cluster in clusters {
            verbose!("  - {}", cluster.name);
        }
        verbose!();
    }

    // Display dependency graph
    verbose!("Dependency Graph:");
    for (object_id, deps) in &mir_project.dependency_graph {
        if !deps.is_empty() {
            verbose!("  {} depends on:", object_id);
            for dep in deps {
                // Mark external dependencies
                if mir_project.external_dependencies.contains(dep) {
                    verbose!("    - {} (external)", dep);
                } else {
                    verbose!("    - {}", dep);
                }
            }
        }
    }

    verbose!("\nDeployment order:");
    match mir_project.topological_sort() {
        Ok(sorted) => {
            for (idx, object_id) in sorted.iter().enumerate() {
                verbose!("  {}. {}", idx + 1, object_id);
            }
        }
        Err(e) => {
            verbose!("error: {}", e);
        }
    }

    let mod_stmts = mir_project.iter_mod_statements();
    if !mod_stmts.is_empty() {
        verbose!("\nModule Setup Statements:");
        for (idx, mod_stmt) in mod_stmts.iter().enumerate() {
            match mod_stmt {
                project::ModStatement::Database {
                    database,
                    statement,
                } => {
                    verbose!("  {}. Database {}: {}", idx + 1, database, statement);
                }
                project::ModStatement::Schema {
                    database,
                    schema,
                    statement,
                } => {
                    verbose!(
                        "  {}. Schema {}.{}: {}",
                        idx + 1,
                        database,
                        schema,
                        statement
                    );
                }
            }
        }
    }

    verbose!();
    verbose!("SQL Deployment Plan (fully qualified)");

    for (idx, mod_stmt) in mod_stmts.iter().enumerate() {
        match mod_stmt {
            project::ModStatement::Database {
                database,
                statement,
            } => {
                verbose!("-- Module Setup {}: Database {}", idx + 1, database);
                verbose!("{};", statement);
                verbose!();
            }
            project::ModStatement::Schema {
                database,
                schema,
                statement,
            } => {
                verbose!(
                    "-- Module Setup {}: Schema {}.{}",
                    idx + 1,
                    database,
                    schema
                );
                verbose!("{};", statement);
                verbose!();
            }
        }
    }

    // Print objects in deployment order
    let objects = mir_project.get_sorted_objects()?;
    for (idx, (object_id, hir_obj)) in objects.iter().enumerate() {
        verbose!("-- Step {}: {}", idx + 1, object_id);
        verbose!("{};", hir_obj.stmt);

        // Print indexes for this object
        for index in &hir_obj.indexes {
            verbose!("{};", index);
        }

        // Print grants for this object
        for grant in &hir_obj.grants {
            verbose!("{};", grant);
        }

        // Print comments for this object
        for comment in &hir_obj.comments {
            verbose!("{};", comment);
        }

        verbose!();
    }

    let duration = now.elapsed().unwrap();
    println!(
        "Project successfully compiled in {}.{}s",
        duration.as_secs(),
        duration.subsec_millis()
    );
    Ok(mir_project)
}
