//! Compile command - validate project and show deployment plan.

use crate::cli::CliError;
use crate::{project, verbose};
use std::path::Path;
use std::time::SystemTime;

/// Arguments for the compile command
#[derive(Debug, Clone)]
pub struct CompileArgs {
    /// Enable type checking with Docker
    pub typecheck: bool,
    /// Docker image to use for type checking
    pub docker_image: Option<String>,
}

impl Default for CompileArgs {
    fn default() -> Self {
        Self {
            typecheck: true,
            docker_image: None,
        }
    }
}

/// Compile and validate the project, showing the deployment plan.
///
/// This command:
/// - Loads and parses SQL files from the project directory
/// - Validates the project structure and dependencies
/// - Performs optional type checking with Docker
/// - Displays the deployment plan including dependencies and SQL statements
///
/// # Arguments
/// * `directory` - Project root directory
/// * `args` - Compile command arguments
///
/// # Returns
/// Compiled MIR project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
pub async fn run(directory: &Path, args: CompileArgs) -> Result<project::mir::Project, CliError> {
    let now = SystemTime::now();

    let mir_project = project::plan(directory)?;
    println!("Loading project from: {}", directory.display());

    // Type checking with Docker if enabled
    if args.typecheck {
        typecheck_with_docker(directory, &mir_project, args.docker_image).await?;
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

/// Perform type checking using Docker
async fn typecheck_with_docker(
    directory: &Path,
    mir_project: &project::mir::Project,
    docker_image: Option<String>,
) -> Result<(), CliError> {
    use crate::types::{DockerTypeChecker, TypeChecker, TypeCheckError};

    verbose!("Starting type checking with Docker...");

    // Load types.lock if it exists
    let types = crate::types::load_types_lock(directory).unwrap_or_else(|_| {
        println!("No types.lock found, assuming no external dependencies");
        println!("See gen-data-contracts for more information");
        crate::types::Types {
            version: 1,
            objects: std::collections::BTreeMap::new(),
        }
    });

    // Create Docker type checker
    let mut checker = DockerTypeChecker::new(types, directory);
    if let Some(image) = docker_image {
        checker = checker.with_image(image);
    }

    // Run type checking
    match checker.typecheck(mir_project).await {
        Ok(()) => {
            verbose!("✓ Type checking passed");
            Ok(())
        }
        Err(TypeCheckError::ContainerStartFailed(e)) => {
            // Docker not available, warn but don't fail
            println!("Warning: Docker not available for type checking: {}", e);
            println!("  Type checking skipped. Install Docker to enable type checking.");
            Ok(())
        }
        Err(e) => {
            // Real type checking errors
            Err(CliError::TypeCheckFailed(format!("{}", e)))
        }
    }
}

