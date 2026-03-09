//! Compile command - validate project and show deployment plan.

use crate::cli::CliError;
use crate::cli::TypeCheckMode;
use crate::cli::progress;
use crate::config::Settings;
use crate::project::object_id::ObjectId;
use crate::{project, verbose};
use std::path::Path;
use std::time::{Duration, Instant};

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
/// Compiled planned project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
pub async fn run(
    settings: &Settings,
    skip_typecheck: bool,
) -> Result<project::planned::Project, CliError> {
    let start_time = Instant::now();
    let directory = &settings.directory;

    let typecheck = if skip_typecheck {
        TypeCheckMode::Disabled
    } else {
        TypeCheckMode::Enabled {
            image: settings.docker_image.clone(),
        }
    };

    progress::info(&format!("Loading project from: {}", directory.display()));

    // Stage 1: Parse and validate SQL files
    progress::stage_start("Parsing SQL files");
    let parse_start = Instant::now();
    let planned_project = project::plan(directory, &settings.profile_name, settings.suffix())?;
    let parse_duration = parse_start.elapsed();

    // Count objects and schemas
    let object_count: usize = planned_project
        .databases
        .iter()
        .flat_map(|db| &db.schemas)
        .map(|schema| schema.objects.len())
        .sum();
    let schema_count: usize = planned_project
        .databases
        .iter()
        .map(|db| db.schemas.len())
        .sum();

    progress::stage_success(
        &format!("Found {} objects in {} schemas", object_count, schema_count),
        parse_duration,
    );

    // Stage 2: Validate project structure
    progress::stage_start("Validating project structure");
    let validate_start = Instant::now();

    // Topological sort validates the project (detects cycles)
    let sorted = planned_project.topological_sort()?;
    let validate_duration = validate_start.elapsed();

    progress::stage_success(
        &format!("All {} objects validated", sorted.len()),
        validate_duration,
    );

    // Stage 3: Build dependency graph
    progress::stage_start("Building dependency graph");
    let deps_start = Instant::now();

    // Count internal dependencies (excluding external)
    let internal_dep_count: usize = planned_project
        .dependency_graph
        .values()
        .map(|deps| {
            deps.iter()
                .filter(|dep| !planned_project.external_dependencies.contains(dep))
                .count()
        })
        .sum();

    let deps_duration = deps_start.elapsed();
    progress::stage_success(
        &format!("Resolved {} dependencies", internal_dep_count),
        deps_duration,
    );

    // Show additional info
    if !planned_project.external_dependencies.is_empty() {
        progress::info(&format!(
            "{} external dependencies detected",
            planned_project.external_dependencies.len()
        ));
    }
    if !planned_project.cluster_dependencies.is_empty() {
        progress::info(&format!(
            "{} clusters required",
            planned_project.cluster_dependencies.len()
        ));
    }

    // Type checking with Docker if enabled
    if let TypeCheckMode::Enabled { image } = &typecheck {
        let typecheck_duration = typecheck_with_docker(directory, &planned_project, image).await?;

        if let Some(duration) = typecheck_duration {
            progress::stage_success(&format!("{} objects passed", object_count), duration);
        }
    }

    // Show verbose details if requested
    if crate::log::verbose_enabled() {
        print_verbose_details(&planned_project, &sorted);
    }

    // Final summary
    let total_duration = start_time.elapsed();
    progress::summary("Project successfully compiled", total_duration);

    Ok(planned_project)
}

/// Perform type checking using Docker
async fn typecheck_with_docker(
    directory: &Path,
    planned_project: &project::planned::Project,
    docker_image: &str,
) -> Result<Option<Duration>, CliError> {
    use crate::types::docker_runtime::DockerRuntime;
    use crate::types::{TypeCheckError, typecheck_with_client};

    progress::stage_start("Type checking with Docker");
    let typecheck_start = Instant::now();

    // Load types.lock if it exists
    let types = crate::types::load_types_lock(directory).unwrap_or_else(|_| {
        progress::info("No types.lock found, assuming no external dependencies");
        progress::info("See SET api = stable for more information");
        crate::types::Types {
            version: 1,
            objects: std::collections::BTreeMap::new(),
        }
    });

    // Create Docker runtime
    let runtime = DockerRuntime::new().with_image(docker_image);

    // Get connected client with staged dependencies
    let mut client = match runtime.get_client(&types).await {
        Ok(client) => client,
        Err(TypeCheckError::ContainerStartFailed(e)) => {
            // Docker not available, warn but don't fail
            progress::info(&format!("Docker not available: {}", e));
            progress::info("Type checking skipped. Install Docker to enable type checking.");
            return Ok(None);
        }
        Err(e) => {
            return Err(e.into());
        }
    };

    // Run type checking
    match typecheck_with_client(&mut client, planned_project, directory).await {
        Ok(()) => {
            let duration = typecheck_start.elapsed();
            Ok(Some(duration))
        }
        Err(e) => {
            // Real type checking errors
            Err(e.into())
        }
    }
}

/// Print verbose details about the project (only shown with VERBOSE env var)
fn print_verbose_details(planned_project: &project::planned::Project, sorted: &[ObjectId]) {
    let mod_stmts = planned_project.iter_mod_statements();
    print_external_dependencies(planned_project);
    print_cluster_dependencies(planned_project);
    print_dependency_graph(planned_project);
    print_deployment_order(sorted);
    print_module_setup_statements(&mod_stmts);
    print_full_sql_plan(&mod_stmts);
    print_sorted_object_sql(planned_project);
}

/// Prints dependencies that are referenced but not declared in this project tree.
///
/// These are the objects operators must provision externally before deployment.
fn print_external_dependencies(planned_project: &project::planned::Project) {
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
fn print_cluster_dependencies(planned_project: &project::planned::Project) {
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
fn print_dependency_graph(planned_project: &project::planned::Project) {
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

/// Prints final object deployment order derived from topological sorting.
fn print_deployment_order(sorted: &[ObjectId]) {
    verbose!("\nDeployment order:");
    for (idx, object_id) in sorted.iter().enumerate() {
        verbose!("  {}. {}", idx + 1, object_id);
    }
}

/// Prints module setup statements that run before object SQL.
///
/// This section shows database/schema-level setup artifacts separately from
/// object creation steps, which helps explain side-effect ordering.
fn print_module_setup_statements(mod_stmts: &[project::ModStatement]) {
    if mod_stmts.is_empty() {
        return;
    }
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

/// Prints executable SQL for module setup statements in run order.
fn print_full_sql_plan(mod_stmts: &[project::ModStatement]) {
    verbose!("\nSQL Deployment Plan (fully qualified)");
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
}

/// Prints full SQL payload for each object deployment step.
///
/// Includes statement SQL and attached indexes/grants/comments so verbose output
/// reflects what the deploy command would execute end-to-end.
fn print_sorted_object_sql(planned_project: &project::planned::Project) {
    if let Ok(objects) = planned_project.get_sorted_objects() {
        for (idx, (object_id, typed_obj)) in objects.iter().enumerate() {
            verbose!("-- Step {}: {}", idx + 1, object_id);
            verbose!("{};", typed_obj.stmt);
            for index in &typed_obj.indexes {
                verbose!("{};", index);
            }
            for grant in &typed_obj.grants {
                verbose!("{};", grant);
            }
            for comment in &typed_obj.comments {
                verbose!("{};", comment);
            }
            verbose!();
        }
    }
}
