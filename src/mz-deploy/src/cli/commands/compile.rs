//! Compile command - validate project and show deployment plan.

use crate::cli::CliError;
use crate::cli::TypeCheckMode;
use crate::cli::progress;
use crate::config::Settings;
use crate::project::object_id::ObjectId;
use crate::{project, verbose};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

/// Compile and validate the project, showing the deployment plan.
///
/// This command:
/// - Loads and parses SQL files from the project directory
/// - Validates the project structure and dependencies
/// - Performs optional type checking with Docker (incremental when possible)
/// - Displays the deployment plan including dependencies and SQL statements
///
/// When type checking is enabled, the command uses incremental type checking:
/// it compares current AST hashes against the `typecheck.snapshot` to build a
/// dirty set, then only re-validates changed objects. If no objects changed,
/// the type check is skipped entirely.
///
/// # Arguments
/// * `settings` - Resolved project and profile configuration
/// * `skip_typecheck` - If true, disables type checking entirely
/// * `show_progress` - If true, displays progress indicators during compilation
///
/// # Returns
/// Compiled planned project ready for deployment
///
/// # Errors
/// Returns `CliError::Project` if compilation or validation fails
pub async fn run(
    settings: &Settings,
    skip_typecheck: bool,
    show_progress: bool,
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

    if show_progress {
        progress::info(&format!("Loading project from: {}", directory.display()));
    }

    // Stage 1: Parse and validate SQL files
    if show_progress {
        progress::stage_start("Parsing SQL files");
    }
    let parse_start = Instant::now();
    let planned_project = project::plan(
        directory.clone(),
        settings.profile_name.clone(),
        settings.profile_suffix().map(|s| s.to_owned()),
        settings.variables().clone(),
    )
    .await?;
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

    if show_progress {
        progress::stage_success(
            &format!("Found {} objects in {} schemas", object_count, schema_count),
            parse_duration,
        );
    }

    // Stage 2: Validate project structure
    if show_progress {
        progress::stage_start("Validating project structure");
    }
    let validate_start = Instant::now();

    // Topological sort validates the project (detects cycles)
    let sorted = planned_project.topological_sort()?;
    let validate_duration = validate_start.elapsed();

    if show_progress {
        progress::stage_success(
            &format!("All {} objects validated", sorted.len()),
            validate_duration,
        );
    }

    // Stage 3: Build dependency graph
    if show_progress {
        progress::stage_start("Building dependency graph");
    }
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
    if show_progress {
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
    }

    // Pre-typecheck constraint validation (FK target types + partial column check)
    let types_lock = crate::types::load_types_lock(directory).unwrap_or_default();
    validate_constraints_with_types(&planned_project, &types_lock)?;

    // Type checking with Docker if enabled
    if let TypeCheckMode::Enabled { image } = &typecheck {
        let typecheck_duration =
            typecheck_with_docker(directory, &planned_project, image, show_progress).await?;

        if show_progress {
            if let Some(duration) = typecheck_duration {
                progress::stage_success(&format!("{} objects passed", object_count), duration);
            }
        }

        // Post-typecheck column validation
        if let Ok(types_cache) = crate::types::load_types_cache(directory) {
            let mut full_types = types_lock.clone();
            full_types.merge(&types_cache);
            let column_map = build_column_map(&full_types);
            let col_errors =
                project::typed::validate_constraint_columns(&planned_project, &column_map);
            if !col_errors.is_empty() {
                return Err(project::error::ProjectError::from(
                    project::error::ValidationErrors::new(col_errors),
                )
                .into());
            }
        }
    }

    // Show verbose details if requested
    if show_progress && crate::log::verbose_enabled() {
        print_verbose_details(&planned_project, &sorted);
    }

    // Final summary
    if show_progress {
        let total_duration = start_time.elapsed();
        progress::summary("Project successfully compiled", total_duration);
    }

    Ok(planned_project)
}

/// Build the incremental state by comparing current AST hashes against the snapshot.
///
/// Returns `None` if there are no dirty objects and the object set hasn't changed,
/// meaning the typecheck can be skipped entirely. Returns `Some(IncrementalState)`
/// with the dirty set when incremental checking is needed.
fn build_incremental_state(
    directory: &Path,
    planned_project: &project::planned::Project,
) -> Result<Option<crate::types::IncrementalState>, CliError> {
    use crate::project::ast::Statement;
    use crate::project::deployment_snapshot::compute_typed_hash;

    // Load previous snapshot and cache
    let cached_types = crate::types::load_types_cache(directory).unwrap_or_default();
    let old_snapshot = crate::types::load_typecheck_snapshot(directory)
        .map_err(|e| CliError::Message(format!("failed to load typecheck snapshot: {}", e)))?;

    let old_hashes = match old_snapshot {
        Some(h) => h,
        None => {
            // No snapshot exists — full check needed, but we still provide
            // IncrementalState so the incremental path writes the snapshot afterward.
            // All views/MVs are dirty.
            let mut dirty = BTreeSet::new();
            let sorted = planned_project.get_sorted_objects()?;
            for (oid, typed_obj) in &sorted {
                if matches!(
                    typed_obj.stmt,
                    Statement::CreateView(_) | Statement::CreateMaterializedView(_)
                ) {
                    dirty.insert(oid.clone());
                }
            }
            return Ok(Some(crate::types::IncrementalState {
                cached_types,
                dirty,
            }));
        }
    };

    // Compute current AST hashes for all views/MVs
    let sorted = planned_project.get_sorted_objects()?;
    let mut current_hashes = BTreeMap::new();
    for (oid, typed_obj) in &sorted {
        if matches!(
            typed_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            let hash = compute_typed_hash(typed_obj);
            current_hashes.insert(oid.to_string(), hash);
        }
    }

    // Diff: find dirty objects (changed, new, or removed AST hashes)
    let mut dirty = BTreeSet::new();

    for (fqn, current_hash) in &current_hashes {
        match old_hashes.get(fqn) {
            Some(old_hash) if old_hash == current_hash => {} // unchanged
            _ => {
                // Changed or new — parse back to ObjectId
                if let Some(oid) = fqn_to_object_id(fqn) {
                    dirty.insert(oid);
                }
            }
        }
    }

    // Check for removed objects
    let current_fqns: BTreeSet<&String> = current_hashes.keys().collect();
    let old_fqns: BTreeSet<&String> = old_hashes.keys().collect();
    let has_removals = old_fqns.difference(&current_fqns).next().is_some();

    if dirty.is_empty() && !has_removals {
        // Nothing changed — skip typecheck entirely
        return Ok(None);
    }

    verbose!(
        "Incremental typecheck: {} dirty object(s), {} removed",
        dirty.len(),
        old_fqns.difference(&current_fqns).count()
    );

    Ok(Some(crate::types::IncrementalState {
        cached_types,
        dirty,
    }))
}

/// Write the typecheck snapshot with current AST hashes for all views/MVs.
fn write_current_snapshot(
    directory: &Path,
    planned_project: &project::planned::Project,
) -> Result<(), CliError> {
    use crate::project::ast::Statement;
    use crate::project::deployment_snapshot::compute_typed_hash;

    let sorted = planned_project.get_sorted_objects()?;
    let mut hashes = BTreeMap::new();
    for (oid, typed_obj) in &sorted {
        if matches!(
            typed_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            hashes.insert(oid.to_string(), compute_typed_hash(typed_obj));
        }
    }

    crate::types::write_typecheck_snapshot(directory, &hashes)
        .map_err(|e| CliError::Message(format!("failed to write typecheck snapshot: {}", e)))
}

/// Parse a `database.schema.object` FQN string back into an `ObjectId`.
fn fqn_to_object_id(fqn: &str) -> Option<ObjectId> {
    let parts: Vec<&str> = fqn.splitn(3, '.').collect();
    if parts.len() == 3 {
        Some(ObjectId {
            database: parts[0].to_string(),
            schema: parts[1].to_string(),
            object: parts[2].to_string(),
        })
    } else {
        None
    }
}

/// Perform type checking using Docker
async fn typecheck_with_docker(
    directory: &Path,
    planned_project: &project::planned::Project,
    docker_image: &str,
    show_progress: bool,
) -> Result<Option<Duration>, CliError> {
    use crate::types::docker_runtime::DockerRuntime;
    use crate::types::{TypeCheckError, typecheck_with_client};

    if show_progress {
        progress::stage_start("Type checking with Docker");
    }
    let typecheck_start = Instant::now();

    // Build incremental state before starting Docker
    let incremental = build_incremental_state(directory, planned_project)?;

    // If incremental analysis says nothing changed, skip entirely
    if incremental.is_none() {
        verbose!("Typecheck snapshot unchanged — skipping type check");
        if show_progress {
            progress::info("Types unchanged, skipping type check");
        }
        return Ok(None);
    }

    // Load types.lock if it exists
    let types = crate::types::load_types_lock(directory).unwrap_or_else(|_| {
        if show_progress {
            progress::info("No types.lock found, assuming no external dependencies");
            progress::info("See SET api = stable for more information");
        }
        crate::types::Types::default()
    });

    // Create Docker runtime
    let runtime = DockerRuntime::new().with_image(docker_image);

    // Get connected client with staged dependencies
    let mut client = match runtime.get_client(&types).await {
        Ok(client) => client,
        Err(TypeCheckError::ContainerStartFailed(e)) => {
            // Docker not available, warn but don't fail
            if show_progress {
                progress::info(&format!("Docker not available: {}", e));
                progress::info("Type checking skipped. Install Docker to enable type checking.");
            }
            return Ok(None);
        }
        Err(e) => {
            return Err(e.into());
        }
    };

    // Run type checking with incremental state
    match typecheck_with_client(&mut client, planned_project, directory, incremental).await {
        Ok(()) => {
            // Write the snapshot after successful typecheck
            write_current_snapshot(directory, planned_project)?;

            let duration = typecheck_start.elapsed();
            Ok(Some(duration))
        }
        Err(e) => {
            // Real type checking errors
            Err(e.into())
        }
    }
}

/// Validate constraint FK target types and columns against types.lock.
///
/// This runs the pre-typecheck subset of constraint validation:
/// FK target types are fully validated, columns are partially validated
/// (only objects present in types.lock).
fn validate_constraints_with_types(
    planned_project: &project::planned::Project,
    types: &crate::types::Types,
) -> Result<(), CliError> {
    let fk_errors = project::typed::validate_constraint_fk_targets(planned_project, types);
    if !fk_errors.is_empty() {
        return Err(crate::project::error::ProjectError::from(
            crate::project::error::ValidationErrors::new(fk_errors),
        )
        .into());
    }

    let column_map = build_column_map(types);
    let col_errors = project::typed::validate_constraint_columns(planned_project, &column_map);
    if !col_errors.is_empty() {
        return Err(crate::project::error::ProjectError::from(
            crate::project::error::ValidationErrors::new(col_errors),
        )
        .into());
    }

    Ok(())
}

/// Build a column map from a Types object for constraint column validation.
fn build_column_map(
    types: &crate::types::Types,
) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
    types
        .tables
        .iter()
        .map(|(fqn, columns)| {
            let col_names = columns.keys().map(|c| c.to_lowercase()).collect();
            (fqn.to_lowercase(), col_names)
        })
        .collect()
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
