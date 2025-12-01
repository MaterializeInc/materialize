//! Apply command - deploy SQL to database.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::utils::git;
use crate::{project, verbose};
use std::path::Path;

/// Apply the project to the database (direct deployment or blue/green swap).
///
/// This command has two modes:
///
/// 1. **Blue/green deployment** (if staging_env is provided):
///    - Promotes a staging environment to production using ALTER SWAP
///    - Delegates to the swap command
///
/// 2. **Direct deployment** (default):
///    - Computes incremental changes from last deployment
///    - Safety check: prevents dropping production objects without explicit flag
///    - Executes SQL in topological order
///    - Updates deployment tracking records
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project root directory
/// * `in_place_dangerous_will_cause_downtime` - Allow dropping/recreating production objects
/// * `force` - Force promotion despite conflicts (for blue/green mode)
/// * `allow_dirty` - Allow deploying with uncommitted changes
/// * `staging_env` - Optional staging environment name (enables blue/green mode)
///
/// # Returns
/// Ok(()) if deployment succeeds
///
/// # Errors
/// Returns various `CliError` variants for different failure modes
pub async fn run(
    profile: &Profile,
    directory: &Path,
    in_place_dangerous_will_cause_downtime: bool,
    allow_dirty: bool,
) -> Result<(), CliError> {
    // Check for uncommitted changes before proceeding
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    // Compile the project first (skip type checking since we're deploying)
    let compile_args = super::compile::CompileArgs {
        typecheck: false, // Skip type checking for apply
        docker_image: None,
    };
    let planned_project = super::compile::run(directory, compile_args).await?;

    println!("Applying SQL to database");

    // Connect to the database
    let client = helpers::connect_to_database(profile).await?;

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Build new snapshot from current planned project
    let new_snapshot = project::deployment_snapshot::build_snapshot_from_planned(&planned_project)?;

    // Load previous deployment state from database (None = production)
    let old_snapshot = project::deployment_snapshot::load_from_database(&client, None).await?;

    // Compute changeset
    let change_set = if old_snapshot.objects.is_empty() {
        // First deployment
        None
    } else {
        let cs = project::changeset::ChangeSet::from_deployment_snapshot_comparison(
            &old_snapshot,
            &new_snapshot,
            &planned_project,
        );

        // If no changes detected, exit early
        if cs.is_empty() {
            println!("No changes detected, skipping deployment");
            return Ok(());
        }

        Some(cs)
    };

    // Safety check: Validate deployment won't override existing production objects
    if let Some(ref cs) = change_set {
        if !cs.objects_to_deploy.is_empty() && !in_place_dangerous_will_cause_downtime {
            // Check which objects exist in production
            let existing_objects = client.check_objects_exist(&cs.objects_to_deploy).await?;

            if !existing_objects.is_empty() {
                return Err(CliError::ProductionOverwriteNotAllowed {
                    objects: existing_objects
                        .iter()
                        .map(|fqn| {
                            let parts: Vec<&str> = fqn.split('.').collect();
                            if parts.len() == 3 {
                                (
                                    parts[0].to_string(),
                                    parts[1].to_string(),
                                    parts[2].to_string(),
                                )
                            } else {
                                ("unknown".to_string(), "unknown".to_string(), fqn.clone())
                            }
                        })
                        .collect(),
                });
            }
        }
    }

    // If in-place deployment is requested and there are changes, drop existing objects first
    if in_place_dangerous_will_cause_downtime {
        if let Some(ref cs) = change_set {
            if !cs.objects_to_deploy.is_empty() {
                println!(
                    "WARNING: Dropping {} objects for in-place redeployment",
                    cs.objects_to_deploy.len()
                );
                println!("This will cause downtime!");

                let dropped = client.drop_objects(&cs.objects_to_deploy).await?;
                println!("Dropped {} objects:", dropped.len());
                for fqn in &dropped {
                    println!("  - {}", fqn);
                }
                println!();
            }
        }
    }

    // Determine which module statements to execute based on dirty schemas
    let mod_stmts = planned_project.iter_mod_statements();
    let mut filtered_mod_stmts = Vec::new();

    if let Some(ref cs) = change_set {
        // Only execute module statements for dirty schemas
        for mod_stmt in mod_stmts {
            match mod_stmt {
                project::ModStatement::Database { database, .. } => {
                    // Check if any schema in this database is dirty
                    let has_dirty_schema = cs.dirty_schemas.iter().any(|(db, _)| db == database);
                    if has_dirty_schema {
                        filtered_mod_stmts.push(mod_stmt);
                    }
                }
                project::ModStatement::Schema {
                    database, schema, ..
                } => {
                    if cs
                        .dirty_schemas
                        .contains(&(database.to_string(), schema.to_string()))
                    {
                        filtered_mod_stmts.push(mod_stmt);
                    }
                }
            }
        }
    } else {
        // No changeset (first deploy), execute all module statements
        filtered_mod_stmts.extend(mod_stmts);
    }

    // Execute module setup statements
    if !filtered_mod_stmts.is_empty() {
        println!("Applying module setup statements...");

        for mod_stmt in filtered_mod_stmts {
            match mod_stmt {
                project::ModStatement::Database {
                    database,
                    statement,
                } => {
                    verbose!("Applying database setup for: {}", database);
                    client
                        .execute(&statement.to_string(), &[])
                        .await
                        .map_err(|e| CliError::SqlExecutionFailed {
                            statement: statement.to_string(),
                            source: e,
                        })?;
                }
                project::ModStatement::Schema {
                    database,
                    schema,
                    statement,
                } => {
                    verbose!("Applying schema setup for: {}.{}", database, schema);
                    client
                        .execute(&statement.to_string(), &[])
                        .await
                        .map_err(|e| CliError::SqlExecutionFailed {
                            statement: statement.to_string(),
                            source: e,
                        })?;
                }
            }
        }
    }

    // Get objects to deploy (either all or filtered by changeset)
    let objects = if let Some(ref cs) = change_set {
        println!(
            "Incremental deployment: {} objects need redeployment",
            cs.deployment_count()
        );
        if !cs.dirty_schemas.is_empty() {
            verbose!("Dirty schemas:");
            for (db, schema) in &cs.dirty_schemas {
                verbose!("  - {}.{}", db, schema);
            }
        }
        if !cs.dirty_clusters.is_empty() {
            verbose!("Dirty clusters:");
            for cluster in &cs.dirty_clusters {
                verbose!("  - {}", cluster.name);
            }
        }

        planned_project.get_sorted_objects_filtered(&cs.objects_to_deploy)?
    } else {
        planned_project.get_sorted_objects()?
    };

    // Execute SQL for each object in topological order
    let executor = helpers::DeploymentExecutor::new(&client);
    let mut success_count = 0;
    for (idx, (object_id, typed_obj)) in objects.iter().enumerate() {
        verbose!("Applying {}/{}: {}", idx + 1, objects.len(), object_id);

        executor.execute_object(typed_obj).await?;
        success_count += 1;
    }

    // Collect deployment metadata
    let metadata = helpers::collect_deployment_metadata(&client, directory).await;

    // Write deployment state to database (environment="<init>" for direct deploy, promoted_at=now)
    let now = std::time::SystemTime::now();
    project::deployment_snapshot::write_to_database(
        &client,
        &new_snapshot,
        "<init>",
        &metadata,
        Some(now),
    )
    .await?;

    println!(
        "\nSuccessfully applied {} objects to the database",
        success_count
    );

    Ok(())
}
