//! Create tables command - create tables that don't exist in the database.

use crate::cli::{CliError, TypeCheckMode, helpers};
use crate::client::{Client, Profile};
use crate::project::ast::Statement;
use crate::utils::git;
use crate::{project, verbose};
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Create tables that don't exist in the database.
///
/// This command:
/// - Queries the database to find which tables already exist
/// - Creates only tables that don't exist (no IF NOT EXISTS needed)
/// - Creates schemas if they don't exist
/// - Deploys only CREATE TABLE and CREATE TABLE FROM SOURCE statements
/// - Deploys associated indexes, grants, and comments
/// - Tracks deployment under a deploy ID
/// - Only records tables that were actually created
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project root directory
/// * `deploy_id` - Optional deploy ID (defaults to random 7-char hex)
/// * `allow_dirty` - Allow deploying with uncommitted changes
/// * `dry_run` - If true, print SQL instead of executing
///
/// # Returns
/// Ok(()) if deployment succeeds
///
/// # Errors
/// Returns various `CliError` variants for different failure modes
pub async fn run(
    profile: &Profile,
    directory: &Path,
    deploy_id: Option<&str>,
    allow_dirty: bool,
    dry_run: bool,
) -> Result<(), CliError> {
    // Check for uncommitted changes before proceeding
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    // Determine deploy ID (use provided name or random 7-char hex)
    let deploy_id = match deploy_id {
        Some(name) => name.to_string(),
        None => helpers::generate_random_env_name(),
    };

    if dry_run {
        println!("-- DRY RUN: The following SQL would be executed --\n");
    } else {
        println!("Creating tables in deployment: {}", deploy_id);
    }

    // Compile the project first (skip type checking since we're deploying)
    let planned_project = super::compile::run(directory, TypeCheckMode::Disabled).await?;

    // Connect to the database
    let mut client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    (client).validate_privileges(&planned_project).await?;
    client.validate_cluster_isolation(&planned_project).await?;
    client.validate_sources_exist(&planned_project).await?;
    verbose!("Validation successful");

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Validate deployment doesn't already exist
    let existing_metadata = client.get_deployment_metadata(&deploy_id).await?;
    if existing_metadata.is_some() {
        return Err(CliError::InvalidEnvironmentName {
            name: format!("deployment '{}' already exists", deploy_id),
        });
    }

    // Partition objects into tables and sources (they use different catalog tables)
    let mut table_object_ids: BTreeSet<project::object_id::ObjectId> = BTreeSet::new();
    let mut source_object_ids: BTreeSet<project::object_id::ObjectId> = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        match &obj.typed_object.stmt {
            Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => {
                table_object_ids.insert(obj.id.clone());
            }
            Statement::CreateSource(_) => {
                source_object_ids.insert(obj.id.clone());
            }
            _ => {}
        }
    }

    let all_object_ids: BTreeSet<_> = table_object_ids
        .union(&source_object_ids)
        .cloned()
        .collect();

    if all_object_ids.is_empty() {
        println!("No tables found in project");
        return Ok(());
    }

    // Get sorted objects (respecting dependencies)
    let table_objects = planned_project.get_sorted_objects_filtered(&all_object_ids)?;

    println!("Found {} table(s) in project", table_objects.len());

    // Query which tables and sources already exist (separate catalog tables)
    let existing_tables = client.check_tables_exist(&table_object_ids).await?;
    let existing_sources = client.check_sources_exist(&source_object_ids).await?;

    // Merge existing tables and sources
    let existing_objects: BTreeSet<_> = existing_tables
        .union(&existing_sources)
        .cloned()
        .collect();

    // Filter to only objects that don't exist
    let tables_to_create: Vec<_> = table_objects
        .into_iter()
        .filter(|(obj_id, _)| !existing_objects.contains(obj_id))
        .collect();

    // Show what's being skipped
    if !existing_objects.is_empty() {
        println!("\nObjects that already exist (skipping):");
        let mut existing_list: Vec<_> = existing_objects.iter().collect();
        existing_list.sort_by_key(|obj| (&obj.database, &obj.schema, &obj.object));
        for table_id in existing_list {
            println!(
                "  - {}.{}.{}",
                table_id.database, table_id.schema, table_id.object
            );
        }
    }

    // If all tables exist, exit early
    if tables_to_create.is_empty() {
        println!(
            "\nAll {} table(s) already exist. Nothing to create.",
            table_object_ids.len()
        );
        return Ok(());
    }

    println!("\nCreating {} new table(s)...", tables_to_create.len());

    // Collect all schemas that contain tables to create
    let mut table_schemas = BTreeMap::new();
    for (object_id, _) in &tables_to_create {
        table_schemas.insert(
            (object_id.database.clone(), object_id.schema.clone()),
            crate::client::DeploymentKind::Tables,
        );
    }

    // Create executor with dry-run mode
    let executor = helpers::DeploymentExecutor::with_dry_run(&client, dry_run);

    // Create schemas and execute their mod statements
    if !table_schemas.is_empty() {
        if !dry_run {
            println!("Preparing schemas...");
        } else {
            println!("-- Create schemas --");
        }

        for ((database, schema), _kind) in &table_schemas {
            verbose!("Creating schema {}.{} if not exists", database, schema);
            let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {}.{}", database, schema);
            executor.execute_sql(&create_schema_sql).await?;
        }

        // Execute schema mod statements for schemas that contain tables
        for mod_stmt in planned_project.iter_mod_statements() {
            match mod_stmt {
                project::ModStatement::Database {
                    database,
                    statement,
                } => {
                    // Check if any schema in this database contains tables
                    let has_tables = table_schemas.keys().any(|(db, _)| db == database);
                    if has_tables {
                        verbose!("Applying database setup for: {}", database);
                        executor.execute_sql(statement).await?;
                    }
                }
                project::ModStatement::Schema {
                    database,
                    schema,
                    statement,
                } => {
                    if table_schemas.contains_key(&(database.to_string(), schema.to_string())) {
                        verbose!("Applying schema setup for: {}.{}", database, schema);
                        executor.execute_sql(statement).await?;
                    }
                }
            }
        }
    }

    if dry_run {
        println!("-- Create tables --");
    }

    // Execute table statements (only for tables that don't exist)
    let mut success_count = 0;

    for (idx, (object_id, typed_obj)) in tables_to_create.iter().enumerate() {
        verbose!(
            "Creating {}/{}: {}",
            idx + 1,
            tables_to_create.len(),
            object_id
        );

        // Execute the table statement along with indexes, grants, and comments
        executor.execute_object(typed_obj).await?;

        if !dry_run {
            println!(
                "  ✓ {}.{}.{}",
                object_id.database, object_id.schema, object_id.object
            );
        }
        success_count += 1;
    }

    // Skip deployment tracking in dry-run mode
    if !dry_run {
        // Build snapshot for deployment tracking - only include tables that were created
        let mut snapshot_objects = BTreeMap::new();
        for (object_id, typed_obj) in &tables_to_create {
            let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
            snapshot_objects.insert(object_id.clone(), hash);
        }

        let new_snapshot = project::deployment_snapshot::DeploymentSnapshot {
            objects: snapshot_objects,
            schemas: table_schemas.clone(), // Already contains only schemas with tables
        };

        // Collect deployment metadata
        let metadata = helpers::collect_deployment_metadata(&client, directory).await;

        // Write deployment state to database (promoted deployment)
        let now = Utc::now();
        project::deployment_snapshot::write_to_database(
            &client,
            &new_snapshot,
            &deploy_id,
            &metadata,
            Some(now),
        )
        .await?;

        println!("\n✓ Successfully created {} new table(s)", success_count);
        if !existing_objects.is_empty() {
            println!(
                "  Skipped {} object(s) that already existed",
                existing_objects.len()
            );
        }
    } else {
        println!("-- End of dry run ({} statement(s)) --", success_count);
    }

    Ok(())
}
