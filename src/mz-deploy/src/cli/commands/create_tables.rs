//! Create tables command - create tables that don't exist in the database.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::project::ast::Statement;
use crate::utils::git;
use crate::{project, verbose};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

/// Create tables that don't exist in the database.
///
/// This command:
/// - Queries the database to find which tables already exist
/// - Creates only tables that don't exist (no IF NOT EXISTS needed)
/// - Creates schemas if they don't exist
/// - Deploys only CREATE TABLE and CREATE TABLE FROM SOURCE statements
/// - Deploys associated indexes, grants, and comments
/// - Tracks deployment under an environment name
/// - Only records tables that were actually created
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project root directory
/// * `environment_name` - Optional environment name (defaults to first 7 chars of git SHA)
/// * `allow_dirty` - Allow deploying with uncommitted changes
///
/// # Returns
/// Ok(()) if deployment succeeds
///
/// # Errors
/// Returns various `CliError` variants for different failure modes
pub async fn run(
    profile: &Profile,
    directory: &Path,
    environment_name: Option<&str>,
    allow_dirty: bool,
) -> Result<(), CliError> {
    // Check for uncommitted changes before proceeding
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    // Determine environment name (use provided name or random 7-char hex)
    let environment_name = match environment_name {
        Some(name) => name.to_string(),
        None => helpers::generate_random_env_name(),
    };

    println!("Creating tables in environment: {}", environment_name);

    // Compile the project first (skip type checking since we're deploying)
    let compile_args = super::compile::CompileArgs {
        typecheck: false, // Skip type checking for create-tables
        docker_image: None,
    };
    let planned_project = super::compile::run(directory, compile_args).await?;

    // Connect to the database
    let mut client = helpers::connect_to_database(profile).await?;

    (client).validate_privileges(&planned_project).await?;
    client.validate_cluster_isolation(&planned_project).await?;
    client.validate_sources_exist(&planned_project).await?;
    verbose!("Validation successful");

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Validate deployment doesn't already exist
    let existing_metadata = client.get_deployment_metadata(&environment_name).await?;
    if existing_metadata.is_some() {
        return Err(CliError::InvalidEnvironmentName {
            name: format!("deployment '{}' already exists", environment_name),
        });
    }

    // Filter to only table objects (CreateTable and CreateTableFromSource)
    let table_object_ids: HashSet<project::object_id::ObjectId> = planned_project
        .iter_objects()
        .filter(|obj| matches!(
            obj.typed_object.stmt,
            Statement::CreateTable(_) | Statement::CreateTableFromSource(_)
        ))
        .map(|obj| obj.id.clone())
        .collect();

    if table_object_ids.is_empty() {
        println!("No tables found in project");
        return Ok(());
    }

    // Get sorted table objects (respecting dependencies)
    let table_objects = planned_project.get_sorted_objects_filtered(&table_object_ids)?;

    println!("Found {} table(s) in project", table_objects.len());

    // Query which tables already exist
    let existing_tables = client.check_tables_exist(&table_object_ids).await?;

    // Filter to only tables that don't exist
    let tables_to_create: Vec<_> = table_objects
        .into_iter()
        .filter(|(obj_id, _)| !existing_tables.contains(obj_id))
        .collect();

    // Show what's being skipped
    if !existing_tables.is_empty() {
        println!("\nTables that already exist (skipping):");
        let mut existing_list: Vec<_> = existing_tables.iter().collect();
        existing_list.sort_by_key(|obj| (&obj.database, &obj.schema, &obj.object));
        for table_id in existing_list {
            println!("  - {}.{}.{}", table_id.database, table_id.schema, table_id.object);
        }
    }

    // If all tables exist, exit early
    if tables_to_create.is_empty() {
        println!("\nAll {} table(s) already exist. Nothing to create.", table_object_ids.len());
        return Ok(());
    }

    println!("\nCreating {} new table(s)...", tables_to_create.len());

    // Collect all schemas that contain tables to create
    let mut table_schemas = HashSet::new();
    for (object_id, _) in &tables_to_create {
        table_schemas.insert((object_id.database.clone(), object_id.schema.clone()));
    }

    // Create schemas and execute their mod statements
    if !table_schemas.is_empty() {
        println!("Preparing schemas...");

        for (database, schema) in &table_schemas {
            verbose!("Creating schema {}.{} if not exists", database, schema);
            client.create_schema(database, schema).await?;
        }

        // Execute schema mod statements for schemas that contain tables
        for mod_stmt in planned_project.iter_mod_statements() {
            match mod_stmt {
                project::ModStatement::Database { database, statement } => {
                    // Check if any schema in this database contains tables
                    let has_tables = table_schemas.iter().any(|(db, _)| db == database);
                    if has_tables {
                        verbose!("Applying database setup for: {}", database);
                        client
                            .execute(&statement.to_string(), &[])
                            .await
                            .map_err(|e| CliError::SqlExecutionFailed {
                                statement: statement.to_string(),
                                source: e,
                            })?;
                    }
                }
                project::ModStatement::Schema {
                    database,
                    schema,
                    statement,
                } => {
                    if table_schemas.contains(&(database.to_string(), schema.to_string())) {
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
    }

    // Execute table statements (only for tables that don't exist)
    let executor = helpers::DeploymentExecutor::new(&client);
    let mut success_count = 0;

    for (idx, (object_id, typed_obj)) in tables_to_create.iter().enumerate() {
        verbose!("Creating {}/{}: {}", idx + 1, tables_to_create.len(), object_id);

        // Execute the table statement along with indexes, grants, and comments
        executor.execute_object(typed_obj).await?;

        println!("  ✓ {}.{}.{}", object_id.database, object_id.schema, object_id.object);
        success_count += 1;
    }

    // Build snapshot for deployment tracking - only include tables that were created
    let mut snapshot_objects = BTreeMap::new();
    for (object_id, typed_obj) in &tables_to_create {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        snapshot_objects.insert(object_id.clone(), hash);
    }

    let new_snapshot = project::deployment_snapshot::DeploymentSnapshot {
        objects: snapshot_objects,
        schemas: table_schemas.clone(),  // Already contains only schemas with tables
    };

    // Collect deployment metadata
    let metadata = helpers::collect_deployment_metadata(&client, directory).await;

    // Write deployment state to database (promoted deployment)
    let now = std::time::SystemTime::now();
    project::deployment_snapshot::write_to_database(
        &client,
        &new_snapshot,
        &environment_name,
        &metadata,
        Some(now),
    )
    .await?;

    println!("\n✓ Successfully created {} new table(s)", success_count);
    if !existing_tables.is_empty() {
        println!("  Skipped {} table(s) that already existed", existing_tables.len());
    }

    Ok(())
}
