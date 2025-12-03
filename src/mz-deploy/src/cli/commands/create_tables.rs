//! Create tables command - safely deploy table definitions with IF NOT EXISTS.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::project::ast::Statement;
use crate::utils::git;
use crate::{project, verbose};
use std::collections::HashSet;
use std::path::Path;

/// Create all tables in the project with IF NOT EXISTS flag.
///
/// This command:
/// - Validates project structure and permissions
/// - Creates schemas if they don't exist
/// - Deploys only CREATE TABLE and CREATE TABLE FROM SOURCE statements
/// - Sets IF NOT EXISTS on all table statements for safe re-runs
/// - Deploys associated indexes, grants, and comments
/// - Tracks deployment state
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project root directory
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
    allow_dirty: bool,
) -> Result<(), CliError> {
    // Check for uncommitted changes before proceeding
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    // Compile the project first (skip type checking since we're deploying)
    let compile_args = super::compile::CompileArgs {
        typecheck: false, // Skip type checking for create-tables
        docker_image: None,
    };
    let planned_project = super::compile::run(directory, compile_args).await?;

    println!("Creating tables in database");

    // Connect to the database
    let mut client = helpers::connect_to_database(profile).await?;

    (client).validate_privileges(&planned_project).await?;
    client.validate_cluster_isolation(&planned_project).await?;
    client.validate_sources_exist(&planned_project).await?;
    verbose!("Validation successful");

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

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

    println!("Found {} table(s) to create", table_objects.len());

    // Collect all schemas that contain tables
    let mut table_schemas = HashSet::new();
    for (object_id, _) in &table_objects {
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

    // Execute table statements with IF NOT EXISTS
    let executor = helpers::DeploymentExecutor::new(&client);
    let mut success_count = 0;

    for (idx, (object_id, typed_obj)) in table_objects.iter().enumerate() {
        verbose!("Creating {}/{}: {}", idx + 1, table_objects.len(), object_id);

        // Create a modified statement with if_not_exists = true
        let modified_stmt = match &typed_obj.stmt {
            Statement::CreateTable(s) => {
                let mut modified = s.clone();
                modified.if_not_exists = true;
                Statement::CreateTable(modified)
            }
            Statement::CreateTableFromSource(s) => {
                let mut modified = s.clone();
                modified.if_not_exists = true;
                Statement::CreateTableFromSource(modified)
            }
            _ => {
                // Should never happen due to our filter, but be defensive
                continue;
            }
        };

        // Create a temporary DatabaseObject with the modified statement
        let modified_obj = project::typed::DatabaseObject {
            stmt: modified_stmt,
            indexes: typed_obj.indexes.clone(),
            grants: typed_obj.grants.clone(),
            comments: typed_obj.comments.clone(),
            tests: Vec::new(), // Don't run tests during create-tables
        };

        // Execute the table statement along with indexes, grants, and comments
        executor.execute_object(&modified_obj).await?;
        success_count += 1;
    }

    // Build snapshot for deployment tracking
    let new_snapshot = project::deployment_snapshot::build_snapshot_from_planned(&planned_project)?;

    // Collect deployment metadata
    let metadata = helpers::collect_deployment_metadata(&client, directory).await;

    // Write deployment state to database (environment="<init>", promoted_at=now)
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
        "\nSuccessfully created {} table(s) in the database",
        success_count
    );

    Ok(())
}
