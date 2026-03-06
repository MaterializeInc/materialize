//! Create tables command - create tables that don't exist in the database.

use crate::cli::git;
use crate::cli::{CliError, TypeCheckMode, executor};
use crate::client::{Client, Profile};
use crate::config::ProjectSettings;
use crate::project::ast::Statement;
use crate::secret_resolver::SecretResolver;
use crate::{project, verbose};

use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::ObjectRef;

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
    settings: &ProjectSettings,
    deploy_id: Option<&str>,
    allow_dirty: bool,
    dry_run: bool,
) -> Result<(), CliError> {
    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    let deploy_id = deploy_id
        .map(ToString::to_string)
        .unwrap_or_else(executor::generate_random_env_name);

    if dry_run {
        println!("-- DRY RUN: The following SQL would be executed --\n");
    } else {
        println!("Creating tables in deployment: {}", deploy_id);
    }

    let planned_project = super::compile::run(directory, TypeCheckMode::Disabled).await?;
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    (client)
        .validation()
        .validate_privileges(&planned_project)
        .await?;
    client
        .validation()
        .validate_cluster_isolation(&planned_project)
        .await?;
    client
        .validation()
        .validate_sources_exist(&planned_project)
        .await?;
    verbose!("Validation successful");

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Validate deployment doesn't already exist
    let existing_metadata = client
        .deployments()
        .get_deployment_metadata(&deploy_id)
        .await?;
    if existing_metadata.is_some() {
        return Err(CliError::InvalidEnvironmentName {
            name: format!("deployment '{}' already exists", deploy_id),
        });
    }

    let (table_object_ids, source_object_ids, secret_object_ids, connection_object_ids) =
        collect_table_source_secret_and_connection_ids(&planned_project);
    let all_object_ids: BTreeSet<_> = table_object_ids
        .iter()
        .chain(source_object_ids.iter())
        .chain(secret_object_ids.iter())
        .chain(connection_object_ids.iter())
        .cloned()
        .collect();
    if all_object_ids.is_empty() {
        println!("No tables found in project");
        return Ok(());
    }

    let table_objects = planned_project.get_sorted_objects_filtered(&all_object_ids)?;
    println!("Found {} table(s) in project", table_objects.len());

    let introspection = client.introspection();
    let (existing_tables, existing_sources, existing_secrets, existing_connections) = tokio::try_join!(
        introspection.check_tables_exist(&table_object_ids),
        introspection.check_sources_exist(&source_object_ids),
        introspection.check_secrets_exist(&secret_object_ids),
        introspection.check_connections_exist(&connection_object_ids),
    )?;
    let existing_objects: BTreeSet<_> = existing_tables
        .iter()
        .chain(existing_sources.iter())
        .chain(existing_secrets.iter())
        .chain(existing_connections.iter())
        .cloned()
        .collect();
    let tables_to_create: Vec<_> = table_objects
        .into_iter()
        .filter(|(obj_id, _)| !existing_objects.contains(obj_id))
        .collect();

    print_existing_objects(&existing_objects);

    if tables_to_create.is_empty() {
        println!(
            "\nAll {} table(s) already exist. Nothing to create.",
            table_object_ids.len()
        );
        return Ok(());
    }

    println!("\nCreating {} new table(s)...", tables_to_create.len());

    let executor = executor::DeploymentExecutor::with_dry_run(&client, dry_run);
    let table_schemas = collect_table_schemas(&tables_to_create);
    prepare_schemas_and_mod_statements(&executor, &planned_project, &table_schemas, dry_run)
        .await?;
    let resolver = SecretResolver::new(&settings.secret_config);
    let success_count =
        execute_table_creates(&executor, &resolver, &tables_to_create, dry_run).await?;
    finalize_table_deployment(
        &client,
        directory,
        &deploy_id,
        &tables_to_create,
        &table_schemas,
        &existing_objects,
        success_count,
        dry_run,
    )
    .await?;

    Ok(())
}

/// Partitions planned objects into catalog categories used by existence checks.
///
/// Tables, sources, secrets, and connections are checked in different system catalogs,
/// so this split is the input contract for "what already exists" filtering.
fn collect_table_source_secret_and_connection_ids(
    planned_project: &project::planned::Project,
) -> (
    BTreeSet<project::object_id::ObjectId>,
    BTreeSet<project::object_id::ObjectId>,
    BTreeSet<project::object_id::ObjectId>,
    BTreeSet<project::object_id::ObjectId>,
) {
    let mut table_object_ids = BTreeSet::new();
    let mut source_object_ids = BTreeSet::new();
    let mut secret_object_ids = BTreeSet::new();
    let mut connection_object_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        match &obj.typed_object.stmt {
            Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => {
                table_object_ids.insert(obj.id.clone());
            }
            Statement::CreateSource(_) => {
                source_object_ids.insert(obj.id.clone());
            }
            Statement::CreateSecret(_) => {
                secret_object_ids.insert(obj.id.clone());
            }
            Statement::CreateConnection(_) => {
                connection_object_ids.insert(obj.id.clone());
            }
            _ => {}
        }
    }
    (
        table_object_ids,
        source_object_ids,
        secret_object_ids,
        connection_object_ids,
    )
}

/// Emits the skip list shown to users before create execution begins.
///
/// The list is sorted for deterministic output so repeated runs are easy to compare.
fn print_existing_objects(existing_objects: &BTreeSet<project::object_id::ObjectId>) {
    if existing_objects.is_empty() {
        return;
    }
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

/// Builds the schema deployment map for objects that will actually be created.
///
/// This map is reused both for schema preparation and for persisted deployment metadata.
fn collect_table_schemas(
    tables_to_create: &[ObjectRef<'_>],
) -> BTreeMap<project::SchemaQualifier, crate::client::DeploymentKind> {
    let mut table_schemas = BTreeMap::new();
    for (object_id, _) in tables_to_create {
        table_schemas.insert(
            project::SchemaQualifier::new(object_id.database.clone(), object_id.schema.clone()),
            crate::client::DeploymentKind::Tables,
        );
    }
    table_schemas
}

/// Prepares the target environment for table creation.
///
/// Ensures schemas exist and replays relevant database/schema module setup SQL
/// only for the databases/schemas that contain pending table objects.
pub async fn prepare_schemas_and_mod_statements(
    executor: &executor::DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    table_schemas: &BTreeMap<project::SchemaQualifier, crate::client::DeploymentKind>,
    dry_run: bool,
) -> Result<(), CliError> {
    if table_schemas.is_empty() {
        return Ok(());
    }
    if !dry_run {
        println!("Preparing databases and schemas...");
    } else {
        println!("-- Create databases and schemas --");
    }

    // Create databases first (schemas can't exist without their database)
    let databases: BTreeSet<&str> = table_schemas
        .keys()
        .map(|sq| sq.database.as_str())
        .collect();
    for db in databases {
        let create_db_sql = format!("CREATE DATABASE IF NOT EXISTS {}", db);
        executor.execute_sql(&create_db_sql).await?;
    }

    for sq in table_schemas.keys() {
        verbose!(
            "Creating schema {}.{} if not exists",
            sq.database,
            sq.schema
        );
        let create_schema_sql =
            format!("CREATE SCHEMA IF NOT EXISTS {}.{}", sq.database, sq.schema);
        executor.execute_sql(&create_schema_sql).await?;
    }

    for mod_stmt in planned_project.iter_mod_statements() {
        match mod_stmt {
            project::ModStatement::Database {
                database,
                statement,
            } => {
                let has_tables = table_schemas.keys().any(|sq| sq.database == *database);
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
                if table_schemas.contains_key(&project::SchemaQualifier::new(
                    database.to_string(),
                    schema.to_string(),
                )) {
                    verbose!("Applying schema setup for: {}.{}", database, schema);
                    executor.execute_sql(statement).await?;
                }
            }
        }
    }
    Ok(())
}

/// Returns a sort key for object type ordering: secrets, connections, sources, then tables.
fn object_type_order(stmt: &Statement) -> u8 {
    match stmt {
        Statement::CreateSecret(_) => 0,
        Statement::CreateConnection(_) => 1,
        Statement::CreateSource(_) => 2,
        _ => 3,
    }
}

/// Executes missing table/source/secret objects in type order (secrets, sources, tables).
///
/// Returns the count of successfully executed objects for summary and metadata reporting.
async fn execute_table_creates(
    executor: &executor::DeploymentExecutor<'_>,
    resolver: &SecretResolver,
    tables_to_create: &[ObjectRef<'_>],
    dry_run: bool,
) -> Result<usize, CliError> {
    if dry_run {
        println!("-- Create tables --");
    }

    // Sort: secrets first, then sources, then tables
    let mut sorted: Vec<_> = tables_to_create.to_vec();
    sorted.sort_by_key(|(_, typed_obj)| object_type_order(&typed_obj.stmt));

    let mut success_count = 0;
    for (idx, (object_id, typed_obj)) in sorted.iter().enumerate() {
        verbose!("Creating {}/{}: {}", idx + 1, sorted.len(), object_id);

        // Resolve client-side secret providers, then execute
        let resolved_stmt = resolver.resolve_statement_for_cli(&typed_obj.stmt).await?;
        executor.execute_sql(&resolved_stmt).await?;
        for index in &typed_obj.indexes {
            executor.execute_sql(index).await?;
        }
        for grant in &typed_obj.grants {
            executor.execute_sql(grant).await?;
        }
        for comment in &typed_obj.comments {
            executor.execute_sql(comment).await?;
        }

        if !dry_run {
            println!(
                "  ✓ {}.{}.{}",
                object_id.database, object_id.schema, object_id.object
            );
        }
        success_count += 1;
    }
    Ok(success_count)
}

#[allow(clippy::too_many_arguments)]
/// Finalizes create-tables with either dry-run output or persisted deployment state.
///
/// In non-dry-run mode this writes a promoted deployment snapshot containing exactly
/// the objects created in this invocation so later diffing and conflict checks stay accurate.
async fn finalize_table_deployment(
    client: &Client,
    directory: &Path,
    deploy_id: &str,
    tables_to_create: &[ObjectRef<'_>],
    table_schemas: &BTreeMap<project::SchemaQualifier, crate::client::DeploymentKind>,
    existing_objects: &BTreeSet<project::object_id::ObjectId>,
    success_count: usize,
    dry_run: bool,
) -> Result<(), CliError> {
    if dry_run {
        println!("-- End of dry run ({} statement(s)) --", success_count);
        return Ok(());
    }

    let mut snapshot_objects = BTreeMap::new();
    for (object_id, typed_obj) in tables_to_create {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        snapshot_objects.insert(object_id.clone(), hash);
    }

    let new_snapshot = project::deployment_snapshot::DeploymentSnapshot {
        objects: snapshot_objects,
        schemas: table_schemas.clone(),
    };
    let metadata = executor::collect_deployment_metadata(client, directory).await;
    project::deployment_snapshot::write_to_database(
        client,
        &new_snapshot,
        deploy_id,
        &metadata,
        Some(Utc::now()),
    )
    .await?;

    println!("\n✓ Successfully created {} new table(s)", success_count);
    if !existing_objects.is_empty() {
        println!(
            "  Skipped {} object(s) that already existed",
            existing_objects.len()
        );
    }
    Ok(())
}
