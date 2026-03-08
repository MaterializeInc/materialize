//! Apply tables command - create tables that don't exist in the database.

use crate::cli::commands::grants;
use crate::cli::git;
use crate::cli::progress;
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

    progress::info(&format!("Creating tables in deployment: {}", deploy_id));

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
        progress::info("No tables found in project");
        return Ok(());
    }

    let table_objects = planned_project.get_sorted_objects_filtered(&all_object_ids)?;
    progress::info(&format!(
        "Found {} table(s) in project",
        table_objects.len()
    ));

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
        progress::info(&format!(
            "All {} table(s) already exist. Nothing to create.",
            table_object_ids.len()
        ));
        return Ok(());
    }

    progress::info(&format!(
        "Creating {} new table(s)...",
        tables_to_create.len()
    ));

    let executor = executor::DeploymentExecutor::with_dry_run(&client, dry_run);
    let table_schemas = collect_table_schemas(&tables_to_create);
    let schema_keys: BTreeSet<_> = table_schemas.keys().cloned().collect();
    progress::info("Preparing databases and schemas...");
    executor
        .prepare_databases_and_schemas(&planned_project, &schema_keys, None)
        .await?;
    let resolver = SecretResolver::new(&settings.secret_config);
    let success_count = execute_table_creates(&executor, &resolver, &tables_to_create).await?;
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

/// Apply only table objects (no deployment tracking).
///
/// Creates tables that don't exist in the database. Existing tables are skipped.
pub async fn apply_tables(
    directory: &Path,
    profile: &Profile,
    settings: &ProjectSettings,
    dry_run: bool,
) -> Result<(), CliError> {
    apply_by_kind(
        directory,
        profile,
        settings,
        dry_run,
        ObjectKindFilter::Tables,
    )
    .await
}

/// Apply only source objects (no deployment tracking).
///
/// Creates sources that don't exist in the database. Existing sources are skipped.
pub async fn apply_sources(
    directory: &Path,
    profile: &Profile,
    settings: &ProjectSettings,
    dry_run: bool,
) -> Result<(), CliError> {
    apply_by_kind(
        directory,
        profile,
        settings,
        dry_run,
        ObjectKindFilter::Sources,
    )
    .await
}

/// Which object kinds to create.
enum ObjectKindFilter {
    Tables,
    Sources,
}

/// Shared implementation for `apply_tables` and `apply_sources`.
async fn apply_by_kind(
    directory: &Path,
    profile: &Profile,
    settings: &ProjectSettings,
    dry_run: bool,
    filter: ObjectKindFilter,
) -> Result<(), CliError> {
    let (label, matcher): (&str, Box<dyn Fn(&Statement) -> bool>) = match filter {
        ObjectKindFilter::Tables => (
            "table",
            Box::new(|stmt| {
                matches!(
                    stmt,
                    Statement::CreateTable(_) | Statement::CreateTableFromSource(_)
                )
            }),
        ),
        ObjectKindFilter::Sources => (
            "source",
            Box::new(|stmt| matches!(stmt, Statement::CreateSource(_))),
        ),
    };

    let planned_project = super::compile::run(directory, TypeCheckMode::Disabled).await?;
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Collect object IDs matching the filter
    let mut target_ids = BTreeSet::new();
    let mut dep_ids = BTreeSet::new();
    for obj in planned_project.iter_objects() {
        if matcher(&obj.typed_object.stmt) {
            target_ids.insert(obj.id.clone());
        }
        // Also collect dependency objects (secrets, connections, sources) needed by tables
        if matches!(filter, ObjectKindFilter::Tables) {
            match &obj.typed_object.stmt {
                Statement::CreateSecret(_)
                | Statement::CreateConnection(_)
                | Statement::CreateSource(_) => {
                    dep_ids.insert(obj.id.clone());
                }
                _ => {}
            }
        }
    }

    if target_ids.is_empty() {
        progress::info(&format!("No {}s found in project", label));
        return Ok(());
    }

    // For tables, we need to include dependency objects in the create set
    let all_ids: BTreeSet<_> = target_ids.iter().chain(dep_ids.iter()).cloned().collect();
    let all_objects = planned_project.get_sorted_objects_filtered(&all_ids)?;

    // Check which already exist
    let existing = match filter {
        ObjectKindFilter::Tables => {
            client
                .introspection()
                .check_tables_exist(&target_ids)
                .await?
        }
        ObjectKindFilter::Sources => {
            client
                .introspection()
                .check_sources_exist(&target_ids)
                .await?
        }
    };
    // Also check dependency existence
    let existing_deps = if matches!(filter, ObjectKindFilter::Tables) && !dep_ids.is_empty() {
        let mut deps = BTreeSet::new();
        let introspection = client.introspection();
        let (existing_secrets, existing_connections, existing_sources) = tokio::try_join!(
            introspection.check_secrets_exist(&dep_ids),
            introspection.check_connections_exist(&dep_ids),
            introspection.check_sources_exist(&dep_ids),
        )?;
        deps.extend(existing_secrets);
        deps.extend(existing_connections);
        deps.extend(existing_sources);
        deps
    } else {
        BTreeSet::new()
    };

    let all_existing: BTreeSet<_> = existing
        .iter()
        .chain(existing_deps.iter())
        .cloned()
        .collect();

    let to_create: Vec<_> = all_objects
        .into_iter()
        .filter(|(obj_id, _)| !all_existing.contains(obj_id))
        .collect();

    // Reconcile grants on existing target objects
    if !dry_run {
        let (catalog_table, obj_type_keyword, all_privs): (&str, &str, &[&str]) = match filter {
            ObjectKindFilter::Tables => (
                "mz_tables",
                "TABLE",
                &["SELECT", "INSERT", "UPDATE", "DELETE"],
            ),
            ObjectKindFilter::Sources => ("mz_sources", "SOURCE", &["SELECT"]),
        };
        // Build a lookup map to avoid O(N*M) scans
        let obj_map: BTreeMap<_, _> = planned_project
            .iter_objects()
            .map(|obj| (obj.id.clone(), obj))
            .collect();
        let executor = executor::DeploymentExecutor::with_dry_run(&client, false);
        for obj_id in &existing {
            if let Some(obj) = obj_map.get(obj_id) {
                let typed_obj = &obj.typed_object;
                // Re-apply grants (idempotent)
                for grant in &typed_obj.grants {
                    executor.execute_sql(grant).await?;
                }
                let fqn = grants::quoted_fqn(&obj_id.database, &obj_id.schema, &obj_id.object);
                let current_grants = client
                    .introspection()
                    .get_database_object_grants(
                        catalog_table,
                        &obj_id.database,
                        &obj_id.schema,
                        &obj_id.object,
                    )
                    .await
                    .map_err(CliError::Connection)?;
                let desired = grants::desired_grants(&typed_obj.grants, all_privs);
                let revocations = grants::stale_grant_revocations(
                    &current_grants,
                    &desired,
                    obj_type_keyword,
                    &fqn,
                );
                grants::execute_revocations(&client, &revocations, label, &obj_id).await?;
            }
        }
    }

    if to_create.is_empty() {
        progress::info(&format!(
            "All {} {}(s) already exist. Nothing to create.",
            target_ids.len(),
            label,
        ));
        return Ok(());
    }

    progress::info(&format!("Creating {} new {}(s)...", to_create.len(), label));

    let executor = executor::DeploymentExecutor::with_dry_run(&client, dry_run);
    let schemas: BTreeSet<_> = to_create
        .iter()
        .map(|(id, _)| project::SchemaQualifier::new(id.database.clone(), id.schema.clone()))
        .collect();
    executor
        .prepare_databases_and_schemas(&planned_project, &schemas, None)
        .await?;

    let resolver = SecretResolver::new(&settings.secret_config);
    let success_count = execute_table_creates(&executor, &resolver, &to_create).await?;

    progress::success(&format!(
        "Successfully created {} new {}(s)",
        success_count, label
    ));

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
    progress::info("Objects that already exist (skipping):");
    let mut existing_list: Vec<_> = existing_objects.iter().collect();
    existing_list.sort_by_key(|obj| (&obj.database, &obj.schema, &obj.object));
    for table_id in existing_list {
        progress::info(&format!(
            "  - {}.{}.{}",
            table_id.database, table_id.schema, table_id.object
        ));
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
) -> Result<usize, CliError> {
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

        progress::success(&format!(
            "{}.{}.{}",
            object_id.database, object_id.schema, object_id.object
        ));
        success_count += 1;
    }
    Ok(success_count)
}

#[allow(clippy::too_many_arguments)]
/// Finalizes apply-tables with either dry-run output or persisted deployment state.
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
    if !dry_run {
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
    }

    progress::success(&format!(
        "Successfully created {} new table(s)",
        success_count
    ));
    if !existing_objects.is_empty() {
        progress::info(&format!(
            "Skipped {} object(s) that already existed",
            existing_objects.len()
        ));
    }
    Ok(())
}
