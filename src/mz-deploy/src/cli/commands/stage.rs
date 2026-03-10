//! Stage command - deploy to staging environment with renamed schemas and clusters.

use crate::cli::{CliError, executor};
use crate::cli::{git, progress};
use crate::client::quote_identifier;
use crate::client::{Client, ClusterConfig, DeploymentKind, PendingStatement, ReplacementMvRecord};
use crate::config::Settings;
use crate::output;
use crate::project::SchemaQualifier;
use crate::project::ast::Statement;
use crate::project::changeset::ChangeSet;
use crate::project::object_id::ObjectId;
use crate::project::planned::extract_external_indexes;
use crate::project::typed::FullyQualifiedName;
use crate::project::{self, normalize::NormalizingVisitor};
use crate::verbose;
use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use super::ObjectRef;

/// Planning output produced once and consumed by all stage execution phases.
///
/// Keeps stage deterministic by passing one analyzed view of objects/resources through
/// validation, metadata recording, and resource creation.
struct StageAnalysis<'a> {
    objects: Vec<ObjectRef<'a>>,
    sinks: Vec<ObjectRef<'a>>,
    replacement_mvs: Vec<ObjectRef<'a>>,
    schema_set: BTreeSet<SchemaQualifier>,
    cluster_set: BTreeSet<String>,
}

/// Classification result for objects considered during staging.
///
/// Separates deploy-now objects from deferred/special-case categories that apply handles later.
struct PartitionedObjects<'a> {
    objects: Vec<ObjectRef<'a>>,
    sinks: Vec<ObjectRef<'a>>,
    replacement_mvs: Vec<ObjectRef<'a>>,
    table_count: usize,
}

/// Deploy project to staging environment with renamed schemas and clusters.
///
/// This command implements blue/green deployment by creating staging versions of all
/// schemas and clusters with a suffix (e.g., `public_dev` for staging env "dev").
/// Objects are deployed to these staging resources, allowing testing without
/// affecting production. Later, `apply --staging-env` can atomically swap staging
/// and production using ALTER SWAP.
///
/// This command:
/// - Compiles the project (using `compile::run`)
/// - Determines staging environment name (from --name or git SHA)
/// - Creates staging schemas (schema_<env>) for all schemas in the project
/// - Creates staging clusters (cluster_<env>) by cloning production cluster configs
/// - Deploys all objects to staging environment with transformed names
/// - Records deployment metadata for conflict detection
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `stage_name` - Optional staging environment name (defaults to random 7-char hex)
/// * `directory` - Project root directory
/// * `allow_dirty` - Allow deploying with uncommitted changes
/// * `no_rollback` - Skip automatic rollback on failure (for debugging)
/// * `dry_run` - If true, print SQL instead of executing
///
/// # Returns
/// Ok(()) if staging deployment succeeds
///
/// # Errors
/// Returns `CliError::GitDirty` if repository has uncommitted changes and allow_dirty is false
/// Returns `CliError::Connection` for database errors
/// Returns `CliError::Project` for project compilation errors
pub async fn run(
    settings: &Settings,
    stage_name: Option<&str>,
    allow_dirty: bool,
    no_rollback: bool,
    dry_run: bool,
    json_output: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    let stage_name = stage_name
        .map(ToString::to_string)
        .unwrap_or_else(executor::generate_random_env_name);

    progress::info(&format!("Deploying to staging environment: {}", stage_name));

    let planned_project = super::compile::run(settings, true).await?;
    let staging_suffix = format!("_{}", stage_name);

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let Some(analysis) = analyze_project_changes(&client, &planned_project, &stage_name).await?
    else {
        return Ok(());
    };

    validate_project_for_stage(&client, &planned_project, directory).await?;

    if !dry_run {
        record_stage_metadata(
            &client,
            directory,
            &stage_name,
            &staging_suffix,
            &analysis.objects,
            &analysis.sinks,
            &analysis.replacement_mvs,
        )
        .await?;
    }

    if dry_run && json_output {
        let objects_json: Vec<serde_json::Value> = analysis
            .objects
            .iter()
            .map(|(id, _)| {
                serde_json::json!({
                    "database": id.database,
                    "schema": id.schema,
                    "object": id.object,
                })
            })
            .collect();
        let sinks_json: Vec<serde_json::Value> = analysis
            .sinks
            .iter()
            .map(|(id, _)| {
                serde_json::json!({
                    "database": id.database,
                    "schema": id.schema,
                    "object": id.object,
                })
            })
            .collect();
        let replacement_mvs_json: Vec<serde_json::Value> = analysis
            .replacement_mvs
            .iter()
            .map(|(id, _)| {
                serde_json::json!({
                    "database": id.database,
                    "schema": id.schema,
                    "object": id.object,
                })
            })
            .collect();
        let schemas_json: Vec<serde_json::Value> = analysis
            .schema_set
            .iter()
            .map(|sq| {
                serde_json::json!({
                    "database": sq.database,
                    "schema": sq.schema,
                    "staging_schema": format!("{}{}", sq.schema, staging_suffix),
                })
            })
            .collect();
        let clusters_json: Vec<serde_json::Value> = analysis
            .cluster_set
            .iter()
            .map(|c| {
                serde_json::json!({
                    "production_cluster": c,
                    "staging_cluster": format!("{}{}", c, staging_suffix),
                })
            })
            .collect();
        let plan = serde_json::json!({
            "deploy_id": stage_name,
            "schemas": schemas_json,
            "clusters": clusters_json,
            "objects": objects_json,
            "sinks": sinks_json,
            "replacement_mvs": replacement_mvs_json,
        });
        output::machine(&plan);
        return Ok(());
    }

    let success_count = create_resources_with_rollback(
        &client,
        &stage_name,
        &staging_suffix,
        &analysis.schema_set,
        &analysis.cluster_set,
        &planned_project,
        &analysis.objects,
        &analysis.replacement_mvs,
        no_rollback,
        dry_run,
    )
    .await?;

    let total_duration = start_time.elapsed();
    if json_output {
        output::machine(&serde_json::json!({
            "deploy_id": stage_name,
        }));
    } else {
        progress::summary(
            &format!(
                "Successfully deployed to {} objects to '{}' staging environment",
                success_count, stage_name
            ),
            total_duration,
        );
    }
    Ok(())
}

/// Produces the stage deployment plan by diffing against current production snapshot.
///
/// Handles incremental-vs-full mode, applies stage-specific object filtering,
/// validates table dependencies, and returns resource sets required for execution.
async fn analyze_project_changes<'a>(
    client: &Client,
    planned_project: &'a project::planned::Project,
    stage_name: &str,
) -> Result<Option<StageAnalysis<'a>>, CliError> {
    progress::stage_start("Analyzing project changes");
    let analyze_start = Instant::now();

    project::deployment_snapshot::initialize_deployment_table(client).await?;
    if client
        .deployments()
        .get_deployment_metadata(stage_name)
        .await?
        .is_some()
    {
        return Err(CliError::InvalidEnvironmentName {
            name: format!("deployment '{}' already exists", stage_name),
        });
    }

    let new_snapshot = project::deployment_snapshot::build_snapshot_from_planned(planned_project)?;
    let production_snapshot =
        project::deployment_snapshot::load_from_database(client, None).await?;

    let change_set = if production_snapshot.objects.is_empty() {
        None
    } else {
        Some(ChangeSet::from_deployment_snapshot_comparison(
            &production_snapshot,
            &new_snapshot,
            planned_project,
        ))
    };

    let objects = select_stage_objects(planned_project, change_set.as_ref())?;
    if objects.is_empty() && change_set.as_ref().is_some_and(ChangeSet::is_empty) {
        progress::info("No changes detected compared to production, skipping deployment");
        return Ok(None);
    }

    let replacement_object_ids = change_set
        .as_ref()
        .map(|cs| cs.changed_replacement_objects.clone())
        .unwrap_or_default();
    let partitioned = partition_objects(objects, &replacement_object_ids);
    log_partition_summary(&partitioned);

    let object_ids: BTreeSet<_> = partitioned
        .objects
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    client
        .validation()
        .validate_table_dependencies(planned_project, &object_ids)
        .await?;

    let (schema_set, cluster_set) = collect_stage_resources(
        planned_project,
        &partitioned.objects,
        &partitioned.replacement_mvs,
        change_set.as_ref(),
    );

    let analyze_duration = analyze_start.elapsed();
    progress::stage_success(
        &format!(
            "Ready to deploy {} view(s)/materialized view(s)",
            partitioned.objects.len()
        ),
        analyze_duration,
    );

    Ok(Some(StageAnalysis {
        objects: partitioned.objects,
        sinks: partitioned.sinks,
        replacement_mvs: partitioned.replacement_mvs,
        schema_set,
        cluster_set,
    }))
}

/// Chooses the initial object set for stage before stage-specific partitioning.
///
/// Incremental mode uses the change set; full mode uses all sorted project objects.
fn select_stage_objects<'a>(
    planned_project: &'a project::planned::Project,
    change_set: Option<&ChangeSet>,
) -> Result<Vec<ObjectRef<'a>>, CliError> {
    if let Some(cs) = change_set {
        if cs.is_empty() {
            return Ok(Vec::new());
        }
        verbose!("{}", cs);
        Ok(planned_project.get_sorted_objects_filtered(&cs.objects_to_deploy)?)
    } else {
        verbose!("Full deployment: no production deployment found");
        Ok(planned_project.get_sorted_objects()?)
    }
}

/// Splits objects into stage execution categories.
///
/// Tables/sources are excluded, sinks are deferred to apply, and changed replacement MVs
/// are tracked for special replacement handling.
fn partition_objects<'a>(
    objects: Vec<ObjectRef<'a>>,
    replacement_object_ids: &BTreeSet<ObjectId>,
) -> PartitionedObjects<'a> {
    let mut kept = Vec::new();
    let mut sinks = Vec::new();
    let mut replacement_mvs = Vec::new();
    let mut table_count = 0;

    for (object_id, typed_obj) in objects {
        match &typed_obj.stmt {
            Statement::CreateTable(_)
            | Statement::CreateTableFromSource(_)
            | Statement::CreateSource(_) => {
                table_count += 1;
            }
            Statement::CreateSink(_) => sinks.push((object_id, typed_obj)),
            Statement::CreateMaterializedView(_) if replacement_object_ids.contains(&object_id) => {
                replacement_mvs.push((object_id, typed_obj));
            }
            _ => kept.push((object_id, typed_obj)),
        }
    }

    PartitionedObjects {
        objects: kept,
        sinks,
        replacement_mvs,
        table_count,
    }
}

/// Reports the partitioning decisions visible to users in verbose mode.
fn log_partition_summary(partitioned: &PartitionedObjects<'_>) {
    if partitioned.table_count > 0 {
        verbose!(
            "Skipped {} table(s)/source(s) - use 'mz-deploy apply' for those",
            partitioned.table_count
        );
    }
    if !partitioned.sinks.is_empty() {
        verbose!(
            "Found {} sink(s) - will be created during apply after swap",
            partitioned.sinks.len()
        );
    }
    if !partitioned.replacement_mvs.is_empty() {
        verbose!(
            "Found {} replacement MV(s) - will use CREATE REPLACEMENT protocol",
            partitioned.replacement_mvs.len()
        );
    }
}

/// Derives schema/cluster prerequisites for resource creation.
///
/// Combines directly referenced resources with dirty clusters from the change set
/// to ensure swap-time requirements are represented even when not in object SQL.
fn collect_stage_resources(
    planned_project: &project::planned::Project,
    objects: &[ObjectRef<'_>],
    replacement_mvs: &[ObjectRef<'_>],
    change_set: Option<&ChangeSet>,
) -> (BTreeSet<SchemaQualifier>, BTreeSet<String>) {
    let mut schema_set = BTreeSet::new();
    let mut cluster_set = BTreeSet::new();

    for (object_id, typed_obj) in objects.iter().chain(replacement_mvs.iter()) {
        schema_set.insert(SchemaQualifier::new(
            object_id.database.clone(),
            object_id.schema.clone(),
        ));
        cluster_set.extend(typed_obj.clusters());
    }

    if let Some(cs) = change_set {
        for cluster in &cs.dirty_clusters {
            cluster_set.insert(cluster.name.clone());
        }
    } else {
        for cluster in &planned_project.cluster_dependencies {
            cluster_set.insert(cluster.name.clone());
        }
    }
    (schema_set, cluster_set)
}

/// Runs all preflight database validations required before mutating deployment state.
///
/// This is intentionally isolated so stage fails before any metadata/resource writes.
async fn validate_project_for_stage(
    client: &Client,
    planned_project: &project::planned::Project,
    directory: &Path,
) -> Result<(), CliError> {
    progress::stage_start("Validating project");
    let validate_start = Instant::now();
    client
        .validation()
        .validate_project(planned_project, directory)
        .await?;
    client
        .validation()
        .validate_cluster_isolation(planned_project)
        .await?;
    client
        .validation()
        .validate_privileges(planned_project)
        .await?;
    client
        .validation()
        .validate_sink_connections_exist(planned_project)
        .await?;
    let validate_duration = validate_start.elapsed();
    progress::stage_success("All validations passed", validate_duration);
    Ok(())
}

/// Persists stage deployment state and deferred apply actions.
///
/// Records object hashes plus schema deployment kinds, then stores sink/replacement
/// records that the `apply` command consumes after swap.
async fn record_stage_metadata(
    client: &Client,
    directory: &Path,
    stage_name: &str,
    staging_suffix: &str,
    objects: &[ObjectRef<'_>],
    sinks: &[ObjectRef<'_>],
    replacement_mvs: &[ObjectRef<'_>],
) -> Result<(), CliError> {
    progress::stage_start("Recording deployment metadata");
    let metadata_start = Instant::now();
    let metadata = executor::collect_deployment_metadata(client, directory).await;

    let mut staging_snapshot = project::deployment_snapshot::DeploymentSnapshot::default();

    for (object_id, typed_obj) in objects {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot.schemas.insert(
            SchemaQualifier::new(object_id.database.clone(), object_id.schema.clone()),
            DeploymentKind::Objects,
        );
    }

    for (object_id, typed_obj) in sinks {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot
            .schemas
            .entry(SchemaQualifier::new(
                object_id.database.clone(),
                object_id.schema.clone(),
            ))
            .or_insert(DeploymentKind::Sinks);
    }

    for (object_id, typed_obj) in replacement_mvs {
        let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot.schemas.insert(
            SchemaQualifier::new(object_id.database.clone(), object_id.schema.clone()),
            DeploymentKind::Replacement,
        );
    }

    project::deployment_snapshot::write_to_database(
        client,
        &staging_snapshot,
        stage_name,
        &metadata,
        None,
    )
    .await?;

    if !sinks.is_empty() {
        let pending_statements: Vec<PendingStatement> = sinks
            .iter()
            .enumerate()
            .map(|(idx, (object_id, typed_obj))| {
                let original_fqn = FullyQualifiedName::from(object_id.to_unresolved_item_name());
                let visitor = NormalizingVisitor::fully_qualifying(&original_fqn);
                let stmt = typed_obj
                    .stmt
                    .clone()
                    .normalize_name_with(&visitor, &original_fqn.to_item_name())
                    .normalize_dependencies_with(&visitor);
                let hash = project::deployment_snapshot::compute_typed_hash(typed_obj);
                #[allow(clippy::as_conversions)]
                PendingStatement {
                    deploy_id: stage_name.to_string(),
                    sequence_num: idx as i32,
                    database: object_id.database.clone(),
                    schema: object_id.schema.clone(),
                    object: object_id.object.clone(),
                    object_hash: hash,
                    statement_sql: stmt.to_string(),
                    statement_kind: "sink".to_string(),
                    executed_at: None,
                }
            })
            .collect();

        client
            .deployments()
            .insert_pending_statements(&pending_statements)
            .await?;
        verbose!(
            "Stored {} pending sink statement(s)",
            pending_statements.len()
        );
    }

    if !replacement_mvs.is_empty() {
        let records: Vec<ReplacementMvRecord> = replacement_mvs
            .iter()
            .map(|(object_id, _)| ReplacementMvRecord {
                deploy_id: stage_name.to_string(),
                target_database: object_id.database.clone(),
                target_schema: object_id.schema.clone(),
                target_name: object_id.object.clone(),
                replacement_schema: format!("{}{}", object_id.schema, staging_suffix),
            })
            .collect();
        client
            .deployments()
            .insert_replacement_mvs(&records)
            .await?;
        verbose!("Stored {} replacement MV record(s)", records.len());
    }

    let metadata_duration = metadata_start.elapsed();
    progress::stage_success("Deployment metadata recorded", metadata_duration);
    Ok(())
}

/// Create staging resources (schemas, clusters, objects) with automatic rollback on failure.
///
/// This function performs all resource creation and automatically triggers rollback
/// on failure unless the no_rollback flag is set.
#[allow(clippy::too_many_arguments)]
async fn create_resources_with_rollback<'a>(
    client: &crate::client::Client,
    stage_name: &str,
    staging_suffix: &str,
    schema_set: &BTreeSet<SchemaQualifier>,
    cluster_set: &BTreeSet<String>,
    planned_project: &'a project::planned::Project,
    objects: &'a [(ObjectId, &'a project::typed::DatabaseObject)],
    replacement_mvs: &'a [(ObjectId, &'a project::typed::DatabaseObject)],
    no_rollback: bool,
    dry_run: bool,
) -> Result<usize, CliError> {
    // Create executor with dry-run mode
    let executor = executor::DeploymentExecutor::with_dry_run(client, dry_run);

    // Wrap resource creation in a closure that we can call and handle errors from
    let create_result = async {
        // Stage 4a: Create project databases that aren't in schema_set
        // (schema_set databases will be created by prepare_databases_and_schemas)
        progress::info("Creating project databases if not exists");
        let schema_set_dbs: BTreeSet<&str> = schema_set.iter().map(|sq| sq.database.as_str()).collect();
        if !dry_run {
            for db in &planned_project.databases {
                if !schema_set_dbs.contains(db.name.as_str()) {
                    client.provisioning().create_database(&db.name).await?;
                    verbose!("  Ensured database {} exists", db.name);
                }
            }
        } else {
            for db in &planned_project.databases {
                if !schema_set_dbs.contains(db.name.as_str()) {
                    let create_db_sql =
                        format!("CREATE DATABASE IF NOT EXISTS {}", quote_identifier(&db.name));
                    executor.execute_sql(&create_db_sql).await?;
                }
            }
        }

        // Stage 4b: Create staging schemas + apply mod_statements
        progress::stage_start("Creating staging schemas and applying setup statements");
        let schema_start = Instant::now();
        executor.prepare_databases_and_schemas(planned_project, schema_set, Some(staging_suffix)).await?;
        let schema_duration = schema_start.elapsed();
        progress::stage_success(
            &format!("Created {} staging schema(s) with setup statements", schema_set.len()),
            schema_duration,
        );

        // Create production schemas for swap (non-dry-run only)
        if !dry_run {
            progress::info("Creating production schemas if not exists");
            for sq in schema_set {
                client.provisioning().create_schema(&sq.database, &sq.schema).await?;
                verbose!("  Ensured schema {}.{} exists", sq.database, sq.schema);
            }
        }

        if !dry_run {
            // Write cluster mappings to deploy.clusters table BEFORE creating clusters
            // This allows abort logic to clean up even if cluster creation fails
            let cluster_names: Vec<String> = cluster_set.iter().cloned().collect();
            client
                .deployments().insert_deployment_clusters(stage_name, &cluster_names)
                .await?;
            verbose!("Cluster mappings recorded");
        }

        // Stage 5: Create staging clusters (by cloning production cluster configs)
        progress::stage_start("Creating staging clusters");
        let cluster_start = Instant::now();
        let mut created_clusters = 0;

        // Batch check which staging clusters already exist (skip in dry-run mode)
        let existing_staging_clusters = if !dry_run {
            let staging_cluster_names: Vec<String> = cluster_set
                .iter()
                .map(|name| format!("{}{}", name, staging_suffix))
                .collect();
            client.introspection().check_clusters_exist(&staging_cluster_names).await?
        } else {
            BTreeSet::new()
        };

        for prod_cluster in cluster_set {
            let staging_cluster = format!("{}{}", prod_cluster, staging_suffix);

            if dry_run {
                // In dry-run mode, just print the CREATE CLUSTER statement
                // We can't check if cluster exists or get prod config without side effects
                let create_cluster_sql = format!(
                    "CREATE CLUSTER {} (SIZE = '<from {}>')",
                    staging_cluster, prod_cluster
                );
                executor.execute_sql(&create_cluster_sql).await?;
                created_clusters += 1;
                continue;
            }

            // Check if staging cluster already exists using batch result
            if existing_staging_clusters.contains(&staging_cluster) {
                verbose!("  Cluster '{}' already exists, skipping", staging_cluster);
                continue;
            }

            // Get production cluster configuration (handles both managed and unmanaged)
            let config = client.introspection().get_cluster_config(prod_cluster).await?;

            let config = match config {
                Some(config) => config,
                None => {
                    return Err(CliError::ClusterNotFound {
                        name: prod_cluster.clone(),
                    });
                }
            };

            // Create staging cluster with same configuration
            client
                .provisioning()
                .create_cluster_with_config(&staging_cluster, &config)
                .await?;
            created_clusters += 1;

            // Log details based on cluster type
            match &config {
                ClusterConfig::Managed { options, grants } => {
                    verbose!(
                        "  Created managed cluster '{}' (size: {}, replication_factor: {}, {} grant(s), cloned from '{}')",
                        staging_cluster,
                        options.size,
                        options.replication_factor,
                        grants.len(),
                        prod_cluster
                    );
                }
                ClusterConfig::Unmanaged { replicas, grants } => {
                    verbose!(
                        "  Created unmanaged cluster '{}' with {} replica(s), {} grant(s) (cloned from '{}')",
                        staging_cluster,
                        replicas.len(),
                        grants.len(),
                        prod_cluster
                    );
                    for replica in replicas {
                        verbose!(
                            "    - {} (size: {}{})",
                            replica.name,
                            replica.size,
                            replica
                                .availability_zone
                                .as_ref()
                                .map(|az| format!(", az: {}", az))
                                .unwrap_or_default()
                        );
                    }
                }
            }
        }

        let cluster_duration = cluster_start.elapsed();
        progress::stage_success(
            &format!("Created {} cluster(s)", created_clusters),
            cluster_duration,
        );

        // Stage 6: Deploy objects using staging transformer
        progress::stage_start("Deploying objects to staging");
        let deploy_start = Instant::now();

        // Collect ObjectIds from objects being deployed for the staging transformer
        // Include both regular objects and replacement MVs
        let objects_to_deploy_set: BTreeSet<_> = objects
            .iter()
            .chain(replacement_mvs.iter())
            .map(|(oid, _)| oid.clone())
            .collect();

        // Deploy external indexes
        let mut external_indexes: Vec<_> = planned_project
            .iter_objects()
            .filter(|object| !objects_to_deploy_set.contains(&object.id))
            .flat_map(extract_external_indexes)
            .filter_map(|(cluster, index)| cluster_set.contains(&cluster.name).then_some(index))
            .collect();

        // Transform cluster names in external indexes for staging
        crate::project::normalize::transform_cluster_names_for_staging(
            &mut external_indexes,
            staging_suffix,
        );
        for index in external_indexes {
            verbose!("Creating external index {}", index);
            executor.execute_sql(&index).await?;
        }

        let mut success_count = 0;

        // Deploy regular objects
        for (idx, (object_id, typed_obj)) in objects.iter().enumerate() {
            verbose!(
                "Applying {}/{}: {}{} (to schema {}{})",
                idx + 1,
                objects.len(),
                &object_id.object,
                staging_suffix,
                &object_id.schema,
                staging_suffix
            );

            deploy_single_object(
                &executor,
                object_id,
                typed_obj,
                staging_suffix,
                planned_project,
                &objects_to_deploy_set,
                |stmt| stmt,
            )
            .await?;
            success_count += 1;
        }

        // Deploy replacement MVs using CREATE REPLACEMENT MATERIALIZED VIEW ... FOR
        for (idx, (object_id, typed_obj)) in replacement_mvs.iter().enumerate() {
            verbose!(
                "Applying replacement MV {}/{}: {} FOR {}.{}.{}",
                idx + 1,
                replacement_mvs.len(),
                &object_id.object,
                &object_id.database,
                &object_id.schema,
                &object_id.object
            );

            let production_target = object_id.to_unresolved_item_name();
            deploy_single_object(
                &executor,
                object_id,
                typed_obj,
                staging_suffix,
                planned_project,
                &objects_to_deploy_set,
                |stmt| match stmt {
                    Statement::CreateMaterializedView(mut mv) => {
                        mv.replacement_for = Some(
                            mz_sql_parser::ast::RawItemName::Name(production_target),
                        );
                        Statement::CreateMaterializedView(mv)
                    }
                    other => other,
                },
            )
            .await?;
            success_count += 1;
        }

        let deploy_duration = deploy_start.elapsed();
        progress::stage_success(
            &format!("Deployed {} view(s)/materialized view(s)", success_count),
            deploy_duration,
        );

        // Return success count
        Ok::<usize, CliError>(success_count)
    }
    .await;

    // Handle result with rollback on failure (skip rollback in dry-run mode)
    match create_result {
        Ok(count) => Ok(count),
        Err(e) => {
            if dry_run || no_rollback {
                if !dry_run {
                    progress::error(
                        "Deployment failed (skipping rollback due to --no-rollback flag)",
                    );
                }
                return Err(e);
            }

            progress::error("Deployment failed, rolling back...");
            let (schemas, clusters) = rollback_staging_resources(client, stage_name).await;

            if schemas > 0 || clusters > 0 {
                progress::info(&format!(
                    "Rolled back: {} schema(s), {} cluster(s)",
                    schemas, clusters
                ));
            }

            Err(e)
        }
    }
}

/// Rollback staging resources on deployment failure.
///
/// This function performs best-effort cleanup of staging resources created during
/// a failed deployment. It mirrors the abort command logic but uses a best-effort
/// approach where cleanup failures are logged rather than returning errors.
///
/// # Arguments
/// * `client` - Database client
/// * `environment` - Staging environment name
///
/// # Returns
/// Number of schemas and clusters that were cleaned up (for summary message)
async fn rollback_staging_resources(
    client: &crate::client::Client,
    environment: &str,
) -> (usize, usize) {
    let staging_schemas = best_effort_fetch(
        client
            .introspection()
            .get_staging_schemas(environment)
            .await,
        "query staging schemas",
    );
    let staging_clusters = best_effort_fetch(
        client
            .introspection()
            .get_staging_clusters(environment)
            .await,
        "query staging clusters",
    );

    let schema_count = staging_schemas.len();
    let cluster_count = staging_clusters.len();

    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        if let Err(e) = client
            .introspection()
            .drop_staging_schemas(&staging_schemas)
            .await
        {
            verbose!("Warning: Failed to drop some schemas: {}", e);
        } else {
            for sq in &staging_schemas {
                verbose!("  Dropped {}.{}", sq.database, sq.schema);
            }
        }
    }

    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        if let Err(e) = client
            .introspection()
            .drop_staging_clusters(&staging_clusters)
            .await
        {
            verbose!("Warning: Failed to drop some clusters: {}", e);
        } else {
            for cluster in &staging_clusters {
                verbose!("  Dropped {}", cluster);
            }
        }
    }

    verbose!("Deleting deployment records...");
    best_effort_delete(
        client
            .deployments()
            .delete_deployment_clusters(environment)
            .await,
        "delete cluster records",
    );
    best_effort_delete(
        client
            .deployments()
            .delete_pending_statements(environment)
            .await,
        "delete pending statements",
    );
    best_effort_delete(
        client
            .deployments()
            .delete_replacement_mvs(environment)
            .await,
        "delete replacement MV records",
    );
    best_effort_delete(
        client.deployments().delete_deployment(environment).await,
        "delete deployment records",
    );

    (schema_count, cluster_count)
}

/// Best-effort fetch wrapper used by rollback.
///
/// Converts query failures into empty results so cleanup can continue and report
/// as much progress as possible instead of aborting midway.
fn best_effort_fetch<T, E: std::fmt::Display>(result: Result<Vec<T>, E>, action: &str) -> Vec<T> {
    match result {
        Ok(values) => values,
        Err(e) => {
            verbose!("Warning: Failed to {}: {}", action, e);
            vec![]
        }
    }
}

/// Best-effort delete wrapper used by rollback metadata cleanup.
fn best_effort_delete<E: std::fmt::Display>(result: Result<(), E>, action: &str) {
    if let Err(e) = result {
        verbose!("Warning: Failed to {}: {}", action, e);
    }
}

/// Deploy a single object to the staging environment.
///
/// Handles normalization, execution, and deployment of indexes/grants/comments.
/// The `transform` callback allows the caller to modify the normalized statement
/// before execution (e.g., to set `replacement_for` on replacement MVs).
async fn deploy_single_object(
    executor: &executor::DeploymentExecutor<'_>,
    object_id: &ObjectId,
    typed_obj: &project::typed::DatabaseObject,
    staging_suffix: &str,
    planned_project: &project::planned::Project,
    objects_to_deploy_set: &BTreeSet<ObjectId>,
    transform: impl FnOnce(Statement) -> Statement,
) -> Result<(), CliError> {
    let original_fqn = FullyQualifiedName::from(object_id.to_unresolved_item_name());

    let visitor = NormalizingVisitor::staging(
        &original_fqn,
        staging_suffix.to_string(),
        &planned_project.external_dependencies,
        Some(objects_to_deploy_set),
    );

    let stmt = typed_obj
        .stmt
        .clone()
        .normalize_name_with(&visitor, &original_fqn.to_item_name())
        .normalize_dependencies_with(&visitor)
        .normalize_cluster_with(&visitor);

    let stmt = transform(stmt);
    executor.execute_sql(&stmt).await?;

    // Deploy indexes, grants, and comments
    let mut indexes = typed_obj.indexes.clone();
    let mut grants = typed_obj.grants.clone();
    let mut comments = typed_obj.comments.clone();

    visitor.normalize_index_references(&mut indexes);
    visitor.normalize_index_clusters(&mut indexes);
    visitor.normalize_grant_references(&mut grants);
    visitor.normalize_comment_references(&mut comments);

    for index in &indexes {
        executor.execute_sql(index).await?;
    }

    for grant in &grants {
        executor.execute_sql(grant).await?;
    }

    for comment in &comments {
        executor.execute_sql(comment).await?;
    }

    Ok(())
}
