//! Stage command - deploy to staging environment with renamed schemas and clusters.

use super::ObjectRef;
use crate::cli::{CliError, executor};
use crate::cli::{git, progress};
use crate::client::{
    Client, ClusterConfig, ClusterOptions, DeploymentKind, PendingStatement, ReplacementMvRecord,
};
use crate::config::Settings;
use crate::log;
use crate::project::SchemaQualifier;
use crate::project::ast::Statement;
use crate::project::changeset::ChangeSet;
use crate::project::object_id::ObjectId;
use crate::project::planned::extract_external_indexes;
use crate::project::typed::FullyQualifiedName;
use crate::project::{self, normalize::NormalizingVisitor};
use crate::verbose;
use mz_ore::option::OptionExt;
use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;
use std::time::Instant;

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
/// * `stage_name` - Optional staging environment name (defaults to git SHA prefix, or random 7-char hex)
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
#[derive(serde::Serialize)]
struct StageResult {
    deploy_id: String,
    objects_deployed: usize,
    #[serde(skip)]
    duration: std::time::Duration,
}

impl fmt::Display for StageResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "  \u{2713} Successfully deployed {} objects to '{}' staging environment ({:.1}s)",
            self.objects_deployed,
            self.deploy_id,
            self.duration.as_secs_f64()
        )
    }
}

#[derive(serde::Serialize)]
struct StagePlan {
    deploy_id: String,
    schemas: Vec<StagePlanSchema>,
    clusters: Vec<StagePlanCluster>,
    objects: Vec<StagePlanObject>,
    sinks: Vec<StagePlanObject>,
    replacement_mvs: Vec<StagePlanObject>,
}

#[derive(serde::Serialize)]
struct StagePlanSchema {
    database: String,
    schema: String,
    staging_schema: String,
}

#[derive(serde::Serialize)]
struct StagePlanCluster {
    production_cluster: String,
    staging_cluster: String,
}

#[derive(serde::Serialize)]
struct StagePlanObject {
    database: String,
    schema: String,
    object: String,
}

impl fmt::Display for StagePlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Stage plan for '{}':", self.deploy_id)?;

        if !self.schemas.is_empty() {
            writeln!(f, "\nSchemas ({}):", self.schemas.len())?;
            for s in &self.schemas {
                writeln!(
                    f,
                    "    {}.{} \u{2192} {}",
                    s.database, s.schema, s.staging_schema
                )?;
            }
        }

        if !self.clusters.is_empty() {
            writeln!(f, "\nClusters ({}):", self.clusters.len())?;
            for c in &self.clusters {
                writeln!(
                    f,
                    "    {} \u{2192} {}",
                    c.production_cluster, c.staging_cluster
                )?;
            }
        }

        if !self.objects.is_empty() {
            writeln!(f, "\nObjects ({}):", self.objects.len())?;
            for o in &self.objects {
                writeln!(f, "    {}.{}.{}", o.database, o.schema, o.object)?;
            }
        }

        if !self.sinks.is_empty() {
            writeln!(f, "\nSinks ({}):", self.sinks.len())?;
            for s in &self.sinks {
                writeln!(f, "    {}.{}.{}", s.database, s.schema, s.object)?;
            }
        }

        if !self.replacement_mvs.is_empty() {
            writeln!(f, "\nReplacement MVs ({}):", self.replacement_mvs.len())?;
            for m in &self.replacement_mvs {
                writeln!(f, "    {}.{}.{}", m.database, m.schema, m.object)?;
            }
        }

        Ok(())
    }
}

pub async fn run(
    settings: &Settings,
    stage_name: Option<&str>,
    allow_dirty: bool,
    no_rollback: bool,
    dry_run: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    if !allow_dirty && git::is_dirty(directory) {
        return Err(CliError::GitDirty);
    }

    let stage_name = stage_name
        .owned()
        .or_else(|| git::get_git_commit(directory).map(|sha| sha.chars().take(7).collect()))
        .unwrap_or_else(executor::generate_random_env_name);

    progress::info(&format!("Deploying to staging environment: {}", stage_name));

    let planned_project = super::compile::run(settings, true, true).await?;
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

    if dry_run {
        let plan = StagePlan {
            deploy_id: stage_name.to_string(),
            schemas: analysis
                .schema_set
                .iter()
                .map(|sq| StagePlanSchema {
                    database: sq.database.clone(),
                    schema: sq.schema.clone(),
                    staging_schema: format!("{}{}", sq.schema, staging_suffix),
                })
                .collect(),
            clusters: analysis
                .cluster_set
                .iter()
                .map(|c| StagePlanCluster {
                    production_cluster: c.clone(),
                    staging_cluster: format!("{}{}", c, staging_suffix),
                })
                .collect(),
            objects: analysis
                .objects
                .iter()
                .map(|(id, _)| StagePlanObject {
                    database: id.database.clone(),
                    schema: id.schema.clone(),
                    object: id.object.clone(),
                })
                .collect(),
            sinks: analysis
                .sinks
                .iter()
                .map(|(id, _)| StagePlanObject {
                    database: id.database.clone(),
                    schema: id.schema.clone(),
                    object: id.object.clone(),
                })
                .collect(),
            replacement_mvs: analysis
                .replacement_mvs
                .iter()
                .map(|(id, _)| StagePlanObject {
                    database: id.database.clone(),
                    schema: id.schema.clone(),
                    object: id.object.clone(),
                })
                .collect(),
        };
        log::output(&plan);
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

    let result = StageResult {
        deploy_id: stage_name.to_string(),
        objects_deployed: success_count,
        duration: start_time.elapsed(),
    };
    log::output(&result);
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

    // Reject adding brand-new objects to a schema that already has production objects.
    // During incremental deployment:
    //
    //   1. The changeset correctly classifies these as `new_replacement_objects`
    //   2. But `new_replacement_objects` is never consumed by `partition_objects` —
    //      only `changed_replacement_objects` feeds into it
    //   3. The new MV ends up in the regular `objects` partition and deploys to the
    //      staging schema (e.g. `core_v3`)
    //   4. Metadata for the production schema (`core`) gets overwritten to
    //      `DeploymentKind::Replacement` by the changed MVs
    //   5. During promote, the staging schema is skipped from swap and dropped CASCADE
    //      — the new MV is lost
    //
    // The proper long-term fix is to support `ALTER MATERIALIZED VIEW ... SET SCHEMA`.
    // With that, new MVs could be deployed to the staging schema alongside changed MVs,
    // then moved into the production schema during promote via `SET SCHEMA` instead of
    // relying on schema swap. This would eliminate the need for mixed deployment kinds
    // or special-casing in `partition_objects` — new objects simply deploy to staging
    // and get relocated on promote, just like changed objects get swapped.
    //
    // A brand-new stable schema (no prior production objects) deploys fine via normal
    // blue-green swap — only schemas with existing production objects are affected.
    if let Some(ref cs) = change_set {
        validate_no_new_objects_in_existing_stable_schemas(cs, &production_snapshot)?;
    }

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

    let (schema_set, cluster_set) =
        collect_stage_resources(&partitioned.objects, &partitioned.replacement_mvs);

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
            | Statement::CreateSource(_)
            | Statement::CreateSecret(_)
            | Statement::CreateConnection(_) => {
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
/// Builds schema and cluster sets solely from the objects being staged.
/// Apply-managed objects (sources, tables, secrets, connections) are excluded
/// by `partition_objects`, so their schemas and clusters are never staged.
fn collect_stage_resources(
    objects: &[ObjectRef<'_>],
    replacement_mvs: &[ObjectRef<'_>],
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

/// Top-level orchestrator for the staging deployment pipeline.
///
/// Provisions all databases, schemas, clusters, and objects needed for a blue-green
/// deployment. On failure, automatically rolls back every resource created during
/// this invocation unless the `no_rollback` flag is set.
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
    let executor = executor::DeploymentExecutor::with_dry_run(client, dry_run);

    let result = async {
        create_databases_and_schemas(&executor, planned_project, schema_set, staging_suffix)
            .await?;
        create_staging_clusters(&executor, client, stage_name, cluster_set, staging_suffix).await?;
        deploy_objects_to_staging(
            &executor,
            objects,
            replacement_mvs,
            planned_project,
            cluster_set,
            staging_suffix,
        )
        .await
    }
    .await;

    match result {
        Ok(count) => Ok(count),
        Err(e) if dry_run || no_rollback => {
            if !dry_run {
                progress::error("Deployment failed (skipping rollback due to --no-rollback flag)");
            }
            Err(e)
        }
        Err(e) => {
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

/// Provision all database and schema infrastructure required for a staged deployment.
///
/// After this completes, both the suffixed staging schemas (where new objects will be
/// created) and the production schemas (swap targets) are guaranteed to exist.
async fn create_databases_and_schemas(
    executor: &executor::DeploymentExecutor<'_>,
    planned_project: &project::planned::Project,
    schema_set: &BTreeSet<SchemaQualifier>,
    staging_suffix: &str,
) -> Result<(), CliError> {
    // Create project databases that aren't in schema_set
    // (schema_set databases will be created by prepare_databases_and_schemas)
    progress::info("Creating project databases if not exists");
    let schema_set_dbs: BTreeSet<&str> = schema_set.iter().map(|sq| sq.database.as_str()).collect();
    for db in &planned_project.databases {
        if !schema_set_dbs.contains(db.name.as_str()) {
            executor.ensure_database(&db.name).await?;
            verbose!("  Ensured database {} exists", db.name);
        }
    }

    // Create staging schemas + apply mod_statements
    progress::stage_start("Creating staging schemas and applying setup statements");
    let schema_start = Instant::now();
    executor
        .prepare_databases_and_schemas(planned_project, schema_set, Some(staging_suffix))
        .await?;
    let schema_duration = schema_start.elapsed();
    progress::stage_success(
        &format!(
            "Created {} staging schema(s) with setup statements",
            schema_set.len()
        ),
        schema_duration,
    );

    // Create production schemas for swap
    if !executor.is_dry_run() {
        progress::info("Creating production schemas if not exists");
        for sq in schema_set {
            executor.ensure_schema(&sq.database, &sq.schema).await?;
            verbose!("  Ensured schema {}.{} exists", sq.database, sq.schema);
        }
    }

    Ok(())
}

/// Provision staging clusters that mirror the size and configuration of their
/// production counterparts.
///
/// Clusters that already exist are skipped. Cluster names are recorded for rollback
/// tracking before any cluster is created, so partial failures can be cleaned up.
async fn create_staging_clusters(
    executor: &executor::DeploymentExecutor<'_>,
    client: &Client,
    stage_name: &str,
    cluster_set: &BTreeSet<String>,
    staging_suffix: &str,
) -> Result<(), CliError> {
    // Write cluster mappings BEFORE creating clusters so abort can clean up on failure
    let cluster_names: Vec<String> = cluster_set.iter().cloned().collect();
    executor
        .record_deployment_clusters(stage_name, &cluster_names)
        .await?;

    progress::stage_start("Creating staging clusters");
    let cluster_start = Instant::now();
    let mut created_clusters = 0;

    // Batch check which staging clusters already exist (skip in dry-run mode)
    let existing_staging_clusters = if !executor.is_dry_run() {
        let staging_cluster_names: Vec<String> = cluster_set
            .iter()
            .map(|name| format!("{}{}", name, staging_suffix))
            .collect();
        client
            .introspection()
            .check_clusters_exist(&staging_cluster_names)
            .await?
    } else {
        BTreeSet::new()
    };

    for prod_cluster in cluster_set {
        let staging_cluster = format!("{}{}", prod_cluster, staging_suffix);

        if executor.is_dry_run() {
            // Config is unused in dry-run mode; provide a placeholder.
            let placeholder = ClusterConfig::Managed {
                options: ClusterOptions {
                    size: String::new(),
                    replication_factor: 1,
                },
                grants: Vec::new(),
            };
            executor
                .create_cluster(&staging_cluster, prod_cluster, &placeholder)
                .await?;
            created_clusters += 1;
            continue;
        }

        // Check if staging cluster already exists using batch result
        if existing_staging_clusters.contains(&staging_cluster) {
            verbose!("  Cluster '{}' already exists, skipping", staging_cluster);
            continue;
        }

        // Get production cluster configuration (handles both managed and unmanaged)
        let config = client
            .introspection()
            .get_cluster_config(prod_cluster)
            .await?;

        let config = match config {
            Some(config) => config,
            None => {
                return Err(CliError::ClusterNotFound {
                    name: prod_cluster.clone(),
                });
            }
        };

        executor
            .create_cluster(&staging_cluster, prod_cluster, &config)
            .await?;
        created_clusters += 1;

        log_cluster_creation(&staging_cluster, prod_cluster, &config);
    }

    let cluster_duration = cluster_start.elapsed();
    progress::stage_success(
        &format!("Created {} cluster(s)", created_clusters),
        cluster_duration,
    );

    Ok(())
}

/// Log verbose details about a newly created staging cluster.
fn log_cluster_creation(staging_cluster: &str, prod_cluster: &str, config: &ClusterConfig) {
    match config {
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

/// Execute all object definitions (views, materialized views, indexes) into the
/// staging schemas.
///
/// Regular objects are created with suffixed names; replacement materialized views
/// are linked to their production targets via `CREATE REPLACEMENT MATERIALIZED VIEW
/// ... FOR`. Returns the total number of successfully deployed objects.
async fn deploy_objects_to_staging<'a>(
    executor: &executor::DeploymentExecutor<'_>,
    objects: &'a [(ObjectId, &'a project::typed::DatabaseObject)],
    replacement_mvs: &'a [(ObjectId, &'a project::typed::DatabaseObject)],
    planned_project: &'a project::planned::Project,
    cluster_set: &BTreeSet<String>,
    staging_suffix: &str,
) -> Result<usize, CliError> {
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

    // Build the set of replacement object IDs from the replacement MVs slice.
    // Only these specific objects have their references left unsuffixed.
    let replacement_object_ids: BTreeSet<ObjectId> =
        replacement_mvs.iter().map(|(oid, _)| oid.clone()).collect();

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
            executor,
            object_id,
            typed_obj,
            staging_suffix,
            planned_project,
            &objects_to_deploy_set,
            &replacement_object_ids,
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
            executor,
            object_id,
            typed_obj,
            staging_suffix,
            planned_project,
            &objects_to_deploy_set,
            &replacement_object_ids,
            |stmt| match stmt {
                Statement::CreateMaterializedView(mut mv) => {
                    mv.replacement_for =
                        Some(mz_sql_parser::ast::RawItemName::Name(production_target));
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

    Ok(success_count)
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
///
/// `replacement_objects` is the set of specific object IDs being updated
/// in-place via replacement MVs. References to these objects are left
/// unsuffixed (pointing to production). During full deployment the set is
/// empty, so every reference is suffixed to point at the staging schemas.
async fn deploy_single_object(
    executor: &executor::DeploymentExecutor<'_>,
    object_id: &ObjectId,
    typed_obj: &project::typed::DatabaseObject,
    staging_suffix: &str,
    planned_project: &project::planned::Project,
    objects_to_deploy_set: &BTreeSet<ObjectId>,
    replacement_objects: &BTreeSet<ObjectId>,
    transform: impl FnOnce(Statement) -> Statement,
) -> Result<(), CliError> {
    let original_fqn = FullyQualifiedName::from(object_id.to_unresolved_item_name());

    let visitor = NormalizingVisitor::staging(
        &original_fqn,
        staging_suffix.to_string(),
        &planned_project.external_dependencies,
        Some(objects_to_deploy_set),
        replacement_objects,
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

/// Check that no new replacement objects are being added to schemas that already
/// have production objects.
fn validate_no_new_objects_in_existing_stable_schemas(
    change_set: &ChangeSet,
    production_snapshot: &project::deployment_snapshot::DeploymentSnapshot,
) -> Result<(), CliError> {
    let blocked: Vec<_> = change_set
        .new_replacement_objects
        .iter()
        .filter(|obj| {
            production_snapshot
                .objects
                .keys()
                .any(|prod| prod.database == obj.database && prod.schema == obj.schema)
        })
        .collect();

    if blocked.is_empty() {
        return Ok(());
    }

    let first = blocked[0];
    Err(CliError::NewObjectInExistingStableSchema {
        database: first.database.clone(),
        schema: first.schema.clone(),
        objects: blocked.iter().map(|o| o.object.clone()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::deployment_snapshot::build_snapshot_from_planned;
    use crate::project::object_id::ObjectId;
    use crate::project::typed;
    use std::collections::{BTreeMap, BTreeSet};

    /// Parse SQL strings into a typed::DatabaseObject.
    ///
    /// The first CREATE statement becomes the main statement.
    /// Any CREATE INDEX statements become entries in the indexes vec.
    fn make_typed_object(sqls: &[&str]) -> typed::DatabaseObject {
        let mut stmt = None;
        let mut indexes = Vec::new();

        for sql in sqls {
            let parsed = mz_sql_parser::parser::parse_statements(sql).unwrap();
            for p in parsed {
                match p.ast {
                    mz_sql_parser::ast::Statement::CreateView(s) => {
                        stmt = Some(Statement::CreateView(s));
                    }
                    mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                        stmt = Some(Statement::CreateMaterializedView(s));
                    }
                    mz_sql_parser::ast::Statement::CreateTable(s) => {
                        stmt = Some(Statement::CreateTable(s));
                    }
                    mz_sql_parser::ast::Statement::CreateSource(s) => {
                        stmt = Some(Statement::CreateSource(s));
                    }
                    mz_sql_parser::ast::Statement::CreateConnection(s) => {
                        stmt = Some(Statement::CreateConnection(s));
                    }
                    mz_sql_parser::ast::Statement::CreateSecret(s) => {
                        stmt = Some(Statement::CreateSecret(s));
                    }
                    mz_sql_parser::ast::Statement::CreateIndex(s) => {
                        indexes.push(s);
                    }
                    other => panic!("Unexpected statement type: {:?}", other),
                }
            }
        }

        typed::DatabaseObject {
            stmt: stmt.expect("Expected at least one CREATE statement"),
            indexes,
            grants: vec![],
            comments: vec![],
            tests: vec![],
        }
    }

    /// Build a planned::Project from a list of (database, schema, object_name, typed_obj) tuples.
    fn make_planned_project(
        objects: Vec<(&str, &str, &str, typed::DatabaseObject)>,
    ) -> project::planned::Project {
        // Group into databases -> schemas -> objects
        let mut db_map: BTreeMap<String, BTreeMap<String, Vec<typed::DatabaseObject>>> =
            BTreeMap::new();

        for (database, schema, _name, typed_obj) in objects {
            db_map
                .entry(database.to_string())
                .or_default()
                .entry(schema.to_string())
                .or_default()
                .push(typed_obj);
        }

        let databases: Vec<typed::Database> = db_map
            .into_iter()
            .map(|(db_name, schemas)| typed::Database {
                name: db_name,
                schemas: schemas
                    .into_iter()
                    .map(|(schema_name, objs)| typed::Schema {
                        name: schema_name,
                        objects: objs,
                        mod_statements: None,
                    })
                    .collect(),
                mod_statements: None,
            })
            .collect();

        let typed_project = typed::Project {
            databases,
            replacement_schemas: BTreeSet::new(),
        };

        project::planned::Project::from(typed_project)
    }

    // =========================================================================
    // Test 1: Full deploy, view NOT indexed — mixed schema types
    // =========================================================================
    #[test]
    fn test_full_deploy_view_not_indexed_mixed_types() {
        let view_obj = make_typed_object(&["CREATE VIEW my_view AS SELECT 1"]);
        let table_obj = make_typed_object(&["CREATE TABLE my_table (id INT)"]);
        let source_obj = make_typed_object(&[
            "CREATE SOURCE my_source IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER",
        ]);
        let conn_obj =
            make_typed_object(&["CREATE CONNECTION my_conn TO KAFKA (BROKER 'localhost:9092')"]);
        let secret_obj = make_typed_object(&["CREATE SECRET my_secret AS 'hunter2'"]);

        let objects: Vec<ObjectRef> = vec![
            (
                ObjectId::new("db".into(), "public".into(), "my_view".into()),
                &view_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_table".into()),
                &table_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_source".into()),
                &source_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_conn".into()),
                &conn_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_secret".into()),
                &secret_obj,
            ),
        ];

        let replacement_ids = BTreeSet::new();
        let partitioned = partition_objects(objects, &replacement_ids);

        // Only the view should be in staged objects
        assert_eq!(
            partitioned.objects.len(),
            1,
            "Only the view should be staged"
        );
        assert_eq!(partitioned.objects[0].0.object, "my_view");

        // Table, source, connection, secret should be counted as skipped
        assert_eq!(
            partitioned.table_count, 4,
            "Table, source, connection, and secret should all be skipped"
        );

        // No sinks or replacement MVs
        assert!(partitioned.sinks.is_empty());
        assert!(partitioned.replacement_mvs.is_empty());

        // Collect stage resources
        let (schema_set, cluster_set) =
            collect_stage_resources(&partitioned.objects, &partitioned.replacement_mvs);

        // Should have the view's schema
        assert_eq!(schema_set.len(), 1);
        assert!(schema_set.contains(&SchemaQualifier::new("db".into(), "public".into())));

        // View has no cluster, so cluster_set should be empty
        assert!(
            cluster_set.is_empty(),
            "View without index should not require any clusters"
        );
    }

    // =========================================================================
    // Test 2: Full deploy, view indexed on different cluster than source
    // =========================================================================
    #[test]
    fn test_full_deploy_view_indexed_different_cluster() {
        let view_obj = make_typed_object(&[
            "CREATE VIEW my_view AS SELECT 1",
            "CREATE INDEX my_idx IN CLUSTER index_cluster ON my_view (column1)",
        ]);
        let table_obj = make_typed_object(&["CREATE TABLE my_table (id INT)"]);
        let source_obj = make_typed_object(&[
            "CREATE SOURCE my_source IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER",
        ]);
        let conn_obj =
            make_typed_object(&["CREATE CONNECTION my_conn TO KAFKA (BROKER 'localhost:9092')"]);
        let secret_obj = make_typed_object(&["CREATE SECRET my_secret AS 'hunter2'"]);

        let objects: Vec<ObjectRef> = vec![
            (
                ObjectId::new("db".into(), "public".into(), "my_view".into()),
                &view_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_table".into()),
                &table_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_source".into()),
                &source_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_conn".into()),
                &conn_obj,
            ),
            (
                ObjectId::new("db".into(), "public".into(), "my_secret".into()),
                &secret_obj,
            ),
        ];

        let replacement_ids = BTreeSet::new();
        let partitioned = partition_objects(objects, &replacement_ids);

        // Only the view should be staged
        assert_eq!(partitioned.objects.len(), 1);
        assert_eq!(partitioned.objects[0].0.object, "my_view");
        assert_eq!(partitioned.table_count, 4);

        // Collect stage resources
        let (schema_set, cluster_set) =
            collect_stage_resources(&partitioned.objects, &partitioned.replacement_mvs);

        // Should have view's schema
        assert_eq!(schema_set.len(), 1);
        assert!(schema_set.contains(&SchemaQualifier::new("db".into(), "public".into())));

        // Should stage index_cluster (from the view's index), NOT source_cluster
        assert_eq!(
            cluster_set.len(),
            1,
            "Should only have index_cluster, got: {:?}",
            cluster_set
        );
        assert!(
            cluster_set.contains("index_cluster"),
            "Should stage index_cluster from the view's index"
        );
        assert!(
            !cluster_set.contains("source_cluster"),
            "Should NOT stage source_cluster (source is not staged)"
        );
    }

    // =========================================================================
    // Test 3: Incremental deploy, view updated, NOT indexed
    // =========================================================================
    #[test]
    fn test_incremental_deploy_view_updated_not_indexed() {
        // Build planned project with all object types
        let view_obj = make_typed_object(&["CREATE VIEW my_view AS SELECT 1"]);
        let table_obj = make_typed_object(&["CREATE TABLE my_table (id INT)"]);
        let source_obj = make_typed_object(&[
            "CREATE SOURCE my_source IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER",
        ]);
        let conn_obj =
            make_typed_object(&["CREATE CONNECTION my_conn TO KAFKA (BROKER 'localhost:9092')"]);
        let secret_obj = make_typed_object(&["CREATE SECRET my_secret AS 'hunter2'"]);

        let planned_project = make_planned_project(vec![
            ("db", "public", "my_view", view_obj),
            ("db", "storage", "my_table", table_obj),
            ("db", "storage", "my_source", source_obj),
            ("db", "storage", "my_conn", conn_obj),
            ("db", "storage", "my_secret", secret_obj),
        ]);

        // Build new snapshot from planned project
        let new_snapshot = build_snapshot_from_planned(&planned_project).unwrap();

        // Build old snapshot: same hashes for everything EXCEPT the view
        let mut old_snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        for (object_id, hash) in &new_snapshot.objects {
            if object_id.object == "my_view" {
                // Different hash to simulate the view having changed
                old_snapshot
                    .objects
                    .insert(object_id.clone(), "old_hash".to_string());
            } else {
                old_snapshot.objects.insert(object_id.clone(), hash.clone());
            }
        }

        // Compute changeset
        let change_set = ChangeSet::from_deployment_snapshot_comparison(
            &old_snapshot,
            &new_snapshot,
            &planned_project,
        );

        // The view should be in objects_to_deploy
        assert!(
            change_set.objects_to_deploy.contains(&ObjectId::new(
                "db".into(),
                "public".into(),
                "my_view".into()
            )),
            "Changed view should be in objects_to_deploy"
        );

        // Get filtered objects and partition
        let objects = planned_project
            .get_sorted_objects_filtered(&change_set.objects_to_deploy)
            .unwrap();

        let partitioned = partition_objects(objects, &change_set.changed_replacement_objects);

        // Only the view should be staged
        assert_eq!(
            partitioned.objects.len(),
            1,
            "Only the changed view should be staged, got: {:?}",
            partitioned
                .objects
                .iter()
                .map(|(id, _)| &id.object)
                .collect::<Vec<_>>()
        );
        assert_eq!(partitioned.objects[0].0.object, "my_view");

        let (schema_set, cluster_set) =
            collect_stage_resources(&partitioned.objects, &partitioned.replacement_mvs);

        assert_eq!(schema_set.len(), 1);
        assert!(schema_set.contains(&SchemaQualifier::new("db".into(), "public".into())));
        assert!(
            cluster_set.is_empty(),
            "View without index should not require any clusters"
        );
    }

    // =========================================================================
    // Test 4: Incremental deploy, view updated, indexed on different cluster
    // =========================================================================
    #[test]
    fn test_incremental_deploy_view_updated_indexed_different_cluster() {
        // Build planned project with indexed view and other object types
        let view_obj = make_typed_object(&[
            "CREATE VIEW my_view AS SELECT 1",
            "CREATE INDEX my_idx IN CLUSTER index_cluster ON my_view (column1)",
        ]);
        let table_obj = make_typed_object(&["CREATE TABLE my_table (id INT)"]);
        let source_obj = make_typed_object(&[
            "CREATE SOURCE my_source IN CLUSTER source_cluster FROM LOAD GENERATOR COUNTER",
        ]);
        let conn_obj =
            make_typed_object(&["CREATE CONNECTION my_conn TO KAFKA (BROKER 'localhost:9092')"]);
        let secret_obj = make_typed_object(&["CREATE SECRET my_secret AS 'hunter2'"]);

        let planned_project = make_planned_project(vec![
            ("db", "public", "my_view", view_obj),
            ("db", "storage", "my_table", table_obj),
            ("db", "storage", "my_source", source_obj),
            ("db", "storage", "my_conn", conn_obj),
            ("db", "storage", "my_secret", secret_obj),
        ]);

        // Build new snapshot from planned project
        let new_snapshot = build_snapshot_from_planned(&planned_project).unwrap();

        // Build old snapshot: same hashes except the view
        let mut old_snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        for (object_id, hash) in &new_snapshot.objects {
            if object_id.object == "my_view" {
                old_snapshot
                    .objects
                    .insert(object_id.clone(), "old_hash".to_string());
            } else {
                old_snapshot.objects.insert(object_id.clone(), hash.clone());
            }
        }

        // Compute changeset
        let change_set = ChangeSet::from_deployment_snapshot_comparison(
            &old_snapshot,
            &new_snapshot,
            &planned_project,
        );

        assert!(
            change_set.objects_to_deploy.contains(&ObjectId::new(
                "db".into(),
                "public".into(),
                "my_view".into()
            )),
            "Changed view should be in objects_to_deploy"
        );

        // Get filtered objects and partition
        let objects = planned_project
            .get_sorted_objects_filtered(&change_set.objects_to_deploy)
            .unwrap();

        let partitioned = partition_objects(objects, &change_set.changed_replacement_objects);

        // Only the view should be staged
        assert_eq!(
            partitioned.objects.len(),
            1,
            "Only the changed view should be staged, got: {:?}",
            partitioned
                .objects
                .iter()
                .map(|(id, _)| &id.object)
                .collect::<Vec<_>>()
        );
        assert_eq!(partitioned.objects[0].0.object, "my_view");

        let (schema_set, cluster_set) =
            collect_stage_resources(&partitioned.objects, &partitioned.replacement_mvs);

        assert_eq!(schema_set.len(), 1);
        assert!(schema_set.contains(&SchemaQualifier::new("db".into(), "public".into())));

        // Should stage index_cluster only, NOT source_cluster
        assert_eq!(
            cluster_set.len(),
            1,
            "Should only have index_cluster, got: {:?}",
            cluster_set
        );
        assert!(
            cluster_set.contains("index_cluster"),
            "Should stage index_cluster from the view's index"
        );
        assert!(
            !cluster_set.contains("source_cluster"),
            "Should NOT stage source_cluster"
        );
    }

    // =========================================================================
    // Tests for validate_no_new_objects_in_existing_stable_schemas
    // =========================================================================

    fn make_empty_change_set() -> ChangeSet {
        ChangeSet {
            changed_objects: BTreeSet::new(),
            dirty_schemas: BTreeSet::new(),
            dirty_clusters: BTreeSet::new(),
            objects_to_deploy: BTreeSet::new(),
            new_replacement_objects: BTreeSet::new(),
            changed_replacement_objects: BTreeSet::new(),
        }
    }

    #[test]
    fn test_validate_no_new_replacement_objects_first_deploy() {
        let cs = make_empty_change_set();
        let snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[test]
    fn test_validate_new_replacement_objects_in_brand_new_schema() {
        let mut cs = make_empty_change_set();
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "new_mv".into(),
        ));

        // Production has objects in a *different* schema, not analytics
        let mut snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "public".into(), "existing_mv".into()),
            "hash1".into(),
        );

        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[test]
    fn test_validate_new_replacement_objects_in_existing_production_schema() {
        let mut cs = make_empty_change_set();
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "new_mv".into(),
        ));

        // Production already has objects in analytics
        let mut snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "analytics".into(), "existing_mv".into()),
            "hash1".into(),
        );

        let result = validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot);
        assert!(result.is_err());
        match result.unwrap_err() {
            CliError::NewObjectInExistingStableSchema {
                database,
                schema,
                objects,
            } => {
                assert_eq!(database, "db");
                assert_eq!(schema, "analytics");
                assert_eq!(objects, vec!["new_mv"]);
            }
            other => panic!("Expected NewObjectInExistingStableSchema, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_changed_replacement_objects_only() {
        let mut cs = make_empty_change_set();
        // Only changed objects, no new ones
        cs.changed_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "changed_mv".into(),
        ));

        let mut snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "analytics".into(), "changed_mv".into()),
            "hash1".into(),
        );

        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[test]
    fn test_validate_mixed_new_in_new_schema_changed_in_existing() {
        let mut cs = make_empty_change_set();
        // New object in a brand-new schema
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "new_schema".into(),
            "new_mv".into(),
        ));
        // Changed object in an existing schema
        cs.changed_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "existing_schema".into(),
            "changed_mv".into(),
        ));

        // Production has objects only in existing_schema
        let mut snapshot = project::deployment_snapshot::DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "existing_schema".into(), "changed_mv".into()),
            "hash1".into(),
        );

        // Should pass: the new object is in a schema with no production objects
        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }
}
