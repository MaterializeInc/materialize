// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stage command - deploy to staging environment with renamed schemas and clusters.
//!
//! Runs the standard blue/green deployment pipeline that can later be promoted
//! to production via [`super::promote`].

use super::ObjectRef;
use crate::cli::CliError;
use crate::cli::executor::{self, DeploymentExecutor};
use crate::cli::{git, progress};
use crate::client::DeploymentMode;
use crate::client::{Client, ClusterConfig, DeploymentKind, PendingStatement, ReplacementMvRecord};
use crate::config::Settings;
use crate::log;
use crate::project::SchemaQualifier;
use crate::project::analysis::changeset::ChangeSet;
use crate::project::analysis::deployment_snapshot::{self, DeploymentSnapshot};
use crate::project::analysis::deps::extract_external_indexes;
use crate::project::ast::Statement;
use crate::project::ir::compiled::{DatabaseObject, FullyQualifiedName};
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::normalize::{self, NormalizingVisitor};
use crate::verbose;
use mz_ore::option::OptionExt;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{CreateClusterStatement, Ident};
use std::collections::BTreeSet;

/// Reject a stage name long enough that appending the staging suffix
/// `_<stage_name>` to a schema or cluster identifier would exceed the
/// identifier length limit and panic during deploy.
fn validate_stage_name(stage_name: &str) -> Result<(), CliError> {
    // The suffix is appended to existing identifiers, so reserve headroom for
    // the base name rather than letting the suffix consume the whole limit.
    if stage_name.len() + 1 > Ident::MAX_LENGTH / 2 {
        return Err(CliError::InvalidEnvironmentName {
            name: stage_name.to_string(),
        });
    }
    Ok(())
}
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

/// Summary returned after a successful stage run, used for terminal output
/// and `--json`.
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

/// Deploy the project to a staging environment.
///
/// Creates renamed schemas and clusters alongside production, deploys the
/// project onto them, and records metadata so a later `apply` can atomically
/// swap staging into production.
///
/// # Arguments
/// * `settings` - Resolved CLI settings (project directory, profile, etc.)
/// * `stage_name` - Optional environment name; defaults to a git-derived or
///   random identifier
/// * `allow_dirty` - Allow deploying with uncommitted changes
/// * `no_rollback` - Skip automatic rollback on failure (for debugging)
/// * `dry_run` - Print SQL instead of executing it
///
/// # Returns
/// `Ok(())` if the deployment succeeds.
///
/// # Errors
/// Surfaces `CliError` variants from git checks, project compilation, and
/// database execution.
pub async fn run(
    settings: &Settings,
    stage_name: Option<&str>,
    allow_dirty: bool,
    no_rollback: bool,
    dry_run: bool,
    redeploy_schemas: &[String],
    redeploy_all: bool,
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
    validate_stage_name(&stage_name)?;

    let planned_project = super::compile::run(settings, true).await?;
    let staging_suffix = format!("_{}", stage_name);

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    crate::cli::commands::setup::verify(&client, settings.emulator()).await?;
    let role =
        crate::cli::commands::setup::validate_connection(&client, settings.emulator()).await?;
    crate::cli::commands::setup::require_deployer(role)?;

    let forced_dirty_schemas = resolve_redeploy_schemas(&planned_project, redeploy_schemas)?;

    let Some(analysis) = analyze_project_changes(
        &client,
        &planned_project,
        &stage_name,
        forced_dirty_schemas,
        redeploy_all,
    )
    .await?
    else {
        return Ok(());
    };

    validate_project_for_stage(
        &client,
        &planned_project,
        directory,
        &analysis.schema_set,
        &analysis.cluster_set,
    )
    .await?;

    if !dry_run {
        // Metadata is written before any resources are created, so a failure
        // partway through can leave deployment rows behind that block
        // re-staging under the same name. Roll them back on failure, unless
        // --no-rollback asks to preserve state for debugging. The rollback runs
        // a suffix-matching CASCADE drop, so honoring the flag here also avoids
        // dropping resources the operator asked to keep.
        if let Err(e) = record_stage_metadata(
            &client,
            directory,
            &stage_name,
            &staging_suffix,
            &analysis.objects,
            &analysis.sinks,
            &analysis.replacement_mvs,
            &planned_project.replacement_schemas,
        )
        .await
        {
            if no_rollback {
                progress::error("Deployment failed (skipping rollback due to --no-rollback flag)");
            } else {
                progress::error("Deployment failed, rolling back...");
                rollback_staging_resources(&client, &stage_name).await;
            }
            return Err(e);
        }
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
                    database: id.expect_database().to_string(),
                    schema: id.schema().to_string(),
                    object: id.object().to_string(),
                })
                .collect(),
            sinks: analysis
                .sinks
                .iter()
                .map(|(id, _)| StagePlanObject {
                    database: id.expect_database().to_string(),
                    schema: id.schema().to_string(),
                    object: id.object().to_string(),
                })
                .collect(),
            replacement_mvs: analysis
                .replacement_mvs
                .iter()
                .map(|(id, _)| StagePlanObject {
                    database: id.expect_database().to_string(),
                    schema: id.schema().to_string(),
                    object: id.object().to_string(),
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
    log::print_deploy_id(&stage_name);
    Ok(())
}

/// Parse one `--redeploy-schema` value, which must be fully qualified as
/// `database.schema`. Reuses the SQL parser so reserved-word components quote
/// correctly.
fn parse_qualified_schema(raw: &str) -> Result<SchemaQualifier, CliError> {
    let unqualified = || {
        CliError::Message(format!(
            "invalid --redeploy-schema '{}': expected a qualified 'database.schema' name",
            raw
        ))
    };
    let name = mz_sql_parser::parser::parse_item_name(raw).map_err(|_| unqualified())?;
    let [database, schema] = name.0.as_slice() else {
        return Err(unqualified());
    };
    Ok(SchemaQualifier::new(
        database.as_str().to_string(),
        schema.as_str().to_string(),
    ))
}

/// Resolve `--redeploy-schema` values into `SchemaQualifier`s, validating each
/// against the project's schemas.
fn resolve_redeploy_schemas(
    planned_project: &Project,
    redeploy_schemas: &[String],
) -> Result<BTreeSet<SchemaQualifier>, CliError> {
    if redeploy_schemas.is_empty() {
        return Ok(BTreeSet::new());
    }

    let objects: Vec<_> = planned_project.iter_objects().collect();
    let project_schemas = SchemaQualifier::collect_from(&objects);

    let mut resolved = BTreeSet::new();
    for raw in redeploy_schemas {
        let sq = parse_qualified_schema(raw)?;
        if !project_schemas.contains(&sq) {
            let available = project_schemas
                .iter()
                .map(|s| format!("{}.{}", s.database, s.schema))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(CliError::Message(format!(
                "--redeploy-schema '{}.{}' is not a schema in this project; available: {}",
                sq.database, sq.schema, available
            )));
        }
        resolved.insert(sq);
    }
    Ok(resolved)
}

/// Produces the stage deployment plan by diffing against current production snapshot.
///
/// Handles incremental-vs-full mode, applies stage-specific object filtering,
/// validates table dependencies, and returns resource sets required for execution.
async fn analyze_project_changes<'a>(
    client: &Client,
    planned_project: &'a Project,
    stage_name: &str,
    forced_dirty_schemas: BTreeSet<SchemaQualifier>,
    redeploy_all: bool,
) -> Result<Option<StageAnalysis<'a>>, CliError> {
    progress::stage_start("Analyzing project changes");
    let analyze_start = Instant::now();

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

    let new_snapshot = deployment_snapshot::build_snapshot_from_planned(planned_project)?;
    let production_snapshot = deployment_snapshot::load_from_database(client, None).await?;

    let dirty_schemas = if redeploy_all {
        new_snapshot.schemas.keys().cloned().collect()
    } else {
        forced_dirty_schemas
    };

    // A first deploy has no production objects to diff against. An empty old
    // snapshot makes every object read as added, so the fixed point handles it
    // without a separate full-deploy path, but the stable-schema check below
    // only applies once production exists.
    let first_deploy = production_snapshot.objects.is_empty();

    let change_set = ChangeSet::compute(
        client,
        planned_project,
        &production_snapshot,
        &new_snapshot,
        &dirty_schemas,
    )
    .await?;

    // Reject adding brand-new objects to a schema that already has production
    // objects. A new MV in a stable schema is classified as a new replacement
    // object and deploys by blue-green swap, but the changed MVs alongside it
    // set the production schema's metadata to `DeploymentKind::Replacement`.
    // Promote then skips that schema from the swap and drops the staging schema
    // CASCADE, losing the new MV.
    //
    // The fix is `ALTER MATERIALIZED VIEW ... SET SCHEMA`: new MVs could deploy
    // to the staging schema alongside changed ones and be relocated on promote
    // rather than swapped, which removes the need for mixed deployment kinds.
    //
    // A brand-new stable schema deploys fine through the normal swap, so only
    // schemas with existing production objects are rejected.
    if !first_deploy {
        validate_no_new_objects_in_existing_stable_schemas(&change_set, &production_snapshot)?;
    }

    if !first_deploy && change_set.is_empty() {
        progress::success("No changes detected compared to production, skipping deployment");
        return Ok(None);
    }

    if first_deploy {
        verbose!("Full deployment: no production deployment found");
    }
    verbose!("{}", change_set);
    let analysis = partition_objects(planned_project, &change_set)?;
    log_partition_summary(&analysis, change_set.apply_managed_count);

    let object_ids: BTreeSet<_> = analysis.objects.iter().map(|(id, _)| id.clone()).collect();
    client
        .validation()
        .validate_table_dependencies(planned_project, &object_ids)
        .await?;

    let analyze_duration = analyze_start.elapsed();
    progress::stage_success(
        &format!(
            "Ready to deploy {} view(s)/materialized view(s)",
            analysis.objects.len()
        ),
        analyze_duration,
    );

    Ok(Some(analysis))
}

/// Splits the objects the change set routes to stage into execution buckets,
/// preserving the project's topological deployment order.
///
/// Membership comes from the fixed point; ordering comes from the project.
/// An id the change set names but the project does not contain is a deleted
/// object with no statement to deploy, and the sort drops it.
fn partition_objects<'a>(
    planned_project: &'a Project,
    change_set: &ChangeSet,
) -> Result<StageAnalysis<'a>, CliError> {
    let mut deployable: BTreeSet<ObjectId> = change_set.stage_objects.clone();
    deployable.extend(change_set.stage_sinks.iter().cloned());
    deployable.extend(change_set.stage_replacement_mvs.iter().cloned());

    let mut objects = Vec::new();
    let mut sinks = Vec::new();
    let mut replacement_mvs = Vec::new();

    for (object_id, typed_obj) in planned_project.get_sorted_objects_filtered(&deployable)? {
        if change_set.stage_sinks.contains(&object_id) {
            sinks.push((object_id, typed_obj));
        } else if change_set.stage_replacement_mvs.contains(&object_id) {
            replacement_mvs.push((object_id, typed_obj));
        } else {
            objects.push((object_id, typed_obj));
        }
    }

    Ok(StageAnalysis {
        objects,
        sinks,
        replacement_mvs,
        schema_set: change_set.schemas_to_create.clone(),
        cluster_set: change_set.clusters_to_create.clone(),
    })
}

/// Reports the partitioning decisions visible to users in verbose mode.
fn log_partition_summary(analysis: &StageAnalysis<'_>, apply_managed_count: usize) {
    if apply_managed_count > 0 {
        verbose!(
            "Skipped {} table(s)/source(s) - use 'mz-deploy apply' for those",
            apply_managed_count
        );
    }
    if !analysis.sinks.is_empty() {
        verbose!(
            "Found {} sink(s) - will be created during apply after swap",
            analysis.sinks.len()
        );
    }
    if !analysis.replacement_mvs.is_empty() {
        verbose!(
            "Found {} replacement MV(s) - will use CREATE REPLACEMENT protocol",
            analysis.replacement_mvs.len()
        );
    }
}

/// Runs all preflight database validations required before mutating deployment state.
///
/// This is intentionally isolated so stage fails before any metadata/resource writes.
async fn validate_project_for_stage(
    client: &Client,
    planned_project: &Project,
    directory: &Path,
    schema_set: &BTreeSet<SchemaQualifier>,
    cluster_set: &BTreeSet<String>,
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
        .validate_schema_ownership(schema_set)
        .await?;
    client
        .validation()
        .validate_cluster_ownership(cluster_set)
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
    replacement_schemas: &BTreeSet<SchemaQualifier>,
) -> Result<(), CliError> {
    progress::stage_start("Recording deployment metadata");
    let metadata_start = Instant::now();
    let metadata = executor::collect_deployment_metadata(client, directory).await;

    let mut staging_snapshot = DeploymentSnapshot::default();

    for (object_id, typed_obj) in objects {
        let hash = deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot.schemas.insert(
            SchemaQualifier::new(
                object_id.expect_database().to_string(),
                object_id.schema().to_string(),
            ),
            DeploymentKind::Objects,
        );
    }

    for (object_id, typed_obj) in sinks {
        let hash = deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot
            .schemas
            .entry(SchemaQualifier::new(
                object_id.expect_database().to_string(),
                object_id.schema().to_string(),
            ))
            .or_insert(DeploymentKind::Sinks);
    }

    for (object_id, typed_obj) in replacement_mvs {
        let hash = deployment_snapshot::compute_typed_hash(typed_obj);
        staging_snapshot.objects.insert(object_id.clone(), hash);
        staging_snapshot.schemas.insert(
            SchemaQualifier::new(
                object_id.expect_database().to_string(),
                object_id.schema().to_string(),
            ),
            DeploymentKind::Replacement,
        );
    }

    // Ensure replacement schemas record the correct kind.
    // During Objects→Replacement transitions, MVs go through the regular objects
    // path (for blue-green swap), but the metadata must reflect the final kind
    // so future deploys know to use CREATE REPLACEMENT.
    for sq in replacement_schemas {
        if staging_snapshot.schemas.contains_key(sq) {
            staging_snapshot
                .schemas
                .insert(sq.clone(), DeploymentKind::Replacement);
        }
    }

    deployment_snapshot::write_to_database(
        client,
        &staging_snapshot,
        stage_name,
        &metadata,
        None,
        DeploymentMode::Stage,
    )
    .await?;

    if !sinks.is_empty() {
        let pending_statements: Vec<PendingStatement> = sinks
            .iter()
            .enumerate()
            .map(|(idx, (object_id, typed_obj))| {
                let original_fqn: FullyQualifiedName = object_id.clone().into();
                let mut visitor = NormalizingVisitor::fully_qualifying(&original_fqn);
                let stmt = typed_obj
                    .stmt
                    .clone()
                    .normalize_name_with(&visitor, &original_fqn.to_item_name())
                    .normalize_dependencies_with(&mut visitor);
                let hash = deployment_snapshot::compute_typed_hash(typed_obj);
                #[allow(clippy::as_conversions)]
                PendingStatement {
                    deploy_id: stage_name.to_string(),
                    sequence_num: idx as i32,
                    database: object_id.expect_database().to_string(),
                    schema: object_id.schema().to_string(),
                    object: object_id.object().to_string(),
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
                target_database: object_id.expect_database().to_string(),
                target_schema: object_id.schema().to_string(),
                target_name: object_id.object().to_string(),
                replacement_schema: format!("{}{}", object_id.schema(), staging_suffix),
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
    client: &Client,
    stage_name: &str,
    staging_suffix: &str,
    schema_set: &BTreeSet<SchemaQualifier>,
    cluster_set: &BTreeSet<String>,
    planned_project: &'a Project,
    objects: &'a [(ObjectId, &'a DatabaseObject)],
    replacement_mvs: &'a [(ObjectId, &'a DatabaseObject)],
    no_rollback: bool,
    dry_run: bool,
) -> Result<usize, CliError> {
    let executor = DeploymentExecutor::with_dry_run(client, dry_run);

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
                progress::success(&format!(
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
    executor: &DeploymentExecutor<'_>,
    planned_project: &Project,
    schema_set: &BTreeSet<SchemaQualifier>,
    staging_suffix: &str,
) -> Result<(), CliError> {
    // Create project databases that aren't in schema_set
    // (schema_set databases will be created by prepare_databases_and_schemas)
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
    executor: &DeploymentExecutor<'_>,
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
                create_stmt: CreateClusterStatement {
                    name: Ident::new_unchecked(""),
                    options: Vec::new(),
                    features: Vec::new(),
                    if_not_exists: false,
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
        ClusterConfig::Managed {
            create_stmt,
            grants,
        } => {
            verbose!(
                "  Created managed cluster '{}' ({}, {} grant(s), cloned from '{}')",
                staging_cluster,
                create_stmt.to_ast_string_simple(),
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
    executor: &DeploymentExecutor<'_>,
    objects: &'a [(ObjectId, &'a DatabaseObject)],
    replacement_mvs: &'a [(ObjectId, &'a DatabaseObject)],
    planned_project: &'a Project,
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
    normalize::transform_cluster_names_for_staging(&mut external_indexes, staging_suffix);
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
            object_id.object(),
            staging_suffix,
            object_id.schema(),
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
            "Applying replacement MV {}/{}: {} FOR {}",
            idx + 1,
            replacement_mvs.len(),
            object_id.object(),
            object_id
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
async fn rollback_staging_resources(client: &Client, environment: &str) -> (usize, usize) {
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
fn best_effort_fetch<T, E: fmt::Display>(result: Result<Vec<T>, E>, action: &str) -> Vec<T> {
    match result {
        Ok(values) => values,
        Err(e) => {
            verbose!("Warning: Failed to {}: {}", action, e);
            vec![]
        }
    }
}

/// Best-effort delete wrapper used by rollback metadata cleanup.
fn best_effort_delete<E: fmt::Display>(result: Result<(), E>, action: &str) {
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
    executor: &DeploymentExecutor<'_>,
    object_id: &ObjectId,
    typed_obj: &DatabaseObject,
    staging_suffix: &str,
    planned_project: &Project,
    objects_to_deploy_set: &BTreeSet<ObjectId>,
    replacement_objects: &BTreeSet<ObjectId>,
    transform: impl FnOnce(Statement) -> Statement,
) -> Result<(), CliError> {
    let original_fqn: FullyQualifiedName = object_id.clone().into();

    let mut visitor = NormalizingVisitor::staging(
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
        .normalize_dependencies_with(&mut visitor)
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
    production_snapshot: &DeploymentSnapshot,
) -> Result<(), CliError> {
    let blocked: Vec<_> = change_set
        .new_replacement_objects
        .iter()
        .filter(|obj| {
            !production_snapshot.objects.contains_key(obj)
                && production_snapshot
                    .objects
                    .keys()
                    .any(|prod| prod.database() == obj.database() && prod.schema() == obj.schema())
        })
        .collect();

    if blocked.is_empty() {
        return Ok(());
    }

    let first = blocked[0];
    Err(CliError::NewObjectInExistingStableSchema {
        database: first.expect_database().to_string(),
        schema: first.schema().to_string(),
        objects: blocked.iter().map(|o| o.object().to_string()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::analysis::deployment_snapshot::build_snapshot_from_planned;
    use crate::project::ir::compiled;
    use crate::project::ir::object_id::ObjectId;
    use std::collections::{BTreeMap, BTreeSet};

    #[mz_ore::test]
    fn parse_qualified_schema_requires_two_parts() {
        // Fully qualified parses to (database, schema).
        let sq = parse_qualified_schema("app.core").expect("qualified name parses");
        assert_eq!(
            sq,
            SchemaQualifier::new("app".to_string(), "core".to_string())
        );

        // A reserved-word component is handled via the SQL parser.
        let sq = parse_qualified_schema("app.\"select\"").expect("quoted keyword parses");
        assert_eq!(
            sq,
            SchemaQualifier::new("app".to_string(), "select".to_string())
        );

        // Unqualified (1-part) and over-qualified (3-part) are rejected.
        assert!(parse_qualified_schema("core").is_err());
        assert!(parse_qualified_schema("app.core.orders").is_err());
    }

    /// Parse SQL strings into a compiled::DatabaseObject.
    ///
    /// The first CREATE statement becomes the main statement.
    /// Any CREATE INDEX statements become entries in the indexes vec.
    fn make_typed_object(sqls: &[&str]) -> DatabaseObject {
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

        DatabaseObject {
            path: std::path::PathBuf::from("test.sql"),
            stmt: stmt.expect("Expected at least one CREATE statement"),
            indexes,
            grants: vec![],
            comments: vec![],
            tests: vec![],
        }
    }

    /// Build a graph::Project from a list of (database, schema, object_name, typed_obj) tuples.
    fn make_planned_project(objects: Vec<(&str, &str, &str, DatabaseObject)>) -> Project {
        // Group into databases -> schemas -> objects
        let mut db_map: BTreeMap<String, BTreeMap<String, Vec<DatabaseObject>>> = BTreeMap::new();

        for (database, schema, _name, typed_obj) in objects {
            db_map
                .entry(database.to_string())
                .or_default()
                .entry(schema.to_string())
                .or_default()
                .push(typed_obj);
        }

        let databases: Vec<compiled::Database> = db_map
            .into_iter()
            .map(|(db_name, schemas)| compiled::Database {
                name: db_name,
                schemas: schemas
                    .into_iter()
                    .map(|(schema_name, objs)| compiled::Schema {
                        name: schema_name,
                        objects: objs,
                        mod_statements: None,
                    })
                    .collect(),
                mod_statements: None,
            })
            .collect();

        let typed_project = compiled::Project {
            databases,
            replacement_schemas: BTreeSet::new(),
        };

        Project::from(typed_project)
    }

    fn make_empty_change_set() -> ChangeSet {
        ChangeSet::default()
    }

    #[mz_ore::test]
    fn test_validate_no_new_replacement_objects_first_deploy() {
        let cs = make_empty_change_set();
        let snapshot = DeploymentSnapshot::default();
        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[mz_ore::test]
    fn test_validate_new_replacement_objects_in_brand_new_schema() {
        let mut cs = make_empty_change_set();
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "new_mv".into(),
        ));

        // Production has objects in a *different* schema, not analytics
        let mut snapshot = DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "public".into(), "existing_mv".into()),
            "hash1".into(),
        );

        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[mz_ore::test]
    fn test_validate_new_replacement_objects_in_existing_production_schema() {
        let mut cs = make_empty_change_set();
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "new_mv".into(),
        ));

        // Production already has objects in analytics
        let mut snapshot = DeploymentSnapshot::default();
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

    #[mz_ore::test]
    fn test_validate_changed_replacement_objects_only() {
        let mut cs = make_empty_change_set();
        // Only changed objects, no new ones
        cs.stage_replacement_mvs.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "changed_mv".into(),
        ));

        let mut snapshot = DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "analytics".into(), "changed_mv".into()),
            "hash1".into(),
        );

        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[mz_ore::test]
    fn test_validate_mixed_new_in_new_schema_changed_in_existing() {
        let mut cs = make_empty_change_set();
        // New object in a brand-new schema
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "new_schema".into(),
            "new_mv".into(),
        ));
        // Changed object in an existing schema
        cs.stage_replacement_mvs.insert(ObjectId::new(
            "db".into(),
            "existing_schema".into(),
            "changed_mv".into(),
        ));

        // Production has objects only in existing_schema
        let mut snapshot = DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "existing_schema".into(), "changed_mv".into()),
            "hash1".into(),
        );

        // Should pass: the new object is in a schema with no production objects
        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    #[mz_ore::test]
    fn test_validate_transitioning_objects_in_existing_schema_allowed() {
        let mut cs = make_empty_change_set();
        // Object transitioning from Objects→Replacement lands in new_replacement_objects
        cs.new_replacement_objects.insert(ObjectId::new(
            "db".into(),
            "analytics".into(),
            "existing_mv".into(),
        ));

        // The same object already exists in production (it's transitioning, not new)
        let mut snapshot = DeploymentSnapshot::default();
        snapshot.objects.insert(
            ObjectId::new("db".into(), "analytics".into(), "existing_mv".into()),
            "hash1".into(),
        );

        // Should pass: the object already exists in production, it's just changing schema kind
        assert!(validate_no_new_objects_in_existing_stable_schemas(&cs, &snapshot).is_ok());
    }

    fn make_planned_project_with_replacement_schemas(
        objects: Vec<(&str, &str, &str, DatabaseObject)>,
        replacement_schemas: BTreeSet<SchemaQualifier>,
    ) -> Project {
        let mut db_map: BTreeMap<String, BTreeMap<String, Vec<DatabaseObject>>> = BTreeMap::new();

        for (database, schema, _name, typed_obj) in objects {
            db_map
                .entry(database.to_string())
                .or_default()
                .entry(schema.to_string())
                .or_default()
                .push(typed_obj);
        }

        let databases: Vec<compiled::Database> = db_map
            .into_iter()
            .map(|(db_name, schemas)| compiled::Database {
                name: db_name,
                schemas: schemas
                    .into_iter()
                    .map(|(schema_name, objs)| compiled::Schema {
                        name: schema_name,
                        objects: objs,
                        mod_statements: None,
                    })
                    .collect(),
                mod_statements: None,
            })
            .collect();

        let typed_project = compiled::Project {
            databases,
            replacement_schemas,
        };

        Project::from(typed_project)
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn test_build_snapshot_replacement_schema_kind() {
        let mv_obj =
            make_typed_object(&["CREATE MATERIALIZED VIEW my_mv IN CLUSTER compute AS SELECT 1"]);
        let view_obj = make_typed_object(&["CREATE VIEW my_view AS SELECT 1"]);

        let mut replacement_schemas = BTreeSet::new();
        replacement_schemas.insert(SchemaQualifier::new("db".into(), "stable".into()));

        let planned_project = make_planned_project_with_replacement_schemas(
            vec![
                ("db", "stable", "my_mv", mv_obj),
                ("db", "regular", "my_view", view_obj),
            ],
            replacement_schemas,
        );

        let snapshot = build_snapshot_from_planned(&planned_project).unwrap();

        // The stable schema should be Replacement
        assert_eq!(
            snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "stable".into())),
            Some(&DeploymentKind::Replacement),
            "Replacement schema should have Replacement kind in snapshot"
        );

        // The regular schema should be Objects
        assert_eq!(
            snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "regular".into())),
            Some(&DeploymentKind::Objects),
            "Regular schema should have Objects kind in snapshot"
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn test_build_snapshot_no_replacement_schemas_all_objects() {
        let mv_obj =
            make_typed_object(&["CREATE MATERIALIZED VIEW my_mv IN CLUSTER compute AS SELECT 1"]);
        let view_obj = make_typed_object(&["CREATE VIEW my_view AS SELECT 1"]);

        let planned_project = make_planned_project(vec![
            ("db", "stable", "my_mv", mv_obj),
            ("db", "regular", "my_view", view_obj),
        ]);

        let snapshot = build_snapshot_from_planned(&planned_project).unwrap();

        // Both should be Objects when no replacement_schemas configured
        assert_eq!(
            snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "stable".into())),
            Some(&DeploymentKind::Objects),
        );
        assert_eq!(
            snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "regular".into())),
            Some(&DeploymentKind::Objects),
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn test_record_stage_metadata_transition_override() {
        // During an Objects→Replacement transition, MVs go through the regular
        // objects path (not replacement_mvs), but the metadata must still record
        // the schema as Replacement.
        let mv_obj =
            make_typed_object(&["CREATE MATERIALIZED VIEW my_mv IN CLUSTER compute AS SELECT 1"]);

        // Objects path (transition — MV is NOT in replacement_mvs)
        let objects: Vec<ObjectRef> = vec![(
            ObjectId::new("db".into(), "stable".into(), "my_mv".into()),
            &mv_obj,
        )];
        let sinks: Vec<ObjectRef> = vec![];
        let replacement_mvs: Vec<ObjectRef> = vec![];

        // The project declares "stable" as a replacement schema
        let mut replacement_schemas = BTreeSet::new();
        replacement_schemas.insert(SchemaQualifier::new("db".into(), "stable".into()));

        // Simulate what record_stage_metadata does (without DB calls)
        let mut staging_snapshot = DeploymentSnapshot::default();

        for (object_id, typed_obj) in &objects {
            let hash = deployment_snapshot::compute_typed_hash(typed_obj);
            staging_snapshot.objects.insert(object_id.clone(), hash);
            staging_snapshot.schemas.insert(
                SchemaQualifier::new(
                    object_id.expect_database().to_string(),
                    object_id.schema().to_string(),
                ),
                DeploymentKind::Objects,
            );
        }

        for (object_id, typed_obj) in &sinks {
            let hash = deployment_snapshot::compute_typed_hash(typed_obj);
            staging_snapshot.objects.insert(object_id.clone(), hash);
            staging_snapshot
                .schemas
                .entry(SchemaQualifier::new(
                    object_id.expect_database().to_string(),
                    object_id.schema().to_string(),
                ))
                .or_insert(DeploymentKind::Sinks);
        }

        for (object_id, typed_obj) in &replacement_mvs {
            let hash = deployment_snapshot::compute_typed_hash(typed_obj);
            staging_snapshot.objects.insert(object_id.clone(), hash);
            staging_snapshot.schemas.insert(
                SchemaQualifier::new(
                    object_id.expect_database().to_string(),
                    object_id.schema().to_string(),
                ),
                DeploymentKind::Replacement,
            );
        }

        // Before the fix, the schema would remain Objects here.
        assert_eq!(
            staging_snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "stable".into())),
            Some(&DeploymentKind::Objects),
            "Before override, schema should be Objects (from regular objects path)"
        );

        // Apply the replacement_schemas override (the fix)
        for sq in &replacement_schemas {
            if staging_snapshot.schemas.contains_key(sq) {
                staging_snapshot
                    .schemas
                    .insert(sq.clone(), DeploymentKind::Replacement);
            }
        }

        // After the fix, the schema should be Replacement
        assert_eq!(
            staging_snapshot
                .schemas
                .get(&SchemaQualifier::new("db".into(), "stable".into())),
            Some(&DeploymentKind::Replacement),
            "After override, schema should be Replacement"
        );
    }

    #[mz_ore::test]
    fn test_record_stage_metadata_override_only_applies_to_existing_schemas() {
        // The override should NOT create new schema entries — it only applies to
        // schemas that already have objects in the staging snapshot.
        let replacement_schemas =
            BTreeSet::from([SchemaQualifier::new("db".into(), "nonexistent".into())]);

        let mut staging_snapshot = DeploymentSnapshot::default();

        // Apply the replacement_schemas override
        for sq in &replacement_schemas {
            if staging_snapshot.schemas.contains_key(sq) {
                staging_snapshot
                    .schemas
                    .insert(sq.clone(), DeploymentKind::Replacement);
            }
        }

        // Should NOT have created a new entry
        assert!(
            staging_snapshot.schemas.is_empty(),
            "Override should not create entries for schemas with no objects"
        );
    }

    #[mz_ore::test]
    fn test_validate_stage_name_length() {
        assert!(validate_stage_name("prod").is_ok());
        assert!(validate_stage_name(&"a".repeat(Ident::MAX_LENGTH / 2 - 1)).is_ok());
        assert!(validate_stage_name(&"a".repeat(Ident::MAX_LENGTH)).is_err());
    }
}
