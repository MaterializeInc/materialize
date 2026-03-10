//! Deploy command - promote staging deployment to production via ALTER SWAP.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::{
    ApplyState, Client, DependentSink, DeploymentKind, PendingStatement, ReplacementMvRecord,
};
use crate::config::Settings;
use crate::humanln;
use crate::output;
use crate::project::SchemaQualifier;
use crate::project::object_id::ObjectId;
use crate::{project, verbose};
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;
use std::fmt;

/// Central data structure holding everything needed to display and execute a deployment.
///
/// Computed once by `generate_deployment_plan()` and consumed for display (human + JSON)
/// and execution. Raw domain objects are stored so execution functions don't re-query.
struct DeploymentPlan {
    deploy_id: String,
    apply_state: ApplyState,
    staging_suffix: String,
    staging_schemas: BTreeSet<SchemaQualifier>,
    staging_clusters: BTreeSet<String>,
    pending_statements: Vec<PendingStatement>,
    replacement_mvs: Vec<ReplacementMvRecord>,
    dependent_sinks: Vec<DependentSink>,
}

// ---------------------------------------------------------------------------
// View structs for JSON serialization
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct SchemaSwapView<'a> {
    database: &'a str,
    production_schema: String,
    staging_schema: &'a str,
}

#[derive(serde::Serialize)]
struct ClusterSwapView<'a> {
    production_cluster: &'a str,
    staging_cluster: String,
}

#[derive(serde::Serialize)]
struct SinkToCreateView<'a> {
    database: &'a str,
    schema: &'a str,
    object: &'a str,
}

#[derive(serde::Serialize)]
struct ReplacementMvView<'a> {
    target_database: &'a str,
    target_schema: &'a str,
    target_name: &'a str,
    replacement_schema: &'a str,
}

#[derive(serde::Serialize)]
struct SinkToRepointView<'a> {
    sink_database: &'a str,
    sink_schema: &'a str,
    sink_name: &'a str,
    dependency_database: &'a str,
    dependency_schema: &'a str,
    dependency_name: &'a str,
}

#[derive(serde::Serialize)]
struct ResourceToDropView {
    kind: String,
    name: String,
}

// ---------------------------------------------------------------------------
// DeploymentPlan helper methods
// ---------------------------------------------------------------------------

impl DeploymentPlan {
    fn apply_state_str(&self) -> &'static str {
        match self.apply_state {
            ApplyState::NotStarted => "not_started",
            ApplyState::PreSwap => "pre_swap",
            ApplyState::PostSwap => "post_swap",
        }
    }

    fn schema_swaps(&self) -> Vec<SchemaSwapView<'_>> {
        self.staging_schemas
            .iter()
            .map(|sq| {
                let prod_schema = sq.schema.trim_end_matches(&self.staging_suffix).to_string();
                SchemaSwapView {
                    database: &sq.database,
                    production_schema: prod_schema,
                    staging_schema: &sq.schema,
                }
            })
            .collect()
    }

    fn cluster_swaps(&self) -> Vec<ClusterSwapView<'_>> {
        self.staging_clusters
            .iter()
            .map(|cluster| ClusterSwapView {
                production_cluster: cluster,
                staging_cluster: format!("{}{}", cluster, self.staging_suffix),
            })
            .collect()
    }

    fn sinks_to_create(&self) -> Vec<SinkToCreateView<'_>> {
        self.pending_statements
            .iter()
            .map(|stmt| SinkToCreateView {
                database: &stmt.database,
                schema: &stmt.schema,
                object: &stmt.object,
            })
            .collect()
    }

    fn replacement_mv_views(&self) -> Vec<ReplacementMvView<'_>> {
        self.replacement_mvs
            .iter()
            .map(|r| ReplacementMvView {
                target_database: &r.target_database,
                target_schema: &r.target_schema,
                target_name: &r.target_name,
                replacement_schema: &r.replacement_schema,
            })
            .collect()
    }

    fn sinks_to_repoint(&self) -> Vec<SinkToRepointView<'_>> {
        self.dependent_sinks
            .iter()
            .map(|sink| SinkToRepointView {
                sink_database: &sink.sink_database,
                sink_schema: &sink.sink_schema,
                sink_name: &sink.sink_name,
                dependency_database: &sink.dependency_database,
                dependency_schema: &sink.dependency_schema,
                dependency_name: &sink.dependency_name,
            })
            .collect()
    }

    fn replacement_schemas(&self) -> BTreeSet<SchemaQualifier> {
        self.replacement_mvs
            .iter()
            .map(|r| SchemaQualifier::new(r.target_database.clone(), r.replacement_schema.clone()))
            .collect()
    }

    fn resources_to_drop(&self) -> Vec<ResourceToDropView> {
        let mut drops = Vec::new();
        for sq in &self.staging_schemas {
            let prod_schema = sq.schema.trim_end_matches(&self.staging_suffix);
            let old_schema = format!("{}{}", prod_schema, self.staging_suffix);
            drops.push(ResourceToDropView {
                kind: "schema".to_string(),
                name: format!("{}.{}", sq.database, old_schema),
            });
        }
        for cluster in &self.staging_clusters {
            let old_cluster = format!("{}{}", cluster, self.staging_suffix);
            drops.push(ResourceToDropView {
                kind: "cluster".to_string(),
                name: old_cluster,
            });
        }
        for sq in &self.replacement_schemas() {
            drops.push(ResourceToDropView {
                kind: "schema".to_string(),
                name: format!("{}.{}", sq.database, sq.schema),
            });
        }
        drops
    }

    fn has_work(&self) -> bool {
        !self.staging_schemas.is_empty()
            || !self.staging_clusters.is_empty()
            || !self.pending_statements.is_empty()
            || !self.replacement_mvs.is_empty()
            || !self.dependent_sinks.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Serialize impl — produces the exact same JSON shape as old DeploymentPlanJson
// ---------------------------------------------------------------------------

impl serde::Serialize for DeploymentPlan {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("DeploymentPlan", 8)?;
        state.serialize_field("deploy_id", &self.deploy_id)?;
        state.serialize_field("apply_state", self.apply_state_str())?;
        state.serialize_field("schema_swaps", &self.schema_swaps())?;
        state.serialize_field("cluster_swaps", &self.cluster_swaps())?;
        state.serialize_field("sinks_to_create", &self.sinks_to_create())?;
        state.serialize_field("replacement_mvs", &self.replacement_mv_views())?;
        state.serialize_field("sinks_to_repoint", &self.sinks_to_repoint())?;
        state.serialize_field("resources_to_drop", &self.resources_to_drop())?;
        state.end()
    }
}

// ---------------------------------------------------------------------------
// Display impl — human-readable deployment plan
// ---------------------------------------------------------------------------

impl fmt::Display for DeploymentPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Note resume state if applicable
        match self.apply_state {
            ApplyState::PreSwap => {
                writeln!(
                    f,
                    "\n  {} Resuming from pre-swap state (apply state schemas already created)",
                    "note:".yellow().bold()
                )?;
            }
            ApplyState::PostSwap => {
                writeln!(
                    f,
                    "\n  {} Resuming from post-swap state (swap already completed, showing remaining work)",
                    "note:".yellow().bold()
                )?;
            }
            ApplyState::NotStarted => {}
        }

        // Schema Swaps
        writeln!(f, "\n  {}", "Schema Swaps:".bold())?;
        if self.staging_schemas.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for swap in &self.schema_swaps() {
                writeln!(
                    f,
                    "    {} {}.{} {} {}.{}",
                    "~".yellow(),
                    swap.database,
                    swap.production_schema,
                    "<->".dimmed(),
                    swap.database,
                    swap.staging_schema
                )?;
            }
        }

        // Cluster Swaps
        writeln!(f, "\n  {}", "Cluster Swaps:".bold())?;
        if self.staging_clusters.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for swap in &self.cluster_swaps() {
                writeln!(
                    f,
                    "    {} {} {} {}",
                    "~".yellow(),
                    swap.production_cluster,
                    "<->".dimmed(),
                    swap.staging_cluster
                )?;
            }
        }

        // Sinks to Create
        writeln!(f, "\n  {}", "Sinks to Create:".bold())?;
        if self.pending_statements.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for stmt in &self.pending_statements {
                writeln!(
                    f,
                    "    {} {}.{}.{}",
                    "+".green(),
                    stmt.database,
                    stmt.schema,
                    stmt.object
                )?;
            }
        }

        // Replacement MVs
        writeln!(f, "\n  {}", "Replacement Materialized Views:".bold())?;
        if self.replacement_mvs.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for record in &self.replacement_mvs {
                writeln!(
                    f,
                    "    {} {}.{}.{} {} {}.{}.{}",
                    "~".yellow(),
                    record.target_database,
                    record.target_schema,
                    record.target_name,
                    "<-".dimmed(),
                    record.target_database,
                    record.replacement_schema,
                    record.target_name
                )?;
            }
        }

        // Sinks to Repoint
        writeln!(f, "\n  {}", "Sinks to Repoint:".bold())?;
        if self.dependent_sinks.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for sink in &self.dependent_sinks {
                writeln!(
                    f,
                    "    {} {}.{}.{} {} {}.{}.{}",
                    "~".yellow(),
                    sink.sink_database,
                    sink.sink_schema,
                    sink.sink_name,
                    "->".dimmed(),
                    sink.dependency_database,
                    sink.dependency_schema,
                    sink.dependency_name
                )?;
            }
        }

        // Old Resources to Drop
        writeln!(f, "\n  {}", "Old Resources to Drop:".bold())?;
        let drops = self.resources_to_drop();
        if drops.is_empty() {
            writeln!(f, "    (none)")?;
        } else {
            for drop in &drops {
                writeln!(f, "    {} {}", "-".red(), drop.name)?;
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Plan generation
// ---------------------------------------------------------------------------

/// Build a complete deployment plan by querying the database once for all data.
async fn generate_deployment_plan(
    client: &Client,
    deploy_id: &str,
    apply_state: ApplyState,
    force: bool,
) -> Result<DeploymentPlan, CliError> {
    // 1. Gather swap resources (empty for PostSwap)
    let (staging_schemas, staging_clusters, staging_suffix) = if apply_state == ApplyState::PostSwap
    {
        verbose!("Resuming post-swap: skipping conflict check and resource gathering");
        (BTreeSet::new(), BTreeSet::new(), format!("_{}", deploy_id))
    } else {
        gather_resources_and_check_conflicts(client, deploy_id, force).await?
    };

    // 2. Query pending statements once
    let pending_statements = client
        .deployments()
        .get_pending_statements(deploy_id)
        .await?;

    // 3. Query replacement MVs once
    let replacement_mvs = client.deployments().get_replacement_mvs(deploy_id).await?;

    // 4. Query dependent sinks for display (pre-swap schema names)
    let dependent_sinks = if staging_schemas.is_empty() {
        Vec::new()
    } else {
        let prod_schemas: Vec<SchemaQualifier> = staging_schemas
            .iter()
            .map(|sq| {
                let prod_schema = sq.schema.trim_end_matches(&staging_suffix);
                SchemaQualifier::new(sq.database.clone(), prod_schema.to_string())
            })
            .collect();

        client
            .introspection()
            .find_sinks_depending_on_schemas(&prod_schemas)
            .await
            .map_err(CliError::Connection)?
    };

    Ok(DeploymentPlan {
        deploy_id: deploy_id.to_string(),
        apply_state,
        staging_suffix,
        staging_schemas,
        staging_clusters,
        pending_statements,
        replacement_mvs,
        dependent_sinks,
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Promote a staging deployment to production using ALTER SWAP.
pub async fn run(
    settings: &Settings,
    deploy_id: &str,
    force: bool,
    dry_run: bool,
    json_output: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();

    if dry_run {
        progress::info(&format!("Previewing deployment plan for '{}'", deploy_id));
    } else {
        progress::info(&format!("Deploying '{}' to production", deploy_id));
    }

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Validate deployment exists and is not promoted
    client.deployments().validate_staging(deploy_id).await?;

    let apply_state = client.deployments().get_apply_state(deploy_id).await?;
    verbose!("Apply state: {:?}", apply_state);

    let staging_snapshot =
        project::deployment_snapshot::load_from_database(&client, Some(deploy_id)).await?;
    verbose!(
        "Found {} objects in staging deployment",
        staging_snapshot.objects.len()
    );

    let plan = generate_deployment_plan(&client, deploy_id, apply_state, force).await?;

    if json_output {
        output::machine(&plan);
    }

    if dry_run {
        humanln!("{}", plan);
        if plan.has_work() {
            progress::info(&format!(
                "To execute this plan, run: mz-deploy deploy {}",
                deploy_id
            ));
        } else {
            progress::info("Nothing to do.");
        }
        return Ok(());
    }

    execute_swap_phase(&client, &plan).await?;
    run_post_swap_steps(&client, &plan).await?;
    cleanup_apply_state(&client, &plan.deploy_id).await?;

    progress::success("Deployment completed successfully!");
    progress::info(&format!(
        "Staging deployment '{}' is now in production",
        deploy_id
    ));

    Ok(())
}

// ---------------------------------------------------------------------------
// Execution functions
// ---------------------------------------------------------------------------

/// Runs the swap portion of apply according to persisted resume state.
async fn execute_swap_phase(client: &Client, plan: &DeploymentPlan) -> Result<(), CliError> {
    match plan.apply_state {
        ApplyState::NotStarted => {
            verbose!("Creating apply state schemas...");
            client
                .deployments()
                .create_apply_state_schemas(&plan.deploy_id)
                .await?;
            verbose!("Executing atomic swap...");
            execute_atomic_swap(
                client,
                &plan.deploy_id,
                &plan.staging_schemas,
                &plan.staging_clusters,
            )
            .await?;
        }
        ApplyState::PreSwap => {
            verbose!("Resuming from pre-swap state...");
            execute_atomic_swap(
                client,
                &plan.deploy_id,
                &plan.staging_schemas,
                &plan.staging_clusters,
            )
            .await?;
        }
        ApplyState::PostSwap => {
            verbose!("Resuming from post-swap state...");
        }
    }
    Ok(())
}

/// Completes promotion work that must happen after schemas/clusters are swapped.
async fn run_post_swap_steps(client: &Client, plan: &DeploymentPlan) -> Result<(), CliError> {
    execute_pending_sinks(client, plan).await?;
    apply_replacement_mvs(client, plan).await?;

    if !plan.staging_schemas.is_empty() {
        repoint_dependent_sinks(client, plan).await?;
    }

    verbose!("\nUpdating deployment table...");
    client
        .deployments()
        .update_promoted_at(&plan.deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    if !plan.staging_schemas.is_empty() || !plan.staging_clusters.is_empty() {
        progress::info("Dropping old production objects...");
        drop_old_resources(client, plan).await;
    }
    Ok(())
}

/// Removes deployment bookkeeping that is only needed while apply is in-flight.
async fn cleanup_apply_state(client: &Client, deploy_id: &str) -> Result<(), CliError> {
    verbose!("Cleaning up apply state...");
    client
        .deployments()
        .delete_apply_state_schemas(deploy_id)
        .await?;
    client
        .deployments()
        .delete_pending_statements(deploy_id)
        .await?;
    client
        .deployments()
        .delete_replacement_mvs(deploy_id)
        .await?;
    client
        .deployments()
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;
    Ok(())
}

/// Gather staging resources and check for deployment conflicts.
///
/// Returns the staging schemas, clusters, and suffix for the swap operation.
async fn gather_resources_and_check_conflicts(
    client: &Client,
    deploy_id: &str,
    force: bool,
) -> Result<(BTreeSet<SchemaQualifier>, BTreeSet<String>, String), CliError> {
    verbose!("Checking for deployment conflicts...");
    let conflicts = client
        .deployments()
        .check_deployment_conflicts(deploy_id)
        .await?;

    if !conflicts.is_empty() {
        if force {
            // With --force, show warning but continue
            eprintln!(
                "\n{}: deployment conflicts detected, but continuing due to --force flag",
                "warning".yellow().bold()
            );
            for conflict in &conflicts {
                eprintln!(
                    "  - {}.{} (last promoted by '{}' deployment)",
                    conflict.database, conflict.schema, conflict.deploy_id
                );
            }
            eprintln!();
        } else {
            // Without --force, return error
            return Err(CliError::DeploymentConflict { conflicts });
        }
    } else {
        verbose!("No conflicts detected");
    }

    // Get schemas and clusters from deployment tables
    let staging_suffix = format!("_{}", deploy_id);
    let mut staging_schemas = BTreeSet::new();
    let mut staging_clusters = BTreeSet::new();

    // Get schemas from deploy.deployments table for this deployment
    let deployment_records = client
        .deployments()
        .get_schema_deployments(Some(deploy_id))
        .await?;

    // Build list of (database, staging_schema) pairs to check, filtering out Sinks and Replacement
    let schemas_to_check: Vec<(String, String)> = deployment_records
        .iter()
        .filter(|record| {
            if record.kind == DeploymentKind::Sinks {
                verbose!(
                    "Skipping sink-only schema {}.{} (no swap needed)",
                    record.database,
                    record.schema
                );
                false
            } else if record.kind == DeploymentKind::Replacement {
                verbose!(
                    "Skipping replacement schema {}.{} (will be dropped after apply)",
                    record.database,
                    record.schema
                );
                false
            } else {
                true
            }
        })
        .map(|record| {
            let staging_schema = format!("{}{}", record.schema, staging_suffix);
            (record.database.clone(), staging_schema)
        })
        .collect();

    // Batch check which staging schemas exist
    let existing_schemas = client
        .introspection()
        .check_schemas_exist(&schemas_to_check)
        .await?;

    for pair in schemas_to_check {
        if existing_schemas.contains(&pair) {
            staging_schemas.insert(SchemaQualifier::new(pair.0, pair.1));
        } else {
            eprintln!("Warning: Staging schema {}.{} not found", pair.0, pair.1);
        }
    }

    // Validate that all clusters in the deployment still exist
    client
        .deployments()
        .validate_deployment_clusters(deploy_id)
        .await?;

    // Get clusters from deploy.clusters table
    let cluster_names = client
        .deployments()
        .get_deployment_clusters(deploy_id)
        .await?;

    // Batch check which staging clusters exist
    let staging_cluster_names: Vec<String> = cluster_names
        .iter()
        .map(|name| format!("{}{}", name, staging_suffix))
        .collect();
    let existing_clusters = client
        .introspection()
        .check_clusters_exist(&staging_cluster_names)
        .await?;

    for (cluster_name, staging_cluster) in cluster_names.into_iter().zip_eq(staging_cluster_names) {
        if existing_clusters.contains(&staging_cluster) {
            staging_clusters.insert(cluster_name);
        } else {
            eprintln!("Warning: Staging cluster {} not found", staging_cluster);
        }
    }

    verbose!("\nSchemas to swap:");
    for sq in &staging_schemas {
        let prod_schema = sq.schema.trim_end_matches(&staging_suffix);
        verbose!("  - {}.{} <-> {}", sq.database, sq.schema, prod_schema);
    }

    if !staging_clusters.is_empty() {
        verbose!("\nClusters to swap:");
        for cluster in &staging_clusters {
            let staging_cluster = format!("{}{}", cluster, staging_suffix);
            verbose!("  - {} <-> {}", staging_cluster, cluster);
        }
    }

    Ok((staging_schemas, staging_clusters, staging_suffix))
}

/// Execute the atomic swap of schemas, clusters, and state schemas.
async fn execute_atomic_swap(
    client: &Client,
    deploy_id: &str,
    staging_schemas: &BTreeSet<SchemaQualifier>,
    staging_clusters: &BTreeSet<String>,
) -> Result<(), CliError> {
    let staging_suffix = format!("_{}", deploy_id);

    // Begin transaction for atomic swap
    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "BEGIN".to_string(),
            source: e,
        })?;

    // Swap schemas
    for sq in staging_schemas {
        let prod_schema = sq.schema.trim_end_matches(&staging_suffix);
        // Note: second schema name is NOT fully qualified (same database)
        let swap_sql = format!(
            "ALTER SCHEMA \"{}\".\"{}\" SWAP WITH \"{}\";",
            sq.database, prod_schema, sq.schema
        );

        verbose!("  {}", swap_sql);
        if let Err(e) = client.execute(&swap_sql, &[]).await {
            let _ = client.execute("ROLLBACK", &[]).await;
            return Err(CliError::SqlExecutionFailed {
                statement: swap_sql,
                source: e,
            });
        }
    }

    // Swap clusters
    for cluster in staging_clusters {
        let staging_cluster = format!("{}{}", cluster, staging_suffix);
        let swap_sql = format!(
            "ALTER CLUSTER \"{}\" SWAP WITH \"{}\";",
            cluster, staging_cluster
        );

        verbose!("  {}", swap_sql);
        if let Err(e) = client.execute(&swap_sql, &[]).await {
            let _ = client.execute("ROLLBACK", &[]).await;
            return Err(CliError::SqlExecutionFailed {
                statement: swap_sql,
                source: e,
            });
        }
    }

    // Swap the apply state schemas - this atomically marks the swap as complete
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);
    let state_swap_sql = format!(
        "ALTER SCHEMA _mz_deploy.\"{}\" SWAP WITH \"{}\";",
        pre_schema, post_schema
    );

    verbose!("  {}", state_swap_sql);
    if let Err(e) = client.execute(&state_swap_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(CliError::SqlExecutionFailed {
            statement: state_swap_sql,
            source: e,
        });
    }

    // Commit transaction
    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "COMMIT".to_string(),
            source: e,
        })?;

    verbose!("Swap completed successfully");
    Ok(())
}

/// Execute pending sink statements using data from the plan.
///
/// Uses `plan.pending_statements` instead of re-querying. Still checks sink
/// existence at execution time for idempotency.
async fn execute_pending_sinks(client: &Client, plan: &DeploymentPlan) -> Result<(), CliError> {
    if plan.pending_statements.is_empty() {
        verbose!("No pending sinks to execute");
        return Ok(());
    }

    // Build set of sink ObjectIds from pending statements
    let sink_ids: BTreeSet<ObjectId> = plan
        .pending_statements
        .iter()
        .map(|stmt| ObjectId {
            database: stmt.database.clone(),
            schema: stmt.schema.clone(),
            object: stmt.object.clone(),
        })
        .collect();

    // Check which sinks already exist (like tables, skip existing ones)
    let existing_sinks = client.introspection().check_sinks_exist(&sink_ids).await?;

    // Filter to only sinks that don't exist
    let sinks_to_create: Vec<_> = plan
        .pending_statements
        .iter()
        .filter(|stmt| {
            let obj_id = ObjectId {
                database: stmt.database.clone(),
                schema: stmt.schema.clone(),
                object: stmt.object.clone(),
            };
            !existing_sinks.contains(&obj_id)
        })
        .collect();

    // Log skipped sinks
    if !existing_sinks.is_empty() {
        progress::info("Sinks that already exist (skipping):");
        let mut existing_list: Vec<_> = existing_sinks.iter().collect();
        existing_list.sort_by_key(|obj| (&obj.database, &obj.schema, &obj.object));
        for sink_id in existing_list {
            progress::info(&format!(
                "  - {}.{}.{}",
                sink_id.database, sink_id.schema, sink_id.object
            ));
        }
    }

    // If all sinks exist, exit early
    if sinks_to_create.is_empty() {
        if !existing_sinks.is_empty() {
            progress::info(&format!(
                "All {} sink(s) already exist. Nothing to create.",
                sink_ids.len()
            ));
        }
        return Ok(());
    }

    progress::info(&format!("Creating {} sink(s)...", sinks_to_create.len()));

    for stmt in sinks_to_create {
        verbose!(
            "Creating sink {}.{}.{}...",
            stmt.database,
            stmt.schema,
            stmt.object
        );

        // Execute the sink creation statement
        if let Err(e) = client.execute(&stmt.statement_sql, &[]).await {
            eprintln!(
                "Error creating sink {}.{}.{}: {}",
                stmt.database, stmt.schema, stmt.object, e
            );
            return Err(CliError::SqlExecutionFailed {
                statement: stmt.statement_sql.clone(),
                source: e,
            });
        }

        // Mark the statement as executed
        client
            .deployments()
            .mark_statement_executed(&plan.deploy_id, stmt.sequence_num)
            .await?;

        progress::success(&format!(
            "{}.{}.{}",
            stmt.database, stmt.schema, stmt.object
        ));
    }

    Ok(())
}

/// Apply replacement materialized views using data from the plan.
///
/// Uses `plan.replacement_mvs` instead of re-querying.
async fn apply_replacement_mvs(client: &Client, plan: &DeploymentPlan) -> Result<(), CliError> {
    if plan.replacement_mvs.is_empty() {
        verbose!("No replacement MVs to apply");
        return Ok(());
    }

    progress::info(&format!(
        "Applying {} replacement materialized view(s)...",
        plan.replacement_mvs.len()
    ));

    for record in &plan.replacement_mvs {
        let alter_sql = format!(
            "ALTER MATERIALIZED VIEW \"{}\".\"{}\".\"{}\"\
             APPLY REPLACEMENT \"{}\".\"{}\".\"{}\";",
            record.target_database,
            record.target_schema,
            record.target_name,
            record.target_database,
            record.replacement_schema,
            record.target_name
        );

        verbose!("  {}", alter_sql);
        if let Err(e) = client.execute(&alter_sql, &[]).await {
            eprintln!(
                "Error applying replacement for {}.{}.{}: {}",
                record.target_database, record.target_schema, record.target_name, e
            );
            return Err(CliError::SqlExecutionFailed {
                statement: alter_sql,
                source: e,
            });
        }

        progress::success(&format!(
            "{}.{}.{}",
            record.target_database, record.target_schema, record.target_name
        ));
    }

    // Drop now-empty replacement staging schemas
    let replacement_schemas = plan.replacement_schemas();

    for sq in &replacement_schemas {
        let drop_sql = format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE;",
            sq.database, sq.schema
        );
        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!(
                "warning: failed to drop replacement schema {}.{}: {}",
                sq.database, sq.schema, e
            );
        }
    }

    Ok(())
}

/// Repoint sinks that depend on objects in schemas about to be dropped.
///
/// Re-queries post-swap (cannot use `plan.dependent_sinks` because schema names
/// differ after swap). Reads `plan.staging_schemas` and `plan.staging_suffix`.
async fn repoint_dependent_sinks(client: &Client, plan: &DeploymentPlan) -> Result<(), CliError> {
    let staging_suffix = &plan.staging_suffix;

    // Build list of old schema names (database, old_schema_with_suffix)
    // After swap, old production schemas have the staging suffix
    let old_schemas: Vec<SchemaQualifier> = plan
        .staging_schemas
        .iter()
        .map(|sq| {
            let prod_schema = sq.schema.trim_end_matches(staging_suffix);
            let old_schema = format!("{}{}", prod_schema, staging_suffix);
            SchemaQualifier::new(sq.database.clone(), old_schema)
        })
        .collect();

    // Find sinks depending on objects in old schemas
    let dependent_sinks = client
        .introspection()
        .find_sinks_depending_on_schemas(&old_schemas)
        .await
        .map_err(CliError::Connection)?;

    if dependent_sinks.is_empty() {
        verbose!("No sinks depend on objects in schemas being dropped");
        return Ok(());
    }

    progress::info(&format!(
        "Repointing {} sink(s) to new upstream objects...",
        dependent_sinks.len()
    ));

    // Batch check which replacement objects exist
    let replacement_ids: BTreeSet<ObjectId> = dependent_sinks
        .iter()
        .map(|sink| {
            let new_schema = sink
                .dependency_schema
                .trim_end_matches(staging_suffix)
                .to_string();
            ObjectId {
                database: sink.dependency_database.clone(),
                schema: new_schema,
                object: sink.dependency_name.clone(),
            }
        })
        .collect();

    let existing_fqns: BTreeSet<String> = client
        .introspection()
        .check_objects_exist(&replacement_ids)
        .await
        .map_err(CliError::Connection)?
        .into_iter()
        .collect();

    for sink in dependent_sinks {
        // Compute new schema name (strip suffix to get production schema name)
        let new_schema = sink.dependency_schema.trim_end_matches(staging_suffix);

        // Check if replacement object exists using batch result
        let replacement_fqn = format!(
            "{}.{}.{}",
            sink.dependency_database, new_schema, sink.dependency_name
        );

        if !existing_fqns.contains(&replacement_fqn) {
            return Err(CliError::SinkRepointFailed {
                sink: format!(
                    "{}.{}.{}",
                    sink.sink_database, sink.sink_schema, sink.sink_name
                ),
                reason: format!(
                    "replacement object {}.{}.{} does not exist",
                    sink.dependency_database, new_schema, sink.dependency_name
                ),
            });
        }

        // Execute ALTER SINK ... SET FROM
        let alter_sql = format!(
            r#"ALTER SINK "{}"."{}"."{}" SET FROM "{}"."{}"."{}""#,
            sink.sink_database,
            sink.sink_schema,
            sink.sink_name,
            sink.dependency_database,
            new_schema,
            sink.dependency_name
        );

        verbose!("  {}", alter_sql);
        if let Err(e) = client.execute(&alter_sql, &[]).await {
            return Err(CliError::SinkRepointFailed {
                sink: format!(
                    "{}.{}.{}",
                    sink.sink_database, sink.sink_schema, sink.sink_name
                ),
                reason: e.to_string(),
            });
        }

        progress::success(&format!(
            "{}.{}.{} -> {}.{}.{}",
            sink.sink_database,
            sink.sink_schema,
            sink.sink_name,
            sink.dependency_database,
            new_schema,
            sink.dependency_name
        ));
    }

    Ok(())
}

/// Drop old production resources after the swap.
async fn drop_old_resources(client: &Client, plan: &DeploymentPlan) {
    let staging_suffix = &plan.staging_suffix;

    // Drop schemas
    for sq in &plan.staging_schemas {
        let prod_schema = sq.schema.trim_end_matches(staging_suffix);
        let old_schema = format!("{}{}", prod_schema, staging_suffix);
        let drop_sql = format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE;",
            sq.database, old_schema
        );

        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!(
                "warning: failed to drop old schema {}.{}: {}",
                sq.database, old_schema, e
            );
        }
    }

    // Drop clusters
    for cluster in &plan.staging_clusters {
        let old_cluster = format!("{}{}", cluster, staging_suffix);
        let drop_sql = format!("DROP CLUSTER IF EXISTS \"{}\" CASCADE;", old_cluster);

        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!("warning: failed to drop old cluster {}: {}", old_cluster, e);
        }
    }
}
