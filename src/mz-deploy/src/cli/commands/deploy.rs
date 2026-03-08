//! Deploy command - promote staging deployment to production via ALTER SWAP.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::{ApplyState, Client, DeploymentKind, Profile};
use crate::project::SchemaQualifier;
use crate::project::object_id::ObjectId;
use crate::{project, verbose};
use itertools::Itertools;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

/// Runtime context shared across apply phases.
///
/// `run` computes this once and reuses it so each phase can focus on its own
/// responsibility (swap, post-swap work, cleanup) without re-querying state.
struct ApplyResources {
    staging_schemas: BTreeSet<SchemaQualifier>,
    staging_clusters: BTreeSet<String>,
    staging_suffix: String,
}

/// Promote a staging deployment to production using ALTER SWAP.
///
/// This command implements a resumable promotion flow:
/// 1. Check for existing apply state (for resume scenarios)
/// 2. Create apply state schemas if starting fresh
/// 3. Execute atomic swap (schemas, clusters, and state schemas in one transaction)
/// 4. Execute pending sinks (created after swap since they write to external systems)
/// 5. Clean up old resources and state tracking
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `deploy_id` - Staging deployment ID
/// * `force` - Force promotion despite conflicts
///
/// # Returns
/// Ok(()) if promotion succeeds
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if deployment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if already promoted
/// Returns `CliError::DeploymentConflict` if conflicts detected (without --force)
/// Returns `CliError::Connection` for database errors
pub async fn run(profile: &Profile, deploy_id: &str, force: bool) -> Result<(), CliError> {
    progress::info(&format!("Deploying '{}' to production", deploy_id));

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

    let resources = prepare_apply_resources(&client, deploy_id, apply_state, force).await?;
    execute_swap_phase(&client, deploy_id, apply_state, &resources).await?;
    run_post_swap_steps(&client, deploy_id, &resources).await?;
    cleanup_apply_state(&client, deploy_id).await?;

    progress::success("Deployment completed successfully!");
    progress::info(&format!(
        "Staging deployment '{}' is now in production",
        deploy_id
    ));

    Ok(())
}

/// Collects swap targets for the current apply attempt.
///
/// During `PostSwap` resume we intentionally skip discovery because swap already happened;
/// the returned empty sets signal later phases to avoid swap-specific cleanup.
async fn prepare_apply_resources(
    client: &Client,
    deploy_id: &str,
    apply_state: ApplyState,
    force: bool,
) -> Result<ApplyResources, CliError> {
    if apply_state == ApplyState::PostSwap {
        verbose!("Resuming post-swap: skipping conflict check and resource gathering");
        return Ok(ApplyResources {
            staging_schemas: BTreeSet::new(),
            staging_clusters: BTreeSet::new(),
            staging_suffix: format!("_{}", deploy_id),
        });
    }
    let (staging_schemas, staging_clusters, staging_suffix) =
        gather_resources_and_check_conflicts(client, deploy_id, force).await?;
    Ok(ApplyResources {
        staging_schemas,
        staging_clusters,
        staging_suffix,
    })
}

/// Runs the swap portion of apply according to persisted resume state.
///
/// This function is the state-machine boundary for apply: it decides whether
/// to create state schemas, execute swap, or skip directly to post-swap work.
async fn execute_swap_phase(
    client: &Client,
    deploy_id: &str,
    apply_state: ApplyState,
    resources: &ApplyResources,
) -> Result<(), CliError> {
    match apply_state {
        ApplyState::NotStarted => {
            verbose!("Creating apply state schemas...");
            client
                .deployments()
                .create_apply_state_schemas(deploy_id)
                .await?;
            verbose!("Executing atomic swap...");
            execute_atomic_swap(
                client,
                deploy_id,
                &resources.staging_schemas,
                &resources.staging_clusters,
            )
            .await?;
        }
        ApplyState::PreSwap => {
            verbose!("Resuming from pre-swap state...");
            execute_atomic_swap(
                client,
                deploy_id,
                &resources.staging_schemas,
                &resources.staging_clusters,
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
///
/// Includes deferred sink execution, replacement MV apply, promoted timestamp update,
/// and best-effort dropping of old production resources when their identities are known.
async fn run_post_swap_steps(
    client: &Client,
    deploy_id: &str,
    resources: &ApplyResources,
) -> Result<(), CliError> {
    execute_pending_sinks(client, deploy_id).await?;
    apply_replacement_mvs(client, deploy_id).await?;

    if !resources.staging_schemas.is_empty() {
        repoint_dependent_sinks(
            client,
            &resources.staging_schemas,
            &resources.staging_suffix,
        )
        .await?;
    }

    verbose!("\nUpdating deployment table...");
    client
        .deployments()
        .update_promoted_at(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    if !resources.staging_schemas.is_empty() || !resources.staging_clusters.is_empty() {
        progress::info("Dropping old production objects...");
        drop_old_resources(
            client,
            &resources.staging_schemas,
            &resources.staging_clusters,
            &resources.staging_suffix,
        )
        .await;
    }
    Ok(())
}

/// Removes deployment bookkeeping that is only needed while apply is in-flight.
///
/// This finalizes apply by clearing resume metadata and deferred-operation tables.
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
///
/// This transaction includes:
/// - Swapping user schemas (production <-> staging)
/// - Swapping clusters (production <-> staging)
/// - Swapping apply state schemas (pre <-> post, which moves the 'swapped=true' comment to _pre)
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
    // After this swap, apply_<id>_pre will have comment 'swapped=true' (it was _post before)
    let pre_schema = format!("apply_{}_pre", deploy_id);
    let post_schema = format!("apply_{}_post", deploy_id);
    // Note: second schema name is NOT fully qualified (same database: _mz_deploy)
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

/// Execute pending sink statements (created after swap).
///
/// Sinks are created in production after the swap because they immediately
/// start writing to external systems. Like tables, sinks are only created if
/// they don't already exist - the hash is ignored.
async fn execute_pending_sinks(client: &Client, deploy_id: &str) -> Result<(), CliError> {
    let pending = client
        .deployments()
        .get_pending_statements(deploy_id)
        .await?;

    if pending.is_empty() {
        verbose!("No pending sinks to execute");
        return Ok(());
    }

    // Build set of sink ObjectIds from pending statements
    let sink_ids: BTreeSet<ObjectId> = pending
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
    let sinks_to_create: Vec<_> = pending
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
            // Log the error - the statement will remain unexecuted for retry
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
            .mark_statement_executed(deploy_id, stmt.sequence_num)
            .await?;

        progress::success(&format!(
            "{}.{}.{}",
            stmt.database, stmt.schema, stmt.object
        ));
    }

    Ok(())
}

/// Apply replacement materialized views after the swap.
///
/// For each replacement MV record, executes:
///   ALTER MATERIALIZED VIEW "db"."schema"."target" APPLY REPLACEMENT "db"."staging_schema"."name"
///
/// After applying, the replacement MVs are absorbed into their targets and the
/// replacement staging schemas become empty. Those schemas are then dropped.
async fn apply_replacement_mvs(client: &Client, deploy_id: &str) -> Result<(), CliError> {
    let records = client.deployments().get_replacement_mvs(deploy_id).await?;

    if records.is_empty() {
        verbose!("No replacement MVs to apply");
        return Ok(());
    }

    progress::info(&format!(
        "Applying {} replacement materialized view(s)...",
        records.len()
    ));

    for record in &records {
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
    let replacement_schemas: BTreeSet<SchemaQualifier> = records
        .iter()
        .map(|r| SchemaQualifier::new(r.target_database.clone(), r.replacement_schema.clone()))
        .collect();

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
/// After the swap, old production objects are in schemas with the staging suffix.
/// Before dropping those schemas, we need to ALTER SINK any sinks that depend
/// on those objects to point to the new production objects instead.
///
/// This prevents sinks from being transitively dropped by CASCADE when the
/// old schemas are dropped.
async fn repoint_dependent_sinks(
    client: &Client,
    staging_schemas: &BTreeSet<SchemaQualifier>,
    staging_suffix: &str,
) -> Result<(), CliError> {
    // Build list of old schema names (database, old_schema_with_suffix)
    // After swap, old production schemas have the staging suffix
    let old_schemas: Vec<SchemaQualifier> = staging_schemas
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
///
/// After the swap, old production objects now have the staging suffix.
/// This function drops them to clean up.
async fn drop_old_resources(
    client: &Client,
    staging_schemas: &BTreeSet<SchemaQualifier>,
    staging_clusters: &BTreeSet<String>,
    staging_suffix: &str,
) {
    // Drop schemas
    for sq in staging_schemas {
        let prod_schema = sq.schema.trim_end_matches(staging_suffix);
        // After swap, the old production schema is now named with the staging suffix
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
    for cluster in staging_clusters {
        // After swap, the old production cluster is now named with the staging suffix
        let old_cluster = format!("{}{}", cluster, staging_suffix);
        let drop_sql = format!("DROP CLUSTER IF EXISTS \"{}\" CASCADE;", old_cluster);

        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!("warning: failed to drop old cluster {}: {}", old_cluster, e);
        }
    }
}
