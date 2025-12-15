//! Apply command - promote staging deployment to production via ALTER SWAP.

use crate::cli::CliError;
use crate::client::{ApplyState, Client, DeploymentKind, Profile};
use crate::project::object_id::ObjectId;
use crate::{project, verbose};
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

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
    println!("Deploying '{}' to production", deploy_id);

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    project::deployment_snapshot::initialize_deployment_table(&client).await?;

    // Validate deployment exists and is not promoted
    let metadata = client.get_deployment_metadata(deploy_id).await?;

    match metadata {
        Some(meta) if meta.promoted_at.is_some() => {
            return Err(CliError::StagingAlreadyPromoted {
                name: deploy_id.to_string(),
            });
        }
        Some(_) => {
            // Good to proceed
        }
        None => {
            return Err(CliError::StagingEnvironmentNotFound {
                name: deploy_id.to_string(),
            });
        }
    }

    // Check apply state for resume scenarios
    let apply_state = client.get_apply_state(deploy_id).await?;
    verbose!("Apply state: {:?}", apply_state);

    // Load staging deployment state to identify what's deployed in staging
    let staging_snapshot =
        project::deployment_snapshot::load_from_database(&client, Some(deploy_id)).await?;

    verbose!(
        "Found {} objects in staging deployment",
        staging_snapshot.objects.len()
    );

    // Only check conflicts and gather resources if we haven't swapped yet
    let (staging_schemas, staging_clusters, staging_suffix) = if apply_state != ApplyState::PostSwap
    {
        gather_resources_and_check_conflicts(&client, deploy_id, force).await?
    } else {
        // Post-swap: we don't need these for sink execution
        verbose!("Resuming post-swap: skipping conflict check and resource gathering");
        (BTreeSet::new(), BTreeSet::new(), format!("_{}", deploy_id))
    };

    // Execute based on current state
    match apply_state {
        ApplyState::NotStarted => {
            // Fresh apply: create state schemas and execute swap
            verbose!("Creating apply state schemas...");
            client.create_apply_state_schemas(deploy_id).await?;

            verbose!("Executing atomic swap...");
            execute_atomic_swap(&client, deploy_id, &staging_schemas, &staging_clusters).await?;
        }
        ApplyState::PreSwap => {
            // Resume: state schemas exist but swap didn't complete
            verbose!("Resuming from pre-swap state...");
            execute_atomic_swap(&client, deploy_id, &staging_schemas, &staging_clusters).await?;
        }
        ApplyState::PostSwap => {
            // Resume: swap completed, continue to sinks
            verbose!("Resuming from post-swap state...");
        }
    }

    // Execute pending sinks (skip any already executed)
    execute_pending_sinks(&client, deploy_id).await?;

    // Update promoted_at timestamp
    verbose!("\nUpdating deployment table...");
    client
        .update_promoted_at(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Drop old production resources (now have staging suffix after swap)
    // Only do this if we have the resource info (i.e., we did the swap in this run)
    if !staging_schemas.is_empty() || !staging_clusters.is_empty() {
        println!("\nDropping old production objects...");
        drop_old_resources(
            &client,
            &staging_schemas,
            &staging_clusters,
            &staging_suffix,
        )
        .await;
    }

    // Clean up apply state and pending statements
    verbose!("Cleaning up apply state...");
    client.delete_apply_state_schemas(deploy_id).await?;
    client.delete_pending_statements(deploy_id).await?;
    client
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    println!("Deployment completed successfully!");
    println!("Staging deployment '{}' is now in production", deploy_id);

    Ok(())
}

/// Gather staging resources and check for deployment conflicts.
///
/// Returns the staging schemas, clusters, and suffix for the swap operation.
async fn gather_resources_and_check_conflicts(
    client: &Client,
    deploy_id: &str,
    force: bool,
) -> Result<(BTreeSet<(String, String)>, BTreeSet<String>, String), CliError> {
    verbose!("Checking for deployment conflicts...");
    let conflicts = client.check_deployment_conflicts(deploy_id).await?;

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
    let deployment_records = client.get_schema_deployments(Some(deploy_id)).await?;
    for record in deployment_records {
        // Skip sink-only schemas - they don't need swapping
        // Sinks are created after the swap via pending_statements
        if record.kind == DeploymentKind::Sinks {
            verbose!(
                "Skipping sink-only schema {}.{} (no swap needed)",
                record.database,
                record.schema
            );
            continue;
        }

        let staging_schema = format!("{}{}", record.schema, staging_suffix);

        // Verify staging schema still exists
        if client
            .schema_exists(&record.database, &staging_schema)
            .await?
        {
            staging_schemas.insert((record.database.clone(), staging_schema));
        } else {
            eprintln!(
                "Warning: Staging schema {}.{} not found",
                record.database, staging_schema
            );
        }
    }

    // Validate that all clusters in the deployment still exist
    client.validate_deployment_clusters(deploy_id).await?;

    // Get clusters from deploy.clusters table
    let cluster_names = client.get_deployment_clusters(deploy_id).await?;
    for cluster_name in cluster_names {
        let staging_cluster = format!("{}{}", cluster_name, staging_suffix);

        // Verify staging cluster still exists
        if client.cluster_exists(&staging_cluster).await? {
            staging_clusters.insert(cluster_name);
        } else {
            eprintln!("Warning: Staging cluster {} not found", staging_cluster);
        }
    }

    verbose!("\nSchemas to swap:");
    for (database, schema) in &staging_schemas {
        let prod_schema = schema.trim_end_matches(&staging_suffix);
        verbose!("  - {}.{} <-> {}", database, schema, prod_schema);
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
    staging_schemas: &BTreeSet<(String, String)>,
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
    for (database, staging_schema) in staging_schemas {
        let prod_schema = staging_schema.trim_end_matches(&staging_suffix);
        // Note: second schema name is NOT fully qualified (same database)
        let swap_sql = format!(
            "ALTER SCHEMA \"{}\".\"{}\" SWAP WITH \"{}\";",
            database, prod_schema, staging_schema
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
    let pending = client.get_pending_statements(deploy_id).await?;

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
    let existing_sinks = client.check_sinks_exist(&sink_ids).await?;

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
        println!("\nSinks that already exist (skipping):");
        let mut existing_list: Vec<_> = existing_sinks.iter().collect();
        existing_list.sort_by_key(|obj| (&obj.database, &obj.schema, &obj.object));
        for sink_id in existing_list {
            println!(
                "  - {}.{}.{}",
                sink_id.database, sink_id.schema, sink_id.object
            );
        }
    }

    // If all sinks exist, exit early
    if sinks_to_create.is_empty() {
        if !existing_sinks.is_empty() {
            println!(
                "\nAll {} sink(s) already exist. Nothing to create.",
                sink_ids.len()
            );
        }
        return Ok(());
    }

    println!("\nCreating {} sink(s)...", sinks_to_create.len());

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
            .mark_statement_executed(deploy_id, stmt.sequence_num)
            .await?;

        println!("  ✓ {}.{}.{}", stmt.database, stmt.schema, stmt.object);
    }

    Ok(())
}

/// Drop old production resources after the swap.
///
/// After the swap, old production objects now have the staging suffix.
/// This function drops them to clean up.
async fn drop_old_resources(
    client: &Client,
    staging_schemas: &BTreeSet<(String, String)>,
    staging_clusters: &BTreeSet<String>,
    staging_suffix: &str,
) {
    // Drop schemas
    for (database, staging_schema) in staging_schemas {
        let prod_schema = staging_schema.trim_end_matches(staging_suffix);
        // After swap, the old production schema is now named with the staging suffix
        let old_schema = format!("{}{}", prod_schema, staging_suffix);
        let drop_sql = format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE;",
            database, old_schema
        );

        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!(
                "warning: failed to drop old schema {}.{}: {}",
                database, old_schema, e
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
