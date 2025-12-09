//! Apply command - promote staging deployment to production via ALTER SWAP.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::{project, verbose};
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

/// Promote a staging deployment to production using ALTER SWAP.
///
/// This command:
/// - Validates the staging deployment exists and hasn't been promoted
/// - Checks for deployment conflicts (git-merge-style conflict detection)
/// - Uses ALTER SWAP to atomically promote schemas and clusters
/// - Updates deployment records with promoted_at timestamp
/// - Drops old production objects (which have staging suffix after swap)
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project directory
/// * `deploy_id` - Staging deployment ID
/// * `force` - Force promotion despite conflicts
/// * `planned_project` - Compiled project (for cluster dependencies)
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

    // Load staging deployment state to identify what's deployed in staging
    let staging_snapshot =
        project::deployment_snapshot::load_from_database(&client, Some(deploy_id)).await?;

    if staging_snapshot.objects.is_empty() {
        return Err(CliError::NoSchemas);
    }

    verbose!(
        "Found {} objects in staging deployment",
        staging_snapshot.objects.len()
    );

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

    if staging_schemas.is_empty() && staging_clusters.is_empty() {
        return Err(CliError::NoSchemas);
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

    // Begin transaction for atomic swap
    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "BEGIN".to_string(),
            source: e,
        })?;

    // Swap schemas
    for (database, staging_schema) in &staging_schemas {
        let prod_schema = staging_schema.trim_end_matches(&staging_suffix);
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
    for cluster in &staging_clusters {
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

    // Commit transaction
    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "COMMIT".to_string(),
            source: e,
        })?;

    // Promote staging to production with promoted_at timestamps
    verbose!("\nUpdating deployment table...");

    // Promote to production by updating promoted_at timestamp
    client
        .update_promoted_at(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up cluster tracking records after successful swap
    verbose!("Cleaning up cluster tracking records...");
    client
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    println!("\nDropping old production objects...");

    // Drop schemas
    for (database, staging_schema) in &staging_schemas {
        let prod_schema = staging_schema.trim_end_matches(&staging_suffix);
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
    for cluster in &staging_clusters {
        // After swap, the old production cluster is now named with the staging suffix
        let old_cluster = format!("{}{}", cluster, staging_suffix);
        let drop_sql = format!("DROP CLUSTER IF EXISTS \"{}\" CASCADE;", old_cluster);

        verbose!("  {}", drop_sql);
        if let Err(e) = client.execute(&drop_sql, &[]).await {
            eprintln!("warning: failed to drop old cluster {}: {}", old_cluster, e);
        }
    }

    println!("Deployment completed successfully!");
    println!("Staging deployment '{}' is now in production", deploy_id);

    Ok(())
}
