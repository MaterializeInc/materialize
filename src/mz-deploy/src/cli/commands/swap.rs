//! Swap command - promote staging environment to production via ALTER SWAP.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::{project, verbose};
use owo_colors::OwoColorize;
use std::collections::HashSet;
use std::path::Path;
use std::time::SystemTime;

/// Promote a staging environment to production using ALTER SWAP.
///
/// This command:
/// - Validates the staging environment exists and hasn't been promoted
/// - Checks for deployment conflicts (git-merge-style conflict detection)
/// - Uses ALTER SWAP to atomically promote schemas and clusters
/// - Updates deployment records with promoted_at timestamp
/// - Drops old production objects (which have staging suffix after swap)
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `directory` - Project directory
/// * `stage_name` - Staging environment name
/// * `force` - Force promotion despite conflicts
/// * `mir_project` - Compiled project (for cluster dependencies)
///
/// # Returns
/// Ok(()) if promotion succeeds
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if environment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if already promoted
/// Returns `CliError::DeploymentConflict` if conflicts detected (without --force)
/// Returns `CliError::Connection` for database errors
pub async fn run(
    profile: Option<&Profile>,
    _directory: &Path,
    stage_name: &str,
    force: bool,
    mir_project: &project::mir::Project,
) -> Result<(), CliError> {
    println!("Deploying '{}' to production", stage_name);

    // Connect to the database
    let client = helpers::connect_to_database(profile.unwrap()).await?;

    // Initialize deployment tracking infrastructure
    helpers::initialize_deployment_tracking(&client).await?;

    // Validate deployment exists and is not promoted
    let metadata = client
        .introspection()
        .get_deployment_metadata(stage_name)
        .await?;

    match metadata {
        Some(meta) if meta.promoted_at.is_some() => {
            return Err(CliError::StagingAlreadyPromoted {
                name: stage_name.to_string(),
            });
        }
        Some(_) => {
            // Good to proceed
        }
        None => {
            return Err(CliError::StagingEnvironmentNotFound {
                name: stage_name.to_string(),
            });
        }
    }

    // Load staging deployment state to identify what's deployed in staging
    let staging_snapshot =
        project::deployment_snapshot::load_from_database(&client, Some(stage_name)).await?;

    if staging_snapshot.objects.is_empty() {
        return Err(CliError::NoSchemas);
    }

    verbose!(
        "Found {} objects in staging environment",
        staging_snapshot.objects.len()
    );

    verbose!("Checking for deployment conflicts...");
    let conflicts = client
        .introspection()
        .check_deployment_conflicts(stage_name)
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
                    "  - {}.{} (last promoted by '{}' environment)",
                    conflict.database, conflict.schema, conflict.environment
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

    // Extract unique schemas and clusters from the staging objects
    let staging_suffix = format!("_{}", stage_name);
    let mut staging_schemas = HashSet::new();
    let mut staging_clusters = HashSet::new();

    // Collect schemas from staged objects
    // Note: The deployment table stores ORIGINAL object IDs (without staging suffix),
    // so we need to construct the staging schema name and verify it exists
    for object_id in staging_snapshot.objects.keys() {
        // Construct the staging schema name
        let staging_schema = format!("{}{}", object_id.schema, staging_suffix);

        // Check if the staging schema actually exists
        if client
            .introspection()
            .schema_exists(&object_id.database, &staging_schema)
            .await?
        {
            staging_schemas.insert((object_id.database.clone(), staging_schema));
        }
    }

    // Collect clusters that are used by the project
    for cluster in &mir_project.cluster_dependencies {
        let staging_cluster = format!("{}{}", cluster.name, staging_suffix);

        // Check if this staging cluster exists
        if client
            .introspection()
            .cluster_exists(&staging_cluster)
            .await?
        {
            staging_clusters.insert(cluster.name.clone());
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
        .pg_client()
        .execute("BEGIN", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "BEGIN".to_string(),
            source: e.into(),
        })?;

    // Swap schemas
    for (database, staging_schema) in &staging_schemas {
        let prod_schema = staging_schema.trim_end_matches(&staging_suffix);
        let swap_sql = format!(
            "ALTER SCHEMA \"{}\".\"{}\" SWAP WITH \"{}\";",
            database, prod_schema, staging_schema
        );

        verbose!("  {}", swap_sql);
        if let Err(e) = client.pg_client().execute(&swap_sql, &[]).await {
            let _ = client.pg_client().execute("ROLLBACK", &[]).await;
            return Err(CliError::SqlExecutionFailed {
                statement: swap_sql,
                source: e.into(),
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
        if let Err(e) = client.pg_client().execute(&swap_sql, &[]).await {
            let _ = client.pg_client().execute("ROLLBACK", &[]).await;
            return Err(CliError::SqlExecutionFailed {
                statement: swap_sql,
                source: e.into(),
            });
        }
    }

    // Commit transaction
    client
        .pg_client()
        .execute("COMMIT", &[])
        .await
        .map_err(|e| CliError::SqlExecutionFailed {
            statement: "COMMIT".to_string(),
            source: e.into(),
        })?;

    // Promote staging to production with promoted_at timestamps
    verbose!("\nUpdating deployment table...");

    let _now = SystemTime::now();

    // Promote to production by updating promoted_at timestamp
    client
        .creation()
        .update_promoted_at(stage_name)
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
        if let Err(e) = client.pg_client().execute(&drop_sql, &[]).await {
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
        if let Err(e) = client.pg_client().execute(&drop_sql, &[]).await {
            eprintln!("warning: failed to drop old cluster {}: {}", old_cluster, e);
        }
    }

    println!("Deployment completed successfully!");
    println!("Staging environment '{}' is now in production", stage_name);

    Ok(())
}
