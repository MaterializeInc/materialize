//! Abort command - cleanup a staged deployment.

use crate::cli::CliError;
use crate::client::{Client, Profile};
use crate::verbose;

/// Abort a staged deployment by dropping schemas, clusters, and deployment records.
///
/// This command:
/// - Validates that the staging deployment exists and hasn't been promoted
/// - Drops all staging schemas (with _<deploy_id> suffix)
/// - Drops all staging clusters (with _<deploy_id> suffix)
/// - Deletes deployment tracking records
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `deploy_id` - Staging deployment ID to abort
///
/// # Returns
/// Ok(()) if abort succeeds
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if the deployment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if the deployment was already promoted
/// Returns `CliError::Connection` for database errors
pub async fn run(profile: &Profile, deploy_id: &str) -> Result<(), CliError> {
    println!("Aborting staged deployment: {}", deploy_id);

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

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

    // Get staging schemas and clusters
    let staging_schemas = client.get_staging_schemas(deploy_id).await?;

    let staging_clusters = client.get_staging_clusters(deploy_id).await?;

    verbose!("Dropping staging resources:");
    verbose!("  Schemas: {}", staging_schemas.len());
    verbose!("  Clusters: {}", staging_clusters.len());
    verbose!();

    // Drop staging schemas
    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        client.drop_staging_schemas(&staging_schemas).await?;
        for (database, schema) in &staging_schemas {
            verbose!("  Dropped {}.{}", database, schema);
        }
    }

    // Drop staging clusters
    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        client.drop_staging_clusters(&staging_clusters).await?;
        for cluster in &staging_clusters {
            verbose!("  Dropped {}", cluster);
        }
    }

    // Delete deployment records
    verbose!("Deleting deployment records...");

    // Clean up cluster tracking records
    client
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    client.delete_deployment(deploy_id).await?;

    println!("Successfully aborted deployment '{}'", deploy_id);

    Ok(())
}
