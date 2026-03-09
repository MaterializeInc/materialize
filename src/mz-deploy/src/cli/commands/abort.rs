//! Abort command - cleanup a staged deployment.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Client;
use crate::config::Settings;
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
pub async fn run(settings: &Settings, deploy_id: &str) -> Result<(), CliError> {
    let profile = settings.connection();
    progress::info(&format!("Aborting staged deployment: {}", deploy_id));

    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.deployments().validate_staging(deploy_id).await?;

    // Get staging schemas and clusters
    let staging_schemas = client
        .introspection()
        .get_staging_schemas(deploy_id)
        .await?;

    let staging_clusters = client
        .introspection()
        .get_staging_clusters(deploy_id)
        .await?;

    verbose!("Dropping staging resources:");
    verbose!("  Schemas: {}", staging_schemas.len());
    verbose!("  Clusters: {}", staging_clusters.len());
    verbose!();

    // Drop staging schemas
    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        client
            .introspection()
            .drop_staging_schemas(&staging_schemas)
            .await?;
        for sq in &staging_schemas {
            verbose!("  Dropped {}.{}", sq.database, sq.schema);
        }
    }

    // Drop staging clusters
    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        client
            .introspection()
            .drop_staging_clusters(&staging_clusters)
            .await?;
        for cluster in &staging_clusters {
            verbose!("  Dropped {}", cluster);
        }
    }

    // Delete deployment records
    verbose!("Deleting deployment records...");

    // Clean up cluster tracking records
    client
        .deployments()
        .delete_deployment_clusters(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up pending statements (for sinks)
    client
        .deployments()
        .delete_pending_statements(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up replacement MV records
    client
        .deployments()
        .delete_replacement_mvs(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    // Clean up apply state schemas if they exist (from interrupted apply)
    client
        .deployments()
        .delete_apply_state_schemas(deploy_id)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    client.deployments().delete_deployment(deploy_id).await?;

    progress::success(&format!("Successfully aborted deployment '{}'", deploy_id));

    Ok(())
}
