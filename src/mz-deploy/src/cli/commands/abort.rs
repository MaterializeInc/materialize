//! Abort command - cleanup a staged deployment.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use crate::verbose;

/// Abort a staged deployment by dropping schemas, clusters, and deployment records.
///
/// This command:
/// - Validates that the staging environment exists and hasn't been promoted
/// - Drops all staging schemas (with _<environment> suffix)
/// - Drops all staging clusters (with _<environment> suffix)
/// - Deletes deployment tracking records
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `environment` - Staging environment name to abort
///
/// # Returns
/// Ok(()) if abort succeeds
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if the environment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if the environment was already promoted
/// Returns `CliError::Connection` for database errors
pub async fn run(profile: &Profile, environment: &str) -> Result<(), CliError> {
    println!("Aborting staged deployment: {}", environment);

    // Connect to database
    let client = helpers::connect_to_database(profile).await?;

    // Validate deployment exists and is not promoted
    let metadata = client.get_deployment_metadata(environment).await?;

    match metadata {
        Some(meta) if meta.promoted_at.is_some() => {
            return Err(CliError::StagingAlreadyPromoted {
                name: environment.to_string(),
            });
        }
        Some(_) => {
            // Good to proceed
        }
        None => {
            return Err(CliError::StagingEnvironmentNotFound {
                name: environment.to_string(),
            });
        }
    }

    // Get staging schemas and clusters
    let staging_schemas = client.get_staging_schemas(environment).await?;

    let staging_clusters = client.get_staging_clusters(environment).await?;

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
        .delete_deployment_clusters(environment)
        .await
        .map_err(|source| CliError::DeploymentStateWriteFailed { source })?;

    client.delete_deployment(environment).await?;

    println!("Successfully aborted deployment '{}'", environment);

    Ok(())
}
