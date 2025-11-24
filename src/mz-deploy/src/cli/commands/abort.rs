//! Abort command - cleanup a staged deployment.

use crate::cli::{CliError, helpers};
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
/// * `profile_name` - Optional database profile name
/// * `environment` - Staging environment name to abort
///
/// # Returns
/// Ok(()) if abort succeeds
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if the environment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if the environment was already promoted
/// Returns `CliError::Connection` for database errors
pub async fn run(profile_name: Option<&str>, environment: &str) -> Result<(), CliError> {
    println!("Aborting staged deployment: {}", environment);

    // Connect to database
    let client = helpers::connect_to_database(profile_name).await?;

    // Validate deployment exists and is not promoted
    let metadata = client
        .introspection()
        .get_deployment_metadata(environment)
        .await?;

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
    let staging_schemas = client
        .introspection()
        .get_staging_schemas(environment)
        .await?;

    let staging_clusters = client
        .introspection()
        .get_staging_clusters(environment)
        .await?;

    verbose!("Dropping staging resources:");
    verbose!("  Schemas: {}", staging_schemas.len());
    verbose!("  Clusters: {}", staging_clusters.len());
    verbose!();

    // Drop staging schemas
    if !staging_schemas.is_empty() {
        verbose!("Dropping staging schemas...");
        client
            .destruction()
            .drop_staging_schemas(&staging_schemas)
            .await?;
        for (database, schema) in &staging_schemas {
            verbose!("  Dropped {}.{}", database, schema);
        }
    }

    // Drop staging clusters
    if !staging_clusters.is_empty() {
        verbose!("Dropping staging clusters...");
        client
            .destruction()
            .drop_staging_clusters(&staging_clusters)
            .await?;
        for cluster in &staging_clusters {
            verbose!("  Dropped {}", cluster);
        }
    }

    // Delete deployment records
    verbose!("Deleting deployment records...");
    client.creation().delete_deployment(environment).await?;

    println!("Successfully aborted deployment '{}'", environment);

    Ok(())
}
