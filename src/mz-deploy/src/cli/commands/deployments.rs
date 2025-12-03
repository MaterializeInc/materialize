//! Deployments command - list active staging deployments.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use owo_colors::OwoColorize;

/// List all active staging deployments.
///
/// This command:
/// - Queries all deployments where promoted_at IS NULL (staging only)
/// - Groups results by environment name
/// - Displays schemas in each staging environment with deployment metadata
///
/// Similar to `git branch` - shows active development branches.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
///
/// # Returns
/// Ok(()) if listing succeeds
///
/// # Errors
/// Returns `CliError::Connection` for database errors
pub async fn run(profile: &Profile) -> Result<(), CliError> {
    // Connect to database
    let client = helpers::connect_to_database(profile).await?;

    // List staging deployments
    let deployments = client.list_staging_deployments().await?;

    if deployments.is_empty() {
        println!("No active staging deployments.");
        println!();
        println!("To create a staging deployment, run:");
        println!("  {} {} {}", "mz-deploy".cyan(), "stage".cyan(), ".".cyan());
        return Ok(());
    }

    println!("Active staging deployments:");
    println!();

    let mut env_names: Vec<_> = deployments.keys().collect();
    env_names.sort();

    for env_name in env_names {
        let (deployed_at, deployed_by, schemas) = &deployments[env_name];

        // Format timestamp
        let timestamp = match deployed_at.elapsed() {
            Ok(duration) => {
                let hours = duration.as_secs() / 3600;
                if hours < 1 {
                    let minutes = (duration.as_secs() % 3600) / 60;
                    format!("{} minutes ago", minutes)
                } else if hours < 24 {
                    format!("{} hours ago", hours)
                } else {
                    let days = hours / 24;
                    format!("{} days ago", days)
                }
            }
            Err(_) => "recently".to_string(),
        };

        println!(
            "  {} {} by {} {}",
            "●".green(),
            env_name.cyan().bold(),
            deployed_by.yellow(),
            format!("({})", timestamp).dimmed()
        );

        // Get hydration status for this deployment
        match client.get_deployment_hydration_status(env_name).await {
            Ok(hydration_status) if !hydration_status.is_empty() => {
                let mut total_hydrated = 0i64;
                let mut total_clusters = 0i64;

                for (_cluster_name, (hydrated, total)) in &hydration_status {
                    if hydrated == total {
                        total_hydrated += 1;
                    }
                    total_clusters += 1;
                }

                let text = if total_hydrated == total_clusters {
                    "clusters: all hydrated".to_string()
                } else {
                    format!("clusters: {} of {} hydrated", total_hydrated, total_clusters)
                };
                println!("    {}\n", text.blue());
            }
            Ok(_) => {
                // Empty hydration status - deployment has no clusters
                // Don't display anything
            }
            Err(_) => {
                // Error getting hydration status - don't block display, just skip
            }
        }

        for (database, schema) in schemas {
            println!("    {}.{}", database.dimmed(), schema);
        }
        println!();
    }

    Ok(())
}
