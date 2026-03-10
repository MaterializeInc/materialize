//! Deployments command - list active staging deployments.

use crate::cli::CliError;
use crate::client::Client;
use crate::config::Settings;
use crate::humanln;
use crate::output;
use chrono::Utc;
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
/// * `allowed_lag_secs` - Maximum allowed lag in seconds before marking as "lagging"
///
/// # Returns
/// Ok(()) if listing succeeds
///
/// # Errors
/// Returns `CliError::Connection` for database errors
pub async fn run(
    settings: &Settings,
    allowed_lag_secs: i64,
    json_output: bool,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.deployments().create_deployments().await?;
    let deployments = client.deployments().list_staging_deployments().await?;

    if json_output {
        let mut result = Vec::new();
        let mut env_names: Vec<_> = deployments.keys().collect();
        env_names.sort();
        for env_name in env_names {
            let deployment = &deployments[env_name];
            let clusters = client
                .deployments()
                .get_deployment_hydration_status_with_lag(env_name, allowed_lag_secs)
                .await
                .unwrap_or_default();
            result.push(serde_json::json!({
                "deploy_id": env_name,
                "deployed_at": deployment.deployed_at,
                "deployed_by": deployment.deployed_by,
                "git_commit": deployment.git_commit,
                "kind": deployment.kind,
                "schemas": deployment.schemas,
                "clusters": clusters,
            }));
        }
        output::machine(&result);
        return Ok(());
    }

    if deployments.is_empty() {
        humanln!("No active staging deployments.");
        humanln!();
        humanln!("To create a staging deployment, run:");
        humanln!("  {} {} {}", "mz-deploy".cyan(), "stage".cyan(), ".".cyan());
        return Ok(());
    }

    humanln!("Active staging deployments:");
    humanln!();

    let mut env_names: Vec<_> = deployments.keys().collect();
    env_names.sort();

    for env_name in env_names {
        let deployment = &deployments[env_name];

        // Format timestamp
        let now = Utc::now();
        let duration = now.signed_duration_since(deployment.deployed_at);
        let timestamp = if duration.num_seconds() < 0 {
            "recently".to_string()
        } else {
            let hours = duration.num_hours();
            if hours < 1 {
                let minutes = duration.num_minutes();
                format!("{} minutes ago", minutes)
            } else if hours < 24 {
                format!("{} hours ago", hours)
            } else {
                let days = hours / 24;
                format!("{} days ago", days)
            }
        };

        humanln!(
            "  {} {} by {} {} [{}]",
            "●".green(),
            env_name.cyan().bold(),
            deployment.deployed_by.yellow(),
            format!("({})", timestamp).dimmed(),
            deployment.kind.to_string().dimmed()
        );

        // Display commit if available
        if let Some(commit_sha) = &deployment.git_commit {
            humanln!("    commit: {}", commit_sha.dimmed());
        }

        // Get hydration status for this deployment
        match client
            .deployments()
            .get_deployment_hydration_status_with_lag(env_name, allowed_lag_secs)
            .await
        {
            Ok(hydration_status) if !hydration_status.is_empty() => {
                use crate::client::ClusterDeploymentStatus;
                let mut ready_count = 0i64;
                #[allow(clippy::as_conversions)]
                let total_clusters = hydration_status.len() as i64;

                for ctx in &hydration_status {
                    if matches!(ctx.status, ClusterDeploymentStatus::Ready) {
                        ready_count += 1;
                    }
                }

                let text = if ready_count == total_clusters {
                    "clusters: all ready".to_string()
                } else {
                    format!("clusters: {} of {} ready", ready_count, total_clusters)
                };
                humanln!("    {}\n", text.blue());
            }
            Ok(_) => {
                // Empty hydration status - deployment has no clusters
                // Don't display anything
            }
            Err(_) => {
                // Error getting hydration status - don't block display, just skip
            }
        }

        for sq in &deployment.schemas {
            humanln!("    {}.{}", sq.database.dimmed(), sq.schema);
        }
        humanln!();
    }

    Ok(())
}
