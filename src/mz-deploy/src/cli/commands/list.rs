//! Deployments command - list active staging deployments.

use crate::cli::CliError;
use crate::client::{Client, ClusterDeploymentStatus, ClusterStatusContext, DeploymentKind};
use crate::config::Settings;
use crate::log;
use crate::project::SchemaQualifier;
use chrono::{DateTime, Utc};
use owo_colors::OwoColorize;
use std::fmt;

#[derive(serde::Serialize)]
#[serde(transparent)]
struct ListOutput {
    deployments: Vec<ListDeployment>,
}

#[derive(serde::Serialize)]
struct ListDeployment {
    deploy_id: String,
    deployed_at: DateTime<Utc>,
    deployed_by: String,
    git_commit: Option<String>,
    kind: DeploymentKind,
    schemas: Vec<SchemaQualifier>,
    clusters: Vec<ClusterStatusContext>,
}

impl fmt::Display for ListOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.deployments.is_empty() {
            writeln!(f, "No active staging deployments.")?;
            writeln!(f)?;
            writeln!(f, "To create a staging deployment, run:")?;
            writeln!(
                f,
                "  {} {} {}",
                "mz-deploy".cyan(),
                "stage".cyan(),
                ".".cyan()
            )?;
            return Ok(());
        }

        writeln!(f, "Active staging deployments:")?;
        writeln!(f)?;

        for deployment in &self.deployments {
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

            writeln!(
                f,
                "  {} {} by {} {} [{}]",
                "●".green(),
                deployment.deploy_id.cyan().bold(),
                deployment.deployed_by.yellow(),
                format!("({})", timestamp).dimmed(),
                deployment.kind.to_string().dimmed()
            )?;

            // Display commit if available
            if let Some(commit_sha) = &deployment.git_commit {
                writeln!(f, "    commit: {}", commit_sha.dimmed())?;
            }

            // Display cluster status
            if !deployment.clusters.is_empty() {
                let mut ready_count = 0i64;
                #[allow(clippy::as_conversions)]
                let total_clusters = deployment.clusters.len() as i64;

                for ctx in &deployment.clusters {
                    if matches!(ctx.status, ClusterDeploymentStatus::Ready) {
                        ready_count += 1;
                    }
                }

                let text = if ready_count == total_clusters {
                    "clusters: all ready".to_string()
                } else {
                    format!("clusters: {} of {} ready", ready_count, total_clusters)
                };
                writeln!(f, "    {}\n", text.blue())?;
            }

            for sq in &deployment.schemas {
                writeln!(f, "    {}.{}", sq.database.dimmed(), sq.schema)?;
            }
            writeln!(f)?;
        }

        Ok(())
    }
}

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
) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.deployments().create_deployments().await?;
    let deployments = client.deployments().list_staging_deployments().await?;

    let mut env_names: Vec<_> = deployments.keys().collect();
    env_names.sort();

    let mut list_deployments = Vec::new();
    for env_name in env_names {
        let deployment = &deployments[env_name];
        let clusters = client
            .deployments()
            .get_deployment_hydration_status_with_lag(env_name, allowed_lag_secs)
            .await
            .unwrap_or_default();
        list_deployments.push(ListDeployment {
            deploy_id: env_name.clone(),
            deployed_at: deployment.deployed_at,
            deployed_by: deployment.deployed_by.clone(),
            git_commit: deployment.git_commit.clone(),
            kind: deployment.kind.clone(),
            schemas: deployment.schemas.clone(),
            clusters,
        });
    }

    let output = ListOutput {
        deployments: list_deployments,
    };
    log::output(&output);

    Ok(())
}
