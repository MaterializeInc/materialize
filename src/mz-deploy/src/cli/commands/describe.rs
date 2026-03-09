//! Describe command - show detailed information about a specific deployment.

use crate::cli::CliError;
use crate::client::{Client, DeploymentKind};
use crate::config::Settings;
use chrono::{DateTime, Local};
use owo_colors::OwoColorize;

/// Show detailed information about a specific deployment.
///
/// This command:
/// - Queries deployment metadata (when deployed, by whom, git commit, etc.)
/// - Lists all objects included in the deployment with their hashes
///
/// Use `mz-deploy history` to see a list of deployment IDs, then use this
/// command to drill into a specific deployment's details.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `deploy_id` - The deployment ID to describe
///
/// # Returns
/// Ok(()) if the deployment is found and displayed
///
/// # Errors
/// Returns `CliError::Connection` for database errors
/// Returns `CliError::Message` if deployment is not found
pub async fn run(settings: &Settings, deploy_id: &str) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.deployments().create_deployments().await?;

    // Get deployment metadata
    let details = client
        .deployments()
        .get_deployment_details(deploy_id)
        .await?;
    let Some(details) = details else {
        return Err(CliError::Message(format!(
            "Deployment '{}' not found",
            deploy_id
        )));
    };

    // Get deployment objects
    let snapshot = client
        .deployments()
        .get_deployment_objects(Some(deploy_id))
        .await?;

    // Display deployment header
    println!(
        "{} {} [{}]",
        "deployment".yellow().bold(),
        deploy_id.cyan(),
        details.kind.to_string().dimmed()
    );

    if let Some(commit_sha) = &details.git_commit {
        println!("{}: {}", "Commit".dimmed(), commit_sha);
    }

    println!(
        "{}: {}",
        "Deployed by".dimmed(),
        details.deployed_by.yellow()
    );

    let deployed_datetime: DateTime<Local> = details.deployed_at.with_timezone(&Local);
    let deployed_str = deployed_datetime
        .format("%a %b %d %H:%M:%S %Y %z")
        .to_string();
    println!("{}: {}", "Deployed at".dimmed(), deployed_str);

    if let Some(promoted) = details.promoted_at {
        if details.kind == DeploymentKind::Objects {
            let promoted_datetime: DateTime<Local> = promoted.with_timezone(&Local);
            let promoted_str = promoted_datetime
                .format("%a %b %d %H:%M:%S %Y %z")
                .to_string();
            println!("{}: {}", "Promoted at".dimmed(), promoted_str);
        }
    } else {
        println!("{}: {}", "Status".dimmed(), "staging".yellow());
    }

    println!();

    // Display schemas
    println!("{} ({}):", "Schemas".bold(), details.schemas.len());
    for sq in &details.schemas {
        println!("    {}.{}", sq.database.dimmed(), sq.schema);
    }
    println!();

    // Display objects
    println!("{} ({}):", "Objects".bold(), snapshot.objects.len());
    for (object_id, hash) in &snapshot.objects {
        println!(
            "    {}.{}.{}  {}",
            object_id.database.dimmed(),
            object_id.schema.dimmed(),
            object_id.object,
            hash.chars().take(12).collect::<String>().dimmed()
        );
    }

    Ok(())
}
