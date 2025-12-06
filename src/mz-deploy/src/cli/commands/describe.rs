//! Describe command - show detailed information about a specific deployment.

use crate::cli::CliError;
use crate::client::{Client, Profile};
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
pub async fn run(profile: &Profile, deploy_id: &str) -> Result<(), CliError> {
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    client.create_deployments().await?;

    // Get deployment metadata
    let metadata = client.get_deployment_details(deploy_id).await?;
    let Some((deployed_at, promoted_at, deployed_by, commit, kind, schemas)) = metadata else {
        return Err(CliError::Message(format!(
            "Deployment '{}' not found",
            deploy_id
        )));
    };

    // Get deployment objects
    let snapshot = client.get_deployment_objects(Some(deploy_id)).await?;

    // Display deployment header
    println!(
        "{} {} [{}]",
        "deployment".yellow().bold(),
        deploy_id.cyan(),
        kind.dimmed()
    );

    if let Some(commit_sha) = commit {
        println!("{}: {}", "Commit".dimmed(), commit_sha);
    }

    println!("{}: {}", "Deployed by".dimmed(), deployed_by.yellow());

    let deployed_datetime: DateTime<Local> = deployed_at.into();
    let deployed_str = deployed_datetime
        .format("%a %b %d %H:%M:%S %Y %z")
        .to_string();
    println!("{}: {}", "Deployed at".dimmed(), deployed_str);

    if let Some(promoted) = promoted_at {
        if kind == "objects" {
            let promoted_datetime: DateTime<Local> = promoted.into();
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
    println!("{} ({}):", "Schemas".bold(), schemas.len());
    for (database, schema) in &schemas {
        println!("    {}.{}", database.dimmed(), schema);
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
