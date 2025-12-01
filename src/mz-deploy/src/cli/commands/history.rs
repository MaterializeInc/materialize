//! History command - show deployment history in chronological order.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use chrono::{DateTime, Local};
use owo_colors::OwoColorize;

/// Show deployment history in chronological order (promoted deployments only).
///
/// This command:
/// - Queries all promoted deployments (promoted_at IS NOT NULL) ordered by promoted_at DESC
/// - Groups deployments by environment and promotion time
/// - Lists all schemas included in each deployment
///
/// Similar to `git log` - shows historical production deployment activity with
/// each deployment showing the "commit message" (schemas changed).
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `limit` - Optional limit on number of deployments to show
///
/// # Returns
/// Ok(()) if listing succeeds
///
/// # Errors
/// Returns `CliError::Connection` for database errors
pub async fn run(
    profile: &Profile,
    limit: Option<usize>,
) -> Result<(), CliError> {
    // Connect to database
    let client = helpers::connect_to_database(profile).await?;

    // Get deployment history (promoted only, grouped by deployment)
    let history = client.list_deployment_history(limit).await?;

    if history.is_empty() {
        println!("No deployment history found.");
        println!();
        println!("To create and promote a deployment, run:");
        println!("  {} {} {}", "mz-deploy".cyan(), "stage".cyan(), ".".cyan());
        println!("  {} {} {}", "mz-deploy".cyan(), "apply".cyan(), "--staging-env <name>".cyan());
        return Ok(());
    }

    println!("Deployment history (promoted):");
    println!();

    for (environment, promoted_at, deployed_by, schemas) in history {
        // Convert SystemTime to DateTime for formatting
        let datetime: DateTime<Local> = promoted_at.into();
        let date_str = datetime.format("%a %b %d %H:%M:%S %Y %z").to_string();

        // Display deployment header (like a git commit)
        println!(
            "{} {}",
            "deployment".yellow().bold(),
            environment.cyan()
        );
        println!(
            "{}: {}",
            "Promoted by".dimmed(),
            deployed_by.yellow()
        );
        println!(
            "{}:   {}",
            "Date".dimmed(),
            date_str
        );
        println!();

        // List all schemas in this deployment (like files in a git commit)
        for (database, schema) in schemas {
            println!("    {}.{}", database.dimmed(), schema);
        }
        println!();
    }

    Ok(())
}
