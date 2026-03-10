//! Network policies apply command - converge live network policy state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::{DeploymentExecutor, SqlCollector};
use crate::cli::progress;
use crate::client::{Client, quote_identifier};
use crate::config::Settings;
use crate::humanln;
use crate::project::network_policies::{self, NetworkPolicyDefinition};
use mz_sql_parser::ast::AlterNetworkPolicyStatement;
use owo_colors::OwoColorize;
use std::time::Instant;

/// Run the `network-policies apply` command.
///
/// Loads network policy definitions from `<directory>/network_policies/` and converges
/// the live Materialize state to match: creating missing policies and
/// altering ones whose rules have changed.
pub async fn run(
    settings: &Settings,
    dry_run: bool,
    collector: Option<SqlCollector>,
) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    // Load network policy definitions
    progress::stage_start("Loading network policy definitions");
    let load_start = Instant::now();
    let definitions =
        network_policies::load_network_policies(directory, &profile.name, settings.variables())?;

    if definitions.is_empty() {
        humanln!(
            "  {} No network_policies/ directory or no .sql files found — nothing to do.",
            "info:".blue().bold()
        );
        return Ok(());
    }

    progress::stage_success(
        &format!("Found {} network policy definition(s)", definitions.len()),
        load_start.elapsed(),
    );

    // Connect to Materialize
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    let executor = DeploymentExecutor::with_collector(&client, dry_run, collector);

    // Apply each network policy definition
    for def in &definitions {
        apply_network_policy(&client, &executor, def).await?;
    }

    let total_duration = start_time.elapsed();
    if dry_run {
        progress::summary("Network policies dry run complete", total_duration);
    } else {
        progress::summary("Network policies applied successfully", total_duration);
    }

    Ok(())
}

/// Apply a single network policy definition: create if missing, alter if exists,
/// then execute grants, revocations, and comments.
async fn apply_network_policy(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    def: &NetworkPolicyDefinition,
) -> Result<(), CliError> {
    let policy_name = &def.name;

    // Check if network policy already exists
    let exists = client
        .introspection()
        .network_policy_exists(policy_name)
        .await
        .map_err(CliError::Connection)?;

    if exists {
        // ALTER NETWORK POLICY to converge rules
        if !executor.is_dry_run() {
            humanln!(
                "  {} Altering network policy '{}'",
                "~".yellow().bold(),
                policy_name
            );
        }
        let alter_stmt = AlterNetworkPolicyStatement {
            name: def.create_stmt.name.clone(),
            options: def.create_stmt.options.clone(),
        };
        executor.execute_sql(&alter_stmt).await?;
    } else {
        // Create network policy from the parsed CREATE NETWORK POLICY statement
        if !executor.is_dry_run() {
            humanln!(
                "  {} Creating network policy '{}'",
                "+".green().bold(),
                policy_name
            );
        }
        executor.execute_sql(&def.create_stmt).await?;
    }

    // Execute GRANT statements
    for grant in &def.grants {
        executor.execute_sql(grant).await?;
    }

    // Revoke stale grants
    let current_grants = client
        .introspection()
        .get_network_policy_grants(policy_name)
        .await
        .map_err(CliError::Connection)?;
    let desired = grants::desired_grants(&def.grants, &["USAGE"]);
    let revocations = grants::stale_grant_revocations(
        &current_grants,
        &desired,
        "NETWORK POLICY",
        &quote_identifier(policy_name),
    );
    grants::execute_revocations(executor, &revocations, "network policy", &policy_name).await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        executor.execute_sql(comment).await?;
    }

    Ok(())
}
