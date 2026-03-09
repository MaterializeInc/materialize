//! Network policies apply command - converge live network policy state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::progress;
use crate::client::{Client, ConnectionError, Profile, quote_identifier};
use crate::project::network_policies::{self, NetworkPolicyDefinition};
use mz_sql_parser::ast::AlterNetworkPolicyStatement;
use owo_colors::OwoColorize;
use std::path::Path;
use std::time::Instant;

/// Run the `network-policies apply` command.
///
/// Loads network policy definitions from `<directory>/network_policies/` and converges
/// the live Materialize state to match: creating missing policies and
/// altering ones whose rules have changed.
pub async fn run(directory: &Path, profile: &Profile, dry_run: bool) -> Result<(), CliError> {
    let start_time = Instant::now();

    // Load network policy definitions
    progress::stage_start("Loading network policy definitions");
    let load_start = Instant::now();
    let definitions = network_policies::load_network_policies(directory, &profile.name)?;

    if definitions.is_empty() {
        println!(
            "  {} No network_policies/ directory or no .sql files found — nothing to do.",
            "info:".blue().bold()
        );
        return Ok(());
    }

    progress::stage_success(
        &format!("Found {} network policy definition(s)", definitions.len()),
        load_start.elapsed(),
    );

    if dry_run {
        for def in &definitions {
            println!("-- Would apply network policy '{}'", def.name);
            println!("{};", def.create_stmt);
            for grant in &def.grants {
                println!("{};", grant);
            }
            for comment in &def.comments {
                println!("{};", comment);
            }
        }
        let total_duration = start_time.elapsed();
        progress::summary("Network policies dry run complete", total_duration);
        return Ok(());
    }

    // Connect to Materialize
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Apply each network policy definition
    for def in &definitions {
        apply_network_policy(&client, def).await?;
    }

    let total_duration = start_time.elapsed();
    progress::summary("Network policies applied successfully", total_duration);

    Ok(())
}

/// Apply a single network policy definition: create if missing, alter if exists,
/// then execute grants and comments.
async fn apply_network_policy(
    client: &Client,
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
        println!(
            "  {} Altering network policy '{}'",
            "~".yellow().bold(),
            policy_name
        );
        let alter_stmt = AlterNetworkPolicyStatement {
            name: def.create_stmt.name.clone(),
            options: def.create_stmt.options.clone(),
        };
        let sql = format!("{}", alter_stmt);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to alter network policy '{}': {}",
                policy_name, e
            )))
        })?;
    } else {
        // Create network policy from the parsed CREATE NETWORK POLICY statement
        println!(
            "  {} Creating network policy '{}'",
            "+".green().bold(),
            policy_name
        );
        let sql = format!("{}", def.create_stmt);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to create network policy '{}': {}",
                policy_name, e
            )))
        })?;
    }

    // Execute GRANT statements (idempotent)
    for grant in &def.grants {
        let sql = format!("{}", grant);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute grant on network policy '{}': {}",
                policy_name, e
            )))
        })?;
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
    grants::execute_revocations(client, &revocations, "network policy", &policy_name).await?;

    // Execute COMMENT statements (idempotent)
    for comment in &def.comments {
        let sql = format!("{}", comment);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute comment on network policy '{}': {}",
                policy_name, e
            )))
        })?;
    }

    Ok(())
}
