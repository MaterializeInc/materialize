//! Clusters apply command - converge live cluster state to match definitions.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::progress;
use crate::client::{Client, ClusterOptions, ConnectionError, quote_identifier};
use crate::config::Settings;
use crate::project::clusters::{self, ClusterDefinition, extract_replication_factor, extract_size};
use owo_colors::OwoColorize;
use std::time::Instant;

/// Run the `clusters apply` command.
///
/// Loads cluster definitions from `<directory>/clusters/` and converges
/// the live Materialize state to match: creating missing clusters and
/// altering ones whose configuration has drifted.
pub async fn run(settings: &Settings, dry_run: bool) -> Result<(), CliError> {
    let profile = settings.connection();
    let directory = &settings.directory;
    let start_time = Instant::now();

    // Load cluster definitions
    progress::stage_start("Loading cluster definitions");
    let load_start = Instant::now();
    let definitions = clusters::load_clusters(directory, &profile.name)?;

    if definitions.is_empty() {
        println!(
            "  {} No clusters/ directory or no .sql files found — nothing to do.",
            "info:".blue().bold()
        );
        return Ok(());
    }

    progress::stage_success(
        &format!("Found {} cluster definition(s)", definitions.len()),
        load_start.elapsed(),
    );

    if dry_run {
        for def in &definitions {
            println!("-- Would apply cluster '{}'", def.name);
            println!("{};", def.create_stmt);
            for grant in &def.grants {
                println!("{};", grant);
            }
            for comment in &def.comments {
                println!("{};", comment);
            }
        }
        let total_duration = start_time.elapsed();
        progress::summary("Clusters dry run complete", total_duration);
        return Ok(());
    }

    // Connect to Materialize
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    // Apply each cluster definition
    for def in &definitions {
        apply_cluster(&client, def).await?;
    }

    let total_duration = start_time.elapsed();
    progress::summary("Clusters applied successfully", total_duration);

    Ok(())
}

/// Apply a single cluster definition: create if missing, alter if drifted,
/// then execute grants and comments.
async fn apply_cluster(client: &Client, def: &ClusterDefinition) -> Result<(), CliError> {
    let cluster_name = &def.name;

    // Check if cluster already exists
    let existing = client
        .introspection()
        .get_cluster(cluster_name)
        .await
        .map_err(CliError::Connection)?;

    match existing {
        None => {
            // Create cluster from the parsed CREATE CLUSTER statement
            println!(
                "  {} Creating cluster '{}'",
                "+".green().bold(),
                cluster_name
            );
            let sql = format!("{}", def.create_stmt);
            client.execute(&sql, &[]).await.map_err(|e| {
                CliError::Connection(ConnectionError::Message(format!(
                    "Failed to create cluster '{}': {}",
                    cluster_name, e
                )))
            })?;
        }
        Some(existing_cluster) => {
            // Compare desired vs actual configuration
            let desired_size = extract_size(&def.create_stmt);
            let desired_rf = extract_replication_factor(&def.create_stmt);

            let needs_alter = {
                let size_differs = desired_size.as_deref() != existing_cluster.size.as_deref();
                let rf_differs = desired_rf.map(i64::from) != existing_cluster.replication_factor;
                size_differs || rf_differs
            };

            if needs_alter {
                let size = desired_size.unwrap_or_else(|| {
                    existing_cluster
                        .size
                        .clone()
                        .unwrap_or_else(|| "25cc".to_string())
                });
                let rf = desired_rf.unwrap_or_else(|| {
                    existing_cluster
                        .replication_factor
                        .unwrap_or(1)
                        .try_into()
                        .unwrap_or(1)
                });

                println!(
                    "  {} Altering cluster '{}'",
                    "~".yellow().bold(),
                    cluster_name
                );
                let options = ClusterOptions {
                    size,
                    replication_factor: rf,
                };
                client
                    .provisioning()
                    .alter_cluster(cluster_name, &options)
                    .await
                    .map_err(CliError::Connection)?;
            } else {
                println!(
                    "  {} Cluster '{}' is up to date",
                    "=".dimmed(),
                    cluster_name
                );
            }
        }
    }

    // Execute GRANT statements (idempotent — re-granting is a no-op in Materialize)
    for grant in &def.grants {
        let sql = format!("{}", grant);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute grant on cluster '{}': {}",
                cluster_name, e
            )))
        })?;
    }

    // Revoke stale grants
    let current_grants = client
        .introspection()
        .get_cluster_grants(cluster_name)
        .await
        .map_err(CliError::Connection)?;
    let desired = grants::desired_grants(&def.grants, &["USAGE", "CREATE"]);
    let revocations = grants::stale_grant_revocations(
        &current_grants,
        &desired,
        "CLUSTER",
        &quote_identifier(cluster_name),
    );
    grants::execute_revocations(client, &revocations, "cluster", &cluster_name).await?;

    // Execute COMMENT statements
    for comment in &def.comments {
        let sql = format!("{}", comment);
        client.execute(&sql, &[]).await.map_err(|e| {
            CliError::Connection(ConnectionError::Message(format!(
                "Failed to execute comment on cluster '{}': {}",
                cluster_name, e
            )))
        })?;
    }

    Ok(())
}
