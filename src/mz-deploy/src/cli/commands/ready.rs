//! Ready command - wait for staging deployment cluster hydration.

use crate::cli::{CliError, helpers};
use crate::client::Profile;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use std::collections::HashMap;
use std::time::Duration;

/// Wait for a staging deployment to become ready by monitoring hydration status.
///
/// This command:
/// - Validates the staging deployment exists and hasn't been promoted
/// - Subscribes to cluster hydration status
/// - Shows progress bars tracking hydration for each cluster
/// - Exits when all clusters are hydrated or timeout is reached
///
/// # Arguments
/// * `profile` - Database profile containing connection information
/// * `deploy_id` - Staging deployment ID
/// * `snapshot` - If true, check once and exit; if false, track continuously
/// * `timeout` - Optional timeout in seconds
///
/// # Returns
/// Ok(()) if deployment becomes ready
///
/// # Errors
/// Returns `CliError::StagingEnvironmentNotFound` if deployment doesn't exist
/// Returns `CliError::StagingAlreadyPromoted` if already promoted
/// Returns `CliError::ReadyTimeout` if timeout is reached
pub async fn run(
    profile: &Profile,
    deploy_id: &str,
    snapshot: bool,
    timeout: Option<u64>,
) -> Result<(), CliError> {
    // Connect to database
    let mut client = helpers::connect_to_database(profile).await?;

    // Validate staging deployment exists and is not promoted
    let metadata = client.get_deployment_metadata(deploy_id).await?;
    match metadata {
        Some(meta) if meta.promoted_at.is_some() => {
            return Err(CliError::StagingAlreadyPromoted {
                name: deploy_id.to_string(),
            });
        }
        Some(_) => {}
        None => {
            return Err(CliError::StagingEnvironmentNotFound {
                name: deploy_id.to_string(),
            });
        }
    }

    if snapshot {
        // Snapshot mode: query once and display status
        run_snapshot(deploy_id, &client).await
    } else {
        // Continuous mode: subscribe and track with progress bars
        run_continuous(deploy_id, &mut client, timeout).await
    }
}

/// Run in snapshot mode: query hydration status once and display.
async fn run_snapshot(deploy_id: &str, client: &crate::client::Client) -> Result<(), CliError> {
    let status = client.get_deployment_hydration_status(deploy_id).await?;

    if status.is_empty() {
        println!("No clusters found in staging deployment '{}'", deploy_id);
        return Ok(());
    }

    println!("Hydration status for deployment '{}':", deploy_id);
    println!();

    let mut all_ready = true;
    let mut cluster_names: Vec<_> = status.keys().collect();
    cluster_names.sort();

    for cluster_name in cluster_names {
        let (hydrated, total) = status[cluster_name];
        let percentage = if total > 0 {
            (hydrated as f64 / total as f64 * 100.0) as u8
        } else {
            100
        };

        let status_str = if hydrated >= total {
            "✓ Ready".green().to_string()
        } else {
            all_ready = false;
            "⋯ Hydrating".yellow().to_string()
        };

        println!(
            "  {} {}: {}/{} ({}%)",
            status_str,
            cluster_name.cyan(),
            hydrated,
            total,
            percentage
        );
    }

    println!();

    if all_ready {
        println!("{}", "All clusters are ready!".green().bold());
        Ok(())
    } else {
        Err(CliError::ClustersHydrating)
    }
}

/// Run in continuous mode: subscribe to hydration updates and show progress bars.
async fn run_continuous(
    deploy_id: &str,
    client: &mut crate::client::Client,
    timeout: Option<u64>,
) -> Result<(), CliError> {
    // Get initial hydration status
    let initial_status = client.get_deployment_hydration_status(deploy_id).await?;

    if initial_status.is_empty() {
        println!("No clusters found in staging deployment '{}'", deploy_id);
        return Ok(());
    }

    // Check if already fully hydrated
    let all_ready = initial_status
        .values()
        .all(|(hydrated, total)| hydrated >= total);

    if all_ready {
        println!("{}", "All clusters are already ready!".green().bold());
        return Ok(());
    }

    println!("Waiting for deployment '{}' to be ready...", deploy_id);
    println!();

    // Set up progress bars
    let multi = MultiProgress::new();
    let style = ProgressStyle::default_bar()
        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} {msg}")
        .expect("Invalid progress bar template")
        .progress_chars("#>-");

    let mut progress_bars = HashMap::new();
    let mut cluster_names: Vec<_> = initial_status.keys().cloned().collect();
    cluster_names.sort();

    for cluster_name in cluster_names {
        let (hydrated, total) = initial_status[&cluster_name];
        let pb = multi.add(ProgressBar::new(total as u64));
        pb.set_style(style.clone());
        pb.set_message(cluster_name.clone());
        pb.set_position(hydrated as u64);
        progress_bars.insert(cluster_name, pb);
    }

    // Subscribe to hydration updates and monitor
    let monitor_future = monitor_hydration(deploy_id, client, progress_bars);

    if let Some(secs) = timeout {
        match tokio::time::timeout(Duration::from_secs(secs), monitor_future).await {
            Ok(result) => result,
            Err(_) => Err(CliError::ReadyTimeout {
                name: deploy_id.to_string(),
                seconds: secs,
            }),
        }
    } else {
        monitor_future.await
    }
}

/// Monitor hydration status via SUBSCRIBE and update progress bars.
async fn monitor_hydration(
    deploy_id: &str,
    client: &mut crate::client::Client,
    progress_bars: HashMap<String, ProgressBar>,
) -> Result<(), CliError> {
    let txn = client.subscribe_deployment_hydration(deploy_id).await?;

    loop {
        // Fetch next batch of updates
        let rows = txn
            .query("FETCH ALL c", &[])
            .await
            .map_err(|e| CliError::Message(format!("Failed to fetch subscription data: {}", e)))?;

        for row in rows {
            // Parse SUBSCRIBE row format:
            // [mz_timestamp, mz_diff, name, hydrated, total]
            let mz_diff: i64 = row.get(1);

            if mz_diff == -1 {
                // Retraction - ignore
                continue;
            }

            // mz_diff = +1: This is the current state
            let cluster_name: String = row.get(2);
            let hydrated: i64 = row.get(3);
            let total: i64 = row.get(4);

            if let Some(pb) = progress_bars.get(&cluster_name) {
                pb.set_length(total as u64);
                pb.set_position(hydrated as u64);
            }
        }

        // Check if all clusters are ready
        let all_ready = progress_bars
            .values()
            .all(|pb| pb.position() >= pb.length().unwrap_or(0));

        if all_ready {
            // Finish all progress bars
            for pb in progress_bars.values() {
                pb.finish();
            }
            println!();
            println!("{}", "All clusters are ready!".green().bold());
            return Ok(());
        }

        // Small sleep to avoid busy-waiting
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
