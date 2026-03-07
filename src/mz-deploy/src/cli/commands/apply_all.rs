//! Apply-all orchestrator — runs all infrastructure apply steps in dependency order.
//!
//! Dependency order: clusters → roles → network policies → secrets → connections → sources → tables.

use crate::cli::CliError;
use crate::cli::progress;
use crate::client::Profile;
use crate::config::ProjectSettings;
use std::path::Path;

/// Run all infrastructure apply steps in dependency order.
///
/// Applies: clusters → roles → network policies → secrets (unless skipped) → connections → sources → tables.
/// Each step prints a header and delegates to the existing module's `run()` function.
pub async fn run(
    directory: &Path,
    profile: &Profile,
    settings: &ProjectSettings,
    skip_secrets: bool,
    dry_run: bool,
) -> Result<(), CliError> {
    progress::info("Applying all infrastructure objects...");

    // 1. Clusters
    progress::info("--- Applying clusters ---");
    super::clusters::run(directory, profile, dry_run).await?;

    // 2. Roles
    progress::info("--- Applying roles ---");
    super::roles::run(directory, profile, dry_run).await?;

    // 3. Network Policies
    progress::info("--- Applying network policies ---");
    super::apply_network_policies::run(directory, profile, dry_run).await?;

    // 4. Secrets (unless skipped)
    if skip_secrets {
        progress::info("--- Skipping secrets (--skip-secrets) ---");
    } else {
        progress::info("--- Applying secrets ---");
        super::apply_secrets::run(directory, profile, settings, dry_run).await?;
    }

    // 5. Connections
    progress::info("--- Applying connections ---");
    super::apply_connections::run(directory, profile, settings, dry_run).await?;

    // 6. Sources
    progress::info("--- Applying sources ---");
    super::apply_tables::apply_sources(directory, profile, settings, dry_run).await?;

    // 7. Tables
    progress::info("--- Applying tables ---");
    super::apply_tables::apply_tables(directory, profile, settings, dry_run).await?;

    // Regenerate data contracts after tables are applied
    if !dry_run {
        super::gen_data_contracts::run(profile, directory).await?;
    }

    progress::success("All infrastructure objects applied successfully!");
    Ok(())
}
