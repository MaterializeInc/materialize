// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use http::StatusCode;
use mz_ore::retry::Retry;

use crate::action::State;

/// Level of consistency checks we should enable on a testdrive run.
#[derive(clap::ValueEnum, Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum Level {
    /// Run the consistency checks after the completion of a test file.
    #[default]
    File,
    /// Run the consistency checks after each statement, good for debugging.
    Statement,
    /// Disable consistency checks entirely.
    Disable,
}

impl FromStr for Level {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "file" => Ok(Level::File),
            "statement" => Ok(Level::Statement),
            "disable" => Ok(Level::Disable),
            s => Err(format!("Unknown consistency check level: {s}")),
        }
    }
}

/// Runs consistency checks against multiple parts of Materialize to make sure we haven't violated
/// our invariants or leaked resources.
pub async fn run_consistency_checks(state: &State) -> Result<(), anyhow::Error> {
    let coordinator = check_coordinator(state).await.context("coordinator");
    let catalog_state = check_catalog_state(state).await.context("catalog state");
    // TODO(parkmycar): Fix subsources so they don't leak their shards and then add a leaked shards
    // consistency check.

    // Make sure to report all inconsistencies, not just the first.
    let mut msg = String::new();
    if let Err(e) = coordinator {
        writeln!(&mut msg, "coordinator inconsistency: {e:?}")?;
    }
    if let Err(e) = catalog_state {
        writeln!(&mut msg, "catalog inconsistency: {e:?}")?;
    }

    if msg.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{msg}"))
    }
}

/// Asks the Coordinator to run it's own internal consistency checks.
async fn check_coordinator(state: &State) -> Result<(), anyhow::Error> {
    let response = Retry::default()
        .max_duration(Duration::from_secs(2))
        .retry_async(|_| async {
            reqwest::get(&format!(
                "http://{}/api/coordinator/check",
                state.materialize_internal_http_addr,
            ))
            .await
        })
        .await
        .context("querying coordinator")?;
    if response.status() == StatusCode::NOT_FOUND {
        tracing::info!("Coordinator consistency check not available");
        return Ok(());
    }

    let inconsistencies: serde_json::Value =
        response.json().await.context("deserialize response")?;

    match inconsistencies {
        serde_json::Value::String(x) if x.is_empty() => Ok(()),
        other => Err(anyhow!("coordinator inconsistencies! {other:?}")),
    }
}

/// Checks that the in-memory catalog matches what we have persisted on disk.
async fn check_catalog_state(state: &State) -> Result<(), anyhow::Error> {
    let maybe_disk_catalog = state
        .with_catalog_copy(|catalog| catalog.state().clone())
        .await
        .map_err(|e| anyhow!("failed to read on-disk catalog state: {e}"))?
        .map(|catalog| catalog.dump().expect("state must be dumpable"));
    let Some(disk_catalog) = maybe_disk_catalog else {
        tracing::warn!("No Catalog state on disk, skipping consistency check");
        return Ok(());
    };

    let memory_catalog = reqwest::get(&format!(
        "http://{}/api/catalog/dump",
        state.materialize_internal_http_addr,
    ))
    .await
    .context("GET catalog")?
    .text()
    .await
    .context("deserialize catalog")?;

    if disk_catalog != memory_catalog {
        // The state objects here are around 100k lines pretty printed, so find the
        // first lines that differs and show context around it.
        let diff = similar::TextDiff::from_lines(&memory_catalog, &disk_catalog)
            .unified_diff()
            .context_radius(50)
            .to_string()
            .lines()
            .take(200)
            .collect::<Vec<_>>()
            .join("\n");

        bail!("the in-memory state of the catalog does not match its on-disk state:\n{diff}");
    }

    Ok(())
}
