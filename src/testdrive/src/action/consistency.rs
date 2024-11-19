// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::io::Write as _;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use mz_ore::retry::{Retry, RetryResult};
use mz_persist_client::{PersistLocation, ShardId};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

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

/// Skips consistency checks for the current file.
pub fn skip_consistency_checks(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let reason = cmd
        .args
        .string("reason")
        .context("must provide reason for skipping")?;
    tracing::info!(reason, "Skipping consistency checks as requested.");

    state.consistency_checks_adhoc_skip = true;
    Ok(ControlFlow::Continue)
}

/// Runs consistency checks against multiple parts of Materialize to make sure we haven't violated
/// our invariants or leaked resources.
pub async fn run_consistency_checks(state: &State) -> Result<ControlFlow, anyhow::Error> {
    // Return early if the user adhoc disabled consistency checks for the current file.
    if state.consistency_checks_adhoc_skip {
        return Ok(ControlFlow::Continue);
    }

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
        Ok(ControlFlow::Continue)
    } else {
        Err(anyhow!("{msg}"))
    }
}

/// Checks if a shard in Persist has been tombstoned.
///
/// TODO(parkmycar): Run this as part of the consistency checks, instead of as a specific command.
pub async fn run_check_shard_tombstoned(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let shard_id = cmd.args.string("shard-id")?;
    check_shard_tombstoned(state, &shard_id).await?;
    Ok(ControlFlow::Continue)
}

/// Asks the Coordinator to run it's own internal consistency checks.
async fn check_coordinator(state: &State) -> Result<(), anyhow::Error> {
    // Make sure we can dump the Coordinator state.
    let response = reqwest::get(&format!(
        "http://{}/api/coordinator/dump",
        state.materialize.internal_http_addr
    ))
    .await?;
    // We allow NOT_FOUND to support upgrade tests where this endpoint doesn't yet exist.
    if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
        let response: Result<serde_json::Value, _> = response.json().await;
        bail!("Coordinator failed to dump state: {:?}", response);
    }

    // Run the consistency checks.
    let response = Retry::default()
        .max_duration(Duration::from_secs(2))
        .retry_async(|_| async {
            reqwest::get(&format!(
                "http://{}/api/coordinator/check",
                state.materialize.internal_http_addr,
            ))
            .await
        })
        .await
        .context("querying coordinator")?;
    if response.status() == StatusCode::NOT_FOUND {
        bail!("Coordinator consistency check not available");
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
    #[derive(Debug, Deserialize)]
    struct StorageMetadata {
        unfinalized_shards: Option<BTreeSet<String>>,
    }

    #[derive(Debug, Deserialize)]
    struct CatalogDump {
        system_parameter_defaults: Option<BTreeMap<String, String>>,
        storage_metadata: Option<StorageMetadata>,
    }

    // Dump the in-memory catalog state of the Materialize environment that we're
    // connected to.
    let memory_catalog = reqwest::get(&format!(
        "http://{}/api/catalog/dump",
        state.materialize.internal_http_addr,
    ))
    .await
    .context("GET catalog")?
    .text()
    .await
    .context("deserialize catalog")?;

    // Pull out the system parameter defaults from the in-memory catalog, as we
    // need to load the disk catalog with the same defaults.
    let dump: CatalogDump = serde_json::from_str(&memory_catalog).context("decoding catalog")?;

    let Some(system_parameter_defaults) = dump.system_parameter_defaults else {
        // TODO(parkmycar, def-): Ideally this could be an error, but a lot of test suites fail. We
        // should explicitly disable consistency check in these test suites.
        tracing::warn!(
            "Missing system_parameter_defaults in memory catalog state, skipping consistency check"
        );
        return Ok(());
    };

    let unfinalized_shards = dump
        .storage_metadata
        .and_then(|storage_metadata| storage_metadata.unfinalized_shards);

    // Load the on-disk catalog and dump its state.
    let version: semver::Version = state.build_info.version.parse().expect("invalid version");
    let maybe_disk_catalog = state
        .with_catalog_copy(
            system_parameter_defaults,
            version,
            &state.materialize.bootstrap_args,
            // The expression cache can be taxing on the CPU and is unnecessary for consistency checks.
            Some(false),
            |catalog| catalog.state().clone(),
        )
        .await
        .map_err(|e| anyhow!("failed to read on-disk catalog state: {e}"))?
        .map(|catalog| {
            catalog
                // The set of unfinalized shards in the catalog are updated asynchronously by
                // background processes. As a result, the value may legitimately change after
                // fetching the memory catalog but before fetching the disk catalog, causing the
                // comparison to fail. This is a gross hack that always sets the disk catalog's
                // unfinalized shards equal to the memory catalog's unfinalized shards to ignore
                // false negatives. Unfortunately, we also end up ignoring true negatives.
                .dump(unfinalized_shards)
                .expect("state must be dumpable")
        });
    let Some(disk_catalog) = maybe_disk_catalog else {
        // TODO(parkmycar, def-): Ideally this could be an error, but a lot of test suites fail. We
        // should explicitly disable consistency check in these test suites.
        tracing::warn!("No Catalog state on disk, skipping consistency check");
        return Ok(());
    };

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

/// Checks if the provided `shard_id` is a tombstone, returning an error if it's not.
async fn check_shard_tombstoned(state: &State, shard_id: &str) -> Result<(), anyhow::Error> {
    println!("$ check-shard-tombstoned {shard_id}");

    let (Some(consensus_uri), Some(blob_uri)) =
        (&state.persist_consensus_url, &state.persist_blob_url)
    else {
        // TODO(parkmycar): Testdrive on Cloud Test doesn't currently supply the Persist URLs.
        tracing::warn!("Persist consensus or blob URL not known");
        return Ok(());
    };

    let location = PersistLocation {
        blob_uri: blob_uri.clone(),
        consensus_uri: consensus_uri.clone(),
    };
    let client = state
        .persist_clients
        .open(location)
        .await
        .context("openning persist client")?;
    let shard_id = ShardId::from_str(shard_id).map_err(|s| anyhow!("invalid ShardId: {s}"))?;

    // It might take the storage-controller a moment to drop it's handles, so do a couple retries.
    let (_client, result) = Retry::default()
        .max_duration(state.timeout)
        .retry_async_with_state(client, |retry_state, client| async move {
            let inspect_state = client
                .inspect_shard::<mz_repr::Timestamp>(&shard_id)
                .await
                .context("inspecting shard")
                .and_then(|state| serde_json::to_value(state).context("to json"))
                .and_then(|state| {
                    serde_json::from_value::<ShardState>(state).context("to shard state")
                });

            let result = match inspect_state {
                Ok(state) if state.is_tombstone() => RetryResult::Ok(()),
                Ok(state) => {
                    if retry_state.i == 0 {
                        print!("shard isn't tombstoned; sleeping to see if it gets cleaned up.");
                    }
                    if let Some(backoff) = retry_state.next_backoff {
                        if !backoff.is_zero() {
                            print!(" {:.0?}", backoff);
                        }
                    }
                    std::io::stdout().flush().expect("flushing stdout");

                    RetryResult::RetryableErr(anyhow!("non-tombstone state: {state:?}"))
                }
                Result::Err(e) => RetryResult::FatalErr(e),
            };

            (client, result)
        })
        .await;

    result
}

/// Parts of a shard's state that we read to determine if it's a tombstone.
#[derive(Debug, Serialize, Deserialize)]
struct ShardState {
    leased_readers: BTreeMap<String, serde_json::Value>,
    critical_readers: BTreeMap<String, serde_json::Value>,
    writers: BTreeMap<String, serde_json::Value>,
    since: Vec<mz_repr::Timestamp>,
    upper: Vec<mz_repr::Timestamp>,
}

impl ShardState {
    /// Returns if this shard is currently a tombstsone.
    fn is_tombstone(&self) -> bool {
        self.upper.is_empty()
            && self.since.is_empty()
            && self.writers.is_empty()
            && self.leased_readers.is_empty()
            && self.critical_readers.is_empty()
    }
}
