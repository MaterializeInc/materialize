// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Headless driver entry point for `mzcompose`. Connects to a running `clusterd`,
//! hosts persist PubSub, and runs a JSON command script against it (see
//! [`mz_clusterd_test_driver::script`]), exiting non-zero on assertion failure.
//!
//! The script source is the file named by `DRIVER_SCRIPT`, or stdin when that is
//! unset. Connection and persist configuration come from the environment:
//! `CLUSTERD_COMPUTE_ADDR`, `PERSIST_BLOB_URL`, `PERSIST_CONSENSUS_URL`, and
//! `DRIVER_PUBSUB_BIND`.

use std::net::SocketAddr;

use anyhow::Context;
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_clusterd_test_driver::runner::WorkloadRunner;
use mz_clusterd_test_driver::script;
use mz_clusterd_test_driver::surface::render_cells;
use mz_clusterd_test_driver::workload::Workload;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_types::PersistLocation;
use tokio::io::AsyncReadExt;

/// Connect to `clusterd` and host persist PubSub, reading configuration from the
/// environment. Returns the persist location (for dataflow imports) and a
/// connected [`Driver`].
async fn setup() -> anyhow::Result<(PersistLocation, Driver)> {
    let compute_addr =
        std::env::var("CLUSTERD_COMPUTE_ADDR").unwrap_or_else(|_| "clusterd:2101".to_string());
    let blob = std::env::var("PERSIST_BLOB_URL").expect("PERSIST_BLOB_URL");
    let consensus = std::env::var("PERSIST_CONSENSUS_URL").expect("PERSIST_CONSENSUS_URL");
    let pubsub_bind: SocketAddr = std::env::var("DRIVER_PUBSUB_BIND")
        .unwrap_or_else(|_| "0.0.0.0:6879".to_string())
        .parse()?;

    let loc = PersistLocation {
        blob_uri: blob.parse()?,
        consensus_uri: consensus.parse()?,
    };
    let host = PersistHost::start_on(pubsub_bind, loc.clone()).await?;
    let driver = Driver::connect(host, &compute_addr).await?;
    Ok((loc, driver))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configure tracing so the driver emits structured logs like the real
    // Materialize binaries.
    let _tracing_handle = TracingCliArgs::default()
        .configure_tracing(
            StaticTracingConfig {
                service_name: "headless-driver",
                build_info: mz_persist_client::BUILD_INFO,
            },
            MetricsRegistry::new(),
        )
        .await?;

    let (loc, driver) = setup().await?;

    // `DRIVER_WORKLOADS` selects the generated-corpus mode: a directory of JSON
    // workloads, each rendered and checked by its own oracles. It takes precedence
    // over `DRIVER_SCRIPT` so a single image serves both suites.
    if let Ok(dir) = std::env::var("DRIVER_WORKLOADS") {
        return run_workloads(driver, loc, std::path::Path::new(&dir)).await;
    }

    // Read the script from `DRIVER_SCRIPT` if set, else stdin. The path is passed
    // through so a `REWRITE` run can rewrite the file in place.
    match std::env::var("DRIVER_SCRIPT") {
        Ok(path) => {
            let content = tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("reading DRIVER_SCRIPT {path}"))?;
            script::run(driver, loc, &content, Some(std::path::Path::new(&path))).await
        }
        Err(_) => {
            let mut content = String::new();
            tokio::io::stdin().read_to_string(&mut content).await?;
            script::run(driver, loc, &content, None).await
        }
    }
}

/// Run every JSON workload in `dir`, reporting per-workload results and the
/// surface cells the run covered.
///
/// All workloads run against one connection: the runner reconciles to an empty
/// compute state per configuration anyway, so a fresh process per workload would
/// only cost startup. Every failure is collected rather than aborting on the
/// first, so one run reports the full set of broken workloads instead of hiding
/// the rest behind the earliest.
async fn run_workloads(
    driver: Driver,
    loc: PersistLocation,
    dir: &std::path::Path,
) -> anyhow::Result<()> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("reading workload directory {}", dir.display()))?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    // Deterministic order, so a run is reproducible and a failure list is stable.
    paths.sort();
    anyhow::ensure!(
        !paths.is_empty(),
        "no workloads found in {}; a run that silently checks nothing is worse \
         than a failing one",
        dir.display()
    );

    let mut runner = WorkloadRunner::new(driver, loc).await?;
    let mut covered = std::collections::BTreeSet::new();
    let mut failures = Vec::new();
    for path in &paths {
        let json =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let workload: Workload =
            serde_json::from_str(&json).with_context(|| format!("parsing {}", path.display()))?;
        match runner.run(&workload).await {
            Ok(outcome) => {
                covered.extend(outcome.realized_cells);
                println!(
                    "ok: {} ({} config(s))",
                    outcome.name,
                    outcome.per_config.len()
                );
            }
            Err(e) => {
                println!(
                    "FAILED: {} (seed {:?}): {e:#}",
                    workload.name, workload.seed
                );
                failures.push(workload.name.clone());
            }
        }
    }

    println!("\nsurface cells covered by this run ({}):", covered.len());
    println!("{}", render_cells(&covered));

    if !failures.is_empty() {
        anyhow::bail!(
            "{} workload(s) failed: {}",
            failures.len(),
            failures.join(", ")
        );
    }
    Ok(())
}
