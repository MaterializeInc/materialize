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
use mz_clusterd_test_driver::script;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_types::PersistLocation;
use tokio::io::BufReader;

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

    // Read the script from `DRIVER_SCRIPT` if set, else stdin.
    match std::env::var("DRIVER_SCRIPT") {
        Ok(path) => {
            let file = tokio::fs::File::open(&path)
                .await
                .with_context(|| format!("opening DRIVER_SCRIPT {path}"))?;
            script::run(driver, loc, BufReader::new(file)).await
        }
        Err(_) => script::run(driver, loc, BufReader::new(tokio::io::stdin())).await,
    }
}
