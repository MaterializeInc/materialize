// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Headless driver entry point for `mzcompose`. Hosts a native compute runtime,
//! persist PubSub, and runs a command script against it (see
//! [`mz_clusterd_test_driver::script`]), exiting non-zero on assertion failure.
//!
//! The script source is the file named by `DRIVER_SCRIPT`, or stdin when that is
//! unset. Configuration comes from `DRIVER_WORKERS`, `DRIVER_UNIFIED`,
//! `PERSIST_BLOB_URL`, `PERSIST_CONSENSUS_URL`, and `DRIVER_PUBSUB_BIND`.

use std::net::SocketAddr;

use anyhow::Context;
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_clusterd_test_driver::script;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::metrics::MetricsRegistry;
use mz_persist_types::PersistLocation;
use tokio::io::AsyncReadExt;

/// Start the isolated native runtime and its Persist infrastructure.
async fn setup() -> anyhow::Result<(PersistLocation, Driver)> {
    let workers = std::env::var("DRIVER_WORKERS")
        .unwrap_or_else(|_| "1".into())
        .parse()?;
    let unified = std::env::var("DRIVER_UNIFIED")
        .unwrap_or_else(|_| "true".into())
        .parse()?;
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
    let driver = Driver::start(host, workers, unified).await?;
    Ok((loc, driver))
}

#[tokio::main]
async fn main() {
    // Native workers live until process exit. Do not unwind or shut Tokio down
    // into destructors that join these workers, including on assertion failures.
    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        panic_hook(info);
        std::process::exit(1);
    }));
    let code = match run().await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error:#}");
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> anyhow::Result<()> {
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
