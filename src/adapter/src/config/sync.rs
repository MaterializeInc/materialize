// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use tokio::time;

use crate::config::{
    SynchronizedParameters, SystemParameterBackend, SystemParameterFrontend,
    SystemParameterSyncConfig,
};
use crate::Client;

/// Run a loop that periodically pulls system parameters defined in the
/// LaunchDarkly-backed [SystemParameterFrontend] and pushes modified values to the
/// `ALTER SYSTEM`-backed [SystemParameterBackend].
pub async fn system_parameter_sync(
    sync_config: SystemParameterSyncConfig,
    adapter_client: Client,
    tick_interval: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let Some(tick_interval) = tick_interval else {
        tracing::info!("skipping system parameter sync as tick_interval = None");
        return Ok(());
    };

    // Ensure the frontend client is initialized.
    let mut frontend = Option::<SystemParameterFrontend>::None; // lazy initialize the frontend below
    let mut backend = SystemParameterBackend::new(adapter_client).await?;

    // Tick every `tick_duration` ms, skipping missed ticks.
    let mut interval = time::interval(tick_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // Run the synchronization loop.
    tracing::info!(
        "synchronizing system parameter values every {} seconds",
        tick_interval.as_secs()
    );

    let mut params = SynchronizedParameters::default();
    loop {
        // Wait for the next sync period
        interval.tick().await;

        // Fetch current parameter values from the backend
        backend.pull(&mut params).await;

        if !params.enable_launchdarkly() {
            if frontend.is_some() {
                tracing::info!("stopping system parameter frontend");
                frontend = None;
            } else {
                tracing::info!("system parameter sync is disabled; not syncing")
            }

            // Don't do anything until the next loop.
            continue;
        } else {
            if frontend.is_none() {
                tracing::info!("initializing system parameter frontend");
                frontend = Some(SystemParameterFrontend::from(&sync_config).await?);
            }

            // Pull latest state from frontend and push changes to backend.
            let frontend = frontend.as_ref().expect("frontend exists");
            if frontend.pull(&mut params) {
                backend.push(&mut params).await;
            }
        }
    }
}
