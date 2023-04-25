// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use mz_sql::session::vars::{Value, Var, VarInput, ENABLE_LAUNCHDARKLY};
use tokio::time;

mod backend;
mod frontend;
mod params;

pub use backend::SystemParameterBackend;
pub use frontend::SystemParameterFrontend;
pub use params::{ModifiedParameter, SynchronizedParameters};

/// Run a loop that periodically pulls system parameters defined in the
/// LaunchDarkly-backed [SystemParameterFrontend] and pushes modified values to the
/// `ALTER SYSTEM`-backed [SystemParameterBackend].
pub async fn system_parameter_sync(
    frontend: Arc<SystemParameterFrontend>,
    mut backend: SystemParameterBackend,
    tick_interval: Option<Duration>,
) -> Result<(), anyhow::Error> {
    let tick_interval = match tick_interval {
        Some(tick_interval) => tick_interval,
        None => {
            tracing::info!("skipping system parameter sync as tick_interval = None");
            return Ok(());
        }
    };

    // Ensure the frontend client is initialized.
    frontend.ensure_initialized().await;

    // Run the synchronization loop.
    tracing::info!(
        "synchronizing system parameter values every {} seconds",
        tick_interval.as_secs()
    );

    // Tick every `tick_duration` ms, skipping missed ticks.
    let mut interval = time::interval(tick_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    let mut params = SynchronizedParameters::default();
    loop {
        interval.tick().await;
        backend.pull(&mut params).await;
        let launchdarkly_enabled =
            <bool as Value>::parse(VarInput::Flat(&params.get(ENABLE_LAUNCHDARKLY.name())))
                .expect("This is known to be a bool");
        if launchdarkly_enabled && frontend.pull(&mut params) {
            backend.push(&mut params).await;
        }
    }
}
