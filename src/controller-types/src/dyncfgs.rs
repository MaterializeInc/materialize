// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the controller.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

/// The interval at which to retry cleaning replicas from past generatinos.
pub const CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL: Config<Duration> = Config::new(
    "controller_past_generation_replica_cleanup_retry_interval",
    Duration::from_secs(300),
    "The interval at which to attempt to retry cleaning up replicas from past generations.",
);

pub const ENABLE_0DT_DEPLOYMENT_SOURCES: Config<bool> = Config::new(
    "enable_0dt_deployment_sources",
    false,
    "Whether to enable zero-downtime deployments for sources that support it (experimental).",
);

/// The interval at which to refresh wallclock lag introspection.
pub const WALLCLOCK_LAG_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "wallclock_lag_refresh_interval",
    Duration::from_secs(60),
    "The interval at which to refresh wallclock lag introspection.",
);

/// Adds the full set of all controller `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_SOURCES)
        .add(&WALLCLOCK_LAG_REFRESH_INTERVAL)
}
