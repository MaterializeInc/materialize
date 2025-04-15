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

pub const WALLCLOCK_LAG_RECORDING_INTERVAL: Config<Duration> = Config::new(
    "wallclock_lag_recording_interval",
    Duration::from_secs(60),
    "The interval at which to record `WallclockLagHistory` introspection.",
);

pub const ENABLE_WALLCLOCK_LAG_HISTOGRAM_COLLECTION: Config<bool> = Config::new(
    "enable_wallclock_lag_histogram_collection",
    true,
    "Whether to record `WallclockLagHistogram` introspection.",
);

pub const WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL: Config<Duration> = Config::new(
    "wallclock_lag_histogram_period_interval",
    Duration::from_secs(24 * 60 * 60),
    "The period interval of histograms in `WallclockLagHistogram` introspection.",
);

pub const ENABLE_TIMELY_ZERO_COPY: Config<bool> = Config::new(
    "enable_timely_zero_copy",
    false,
    "Enable the zero copy allocator (timely dataflow).",
);

pub const ENABLE_TIMELY_ZERO_COPY_LGALLOC: Config<bool> = Config::new(
    "enable_timely_zero_copy_lgalloc",
    false,
    "Enable backing the zero copy allocator with lgalloc (timely dataflow).",
);

pub const TIMELY_ZERO_COPY_LIMIT: Config<Option<usize>> = Config::new(
    "timely_zero_copy_limit",
    None,
    "Optional limit of the zero copy allocator in allocations (timely dataflow).",
);

/// Adds the full set of all controller `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_SOURCES)
        .add(&WALLCLOCK_LAG_RECORDING_INTERVAL)
        .add(&ENABLE_WALLCLOCK_LAG_HISTOGRAM_COLLECTION)
        .add(&WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL)
        .add(&ENABLE_TIMELY_ZERO_COPY)
        .add(&ENABLE_TIMELY_ZERO_COPY_LGALLOC)
        .add(&TIMELY_ZERO_COPY_LIMIT)
}
