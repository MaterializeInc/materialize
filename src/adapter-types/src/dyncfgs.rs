// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the adapter layer.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

pub const ALLOW_USER_SESSIONS: Config<bool> = Config::new(
    "allow_user_sessions",
    true,
    "Whether to allow user roles to create new sessions. When false, only system roles will be permitted to create new sessions.",
);

pub const ENABLE_0DT_DEPLOYMENT: Config<bool> = Config::new(
    "enable_0dt_deployment",
    false,
    "Whether to enable zero-downtime deployments (experimental).",
);

// Slightly awkward with the WITH prefix, but we can't start with a 0.
pub const WITH_0DT_DEPLOYMENT_MAX_WAIT: Config<Duration> = Config::new(
    "with_0dt_deployment_max_wait",
    Duration::from_secs(5 * 60),
    "How long to wait at most for clusters to be hydrated, when doing a zero-downtime deployment.",
);

pub const WITH_0DT_DEPLOYMENT_HYDRATION_CHECK_INTERVAL: Config<Duration> = Config::new(
    "0dt_deployment_hydration_check_interval",
    Duration::from_secs(10),
    "Interval at which to check cluster hydration status, when doing zero-downtime deployment.",
);

/// Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.
pub const ENABLE_STATEMENT_LIFECYCLE_LOGGING: Config<bool> = Config::new(
    "enable_statement_lifecycle_logging",
    false,
    "Enable logging of statement lifecycle events in mz_internal.mz_statement_lifecycle_history.",
);

/// Enable installation of introspection subscribes.
pub const ENABLE_INTROSPECTION_SUBSCRIBES: Config<bool> = Config::new(
    "enable_introspection_subscribes",
    true,
    "Enable installation of introspection subscribes.",
);

/// The plan insights notice will not investigate fast path clusters if plan optimization took longer than this.
pub const PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION: Config<Duration> = Config::new(
    "plan_insights_notice fast_path_clusters_optimize_duration",
    // Looking at production values of the mz_optimizer_e2e_optimization_time_seconds metric, most
    // optimizations run faster than 10ms, so this should still work well for most queries. We want
    // to avoid the case where an optimization took just under this value and there are lots of
    // clusters, so the extra delay to produce the plan insights notice will take the optimization
    // time * the number of clusters longer.
    Duration::from_millis(10),
    "Enable plan insights fast path clusters calculation if the optimize step took less than this duration.",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&ENABLE_0DT_DEPLOYMENT)
        .add(&WITH_0DT_DEPLOYMENT_MAX_WAIT)
        .add(&WITH_0DT_DEPLOYMENT_HYDRATION_CHECK_INTERVAL)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
        .add(&ENABLE_INTROSPECTION_SUBSCRIBES)
        .add(&PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION)
}
