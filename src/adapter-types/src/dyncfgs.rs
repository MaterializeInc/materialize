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

pub const ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT: Config<bool> = Config::new(
    "enable_0dt_deployment_panic_after_timeout",
    false,
    "Whether to panic if the maximum wait time is reached but preflight checks have not succeeded.",
);

pub const WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL: Config<Duration> = Config::new(
    // The feature flag name is historical.
    "0dt_deployment_hydration_check_interval",
    Duration::from_secs(10),
    "Interval at which to check whether clusters are caught up, when doing zero-downtime deployment.",
);

pub const ENABLE_0DT_CAUGHT_UP_CHECK: Config<bool> = Config::new(
    "enable_0dt_caught_up_check",
    true,
    "Whether to determine rehydration using a more complicated method that compares collection write frontiers against an allowed lag behind wall-clock time.",
);

pub const WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_allowed_lag",
    Duration::from_secs(60),
    "Maximum allowed lag when determining whether collections are caught up for 0dt deployments.",
);

pub const WITH_0DT_CAUGHT_UP_CHECK_CUTOFF: Config<Duration> = Config::new(
    "with_0dt_caught_up_check_cutoff",
    Duration::from_secs(2 * 60 * 60), // 2 hours
    "Collections whose write frontier is behind 'now' by more than the cutoff are ignored when doing caught-up checks for 0dt deployments.",
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

/// Whether the default sink partitioning strategy for an environment should be 'v1'. When set to
/// false the strategy defaults to 'v0'.
pub const DEFAULT_SINK_PARTITION_STRATEGY: Config<&str> = Config::new(
    "default_sink_partition_strategy",
    "v0",
    "The default sink partitioning strategy for an environment. It defaults to 'v0'.",
);

/// Whether to create system builtin continual tasks on boot.
pub const ENABLE_CONTINUAL_TASK_BUILTINS: Config<bool> = Config::new(
    "enable_continual_task_builtins",
    false,
    "Create system builtin continual tasks on boot.",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ALLOW_USER_SESSIONS)
        .add(&ENABLE_0DT_DEPLOYMENT)
        .add(&WITH_0DT_DEPLOYMENT_MAX_WAIT)
        .add(&ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT)
        .add(&WITH_0DT_DEPLOYMENT_CAUGHT_UP_CHECK_INTERVAL)
        .add(&ENABLE_0DT_CAUGHT_UP_CHECK)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_ALLOWED_LAG)
        .add(&WITH_0DT_CAUGHT_UP_CHECK_CUTOFF)
        .add(&ENABLE_STATEMENT_LIFECYCLE_LOGGING)
        .add(&ENABLE_INTROSPECTION_SUBSCRIBES)
        .add(&PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION)
        .add(&DEFAULT_SINK_PARTITION_STRATEGY)
        .add(&ENABLE_CONTINUAL_TASK_BUILTINS)
}
