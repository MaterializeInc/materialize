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
    true,
    "Whether to enable zero-downtime deployments for sources that support it (experimental).",
);

pub const WALLCLOCK_LAG_RECORDING_INTERVAL: Config<Duration> = Config::new(
    "wallclock_lag_recording_interval",
    Duration::from_secs(60),
    "The interval at which to record `WallclockLagHistory` introspection.",
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

pub const ENABLE_TIMELY_SPILL: Config<bool> = Config::new(
    "enable_timely_spill",
    true,
    "Enable file-backed spilling for the timely communication merge queue.",
);

pub const TIMELY_SPILL_THRESHOLD_BYTES: Config<usize> = Config::new(
    "timely_spill_threshold_bytes",
    256 << 20,
    "Per-queue byte threshold above which the timely merge queue spills to disk.",
);

pub const TIMELY_SPILL_HEAD_RESERVE_BYTES: Config<usize> = Config::new(
    "timely_spill_head_reserve_bytes",
    64 << 20,
    "Bytes kept in memory at the head of the queue / prefetch budget while spilling.",
);

pub const ARRANGEMENT_EXERT_PROPORTIONALITY: Config<u32> = Config::new(
    "arrangement_exert_proportionality",
    16,
    "Value that controls how much merge effort to exert on arrangements.",
);

pub const ENABLE_PAUSED_CLUSTER_READHOLD_DOWNGRADE: Config<bool> = Config::new(
    "enable_paused_cluster_readhold_downgrade",
    true,
    "Aggressively downgrade input read holds for indexes on zero-replica clusters.",
);

/// Adds the full set of all controller `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL)
        .add(&ENABLE_0DT_DEPLOYMENT_SOURCES)
        .add(&WALLCLOCK_LAG_RECORDING_INTERVAL)
        .add(&WALLCLOCK_LAG_HISTOGRAM_PERIOD_INTERVAL)
        .add(&ENABLE_TIMELY_ZERO_COPY)
        .add(&ENABLE_TIMELY_ZERO_COPY_LGALLOC)
        .add(&TIMELY_ZERO_COPY_LIMIT)
        .add(&ENABLE_TIMELY_SPILL)
        .add(&TIMELY_SPILL_THRESHOLD_BYTES)
        .add(&TIMELY_SPILL_HEAD_RESERVE_BYTES)
        .add(&ARRANGEMENT_EXERT_PROPORTIONALITY)
        .add(&ENABLE_PAUSED_CLUSTER_READHOLD_DOWNGRADE)
}
