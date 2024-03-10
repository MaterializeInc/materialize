// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by the compute layer.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

/// Whether rendering should use `mz_join_core` rather than DD's `JoinCore::join_core`.
pub const ENABLE_MZ_JOIN_CORE: Config<bool> = Config::new(
    "enable_mz_join_core",
    true,
    "Whether compute should use `mz_join_core` rather than DD's `JoinCore::join_core` to render \
     linear joins.",
);

/// The yielding behavior with which linear joins should be rendered.
pub const LINEAR_JOIN_YIELDING: Config<String> = Config::new(
    "linear_join_yielding",
    "work:1000000,time:100",
    "The yielding behavior compute rendering should apply for linear join operators. Either \
     'work:<amount>' or 'time:<milliseconds>' or 'work:<amount>,time:<milliseconds>'. Note \
     that omitting one of 'work' or 'time' will entirely disable join yielding by time or \
     work, respectively, rather than falling back to some default.",
);

/// Enable lgalloc for columnation.
pub const ENABLE_COLUMNATION_LGALLOC: Config<bool> = Config::new(
    "enable_columnation_lgalloc",
    false,
    "Enable allocating regions from lgalloc.",
);

/// Enable lgalloc's eager memory return/reclamation feature.
pub const ENABLE_LGALLOC_EAGER_RECLAMATION: Config<bool> = Config::new(
    "enable_lgalloc_eager_reclamation",
    true,
    "Enable lgalloc's eager return behavior.",
);

/// Enable the chunked stack implementation.
pub const ENABLE_CHUNKED_STACK: Config<bool> = Config::new(
    "enable_compute_chunked_stack",
    false,
    "Enable the chunked stack implementation in compute.",
);

/// Enable operator hydration status logging.
pub const ENABLE_OPERATOR_HYDRATION_STATUS_LOGGING: Config<bool> = Config::new(
    "enable_compute_operator_hydration_status_logging",
    true,
    "Enable logging of the hydration status of compute operators.",
);

/// Enable usage of a pre-correction stash in the `persist_sink`.
pub const ENABLE_PERSIST_SINK_STASH: Config<bool> = Config::new(
    "enable_compute_persist_sink_stash",
    true,
    "Enable usage of a pre-correction stash in the compute `persist_sink`.",
);

/// Maximum number of in-flight bytes emitted by persist_sources feeding dataflows.
pub const DATAFLOW_MAX_INFLIGHT_BYTES: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in non-cc clusters.",
);

/// The "physical backpressure" of `compute_dataflow_max_inflight_bytes_cc` has
/// been replaced in cc replicas by persist lgalloc and we intend to remove it
/// once everything has switched to cc. In the meantime, this is a CYA to turn
/// it back on if absolutely necessary.
pub const DATAFLOW_MAX_INFLIGHT_BYTES_CC: Config<Option<usize>> = Config::new(
    "compute_dataflow_max_inflight_bytes_cc",
    None,
    "The maximum number of in-flight bytes emitted by persist_sources feeding \
     compute dataflows in cc clusters.",
);

/// The interval at which the background thread wakes.
pub const LGALLOC_BACKGROUND_INTERVAL: Config<Duration> = Config::new(
    "lgalloc_background_interval",
    Duration::from_secs(1),
    "Scheduling interval for lgalloc's background worker.",
);

/// The bytes to reclaim (slow path) per size class, for each background thread activation.
pub const LGALLOC_SLOW_CLEAR_BYTES: Config<usize> = Config::new(
    "lgalloc_slow_clear_bytes",
    32 << 20,
    "Clear byte size per size class for every invocation",
);

/// Adds the full set of all compute `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&ENABLE_MZ_JOIN_CORE)
        .add(&LINEAR_JOIN_YIELDING)
        .add(&ENABLE_COLUMNATION_LGALLOC)
        .add(&ENABLE_LGALLOC_EAGER_RECLAMATION)
        .add(&ENABLE_CHUNKED_STACK)
        .add(&ENABLE_OPERATOR_HYDRATION_STATUS_LOGGING)
        .add(&ENABLE_PERSIST_SINK_STASH)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES)
        .add(&DATAFLOW_MAX_INFLIGHT_BYTES_CC)
        .add(&LGALLOC_BACKGROUND_INTERVAL)
        .add(&LGALLOC_SLOW_CLEAR_BYTES)
}
