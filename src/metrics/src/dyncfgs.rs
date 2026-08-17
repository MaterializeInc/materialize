// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dyncfgs used by mz-metrics.

use std::time::Duration;

use mz_dyncfg::{Config, ConfigSet};

/// How frequently to refresh lgalloc map stats.
pub(crate) const MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_lgalloc_map_refresh_interval",
    Duration::from_secs(0),
    "How frequently to refresh lgalloc stats. A zero duration disables refreshing.",
);

/// How frequently to refresh lgalloc stats.
pub(crate) const MZ_METRICS_LGALLOC_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_lgalloc_refresh_interval",
    Duration::from_secs(30),
    "How frequently to refresh lgalloc stats. A zero duration disables refreshing.",
);

/// How frequently to refresh lgalloc stats.
pub(crate) const MZ_METRICS_RUSAGE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_rusage_refresh_interval",
    Duration::from_secs(30),
    "How frequently to refresh rusage stats. A zero duration disables refreshing.",
);

/// How frequently to sample resource usage for peak tracking.
///
/// Peaks are high-water marks over these samples, so this interval bounds how short a spike can
/// be and still be observed. It is deliberately separate from `memory_limiter_interval`, which
/// governs OOM-kill behavior and must not be retuned for introspection's sake.
pub(crate) const MZ_METRICS_PEAK_USAGE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_peak_usage_refresh_interval",
    Duration::from_secs(5),
    "How frequently to sample resource usage for peak tracking. A zero duration disables sampling.",
);

/// Adds the full set of all storage `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL)
        .add(&MZ_METRICS_LGALLOC_REFRESH_INTERVAL)
        .add(&MZ_METRICS_RUSAGE_REFRESH_INTERVAL)
        .add(&MZ_METRICS_PEAK_USAGE_REFRESH_INTERVAL)
}
