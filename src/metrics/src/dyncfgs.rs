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

/// How frequently to refresh lgalloc stats.
pub(crate) const MZ_METRICS_RUSAGE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_rusage_refresh_interval",
    Duration::from_secs(30),
    "How frequently to refresh rusage stats. A zero duration disables refreshing.",
);

/// Adds the full set of all storage `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs.add(&MZ_METRICS_RUSAGE_REFRESH_INTERVAL)
}
