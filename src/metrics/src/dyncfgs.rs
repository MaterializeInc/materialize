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

use mz_dyncfg::{Config, ConfigSet, ParameterScope};

/// How frequently to refresh lgalloc map stats.
pub(crate) const MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_lgalloc_map_refresh_interval",
    Duration::from_secs(0),
    "How frequently to refresh lgalloc stats. A zero duration disables refreshing.",
    ParameterScope::Replica,
);

/// How frequently to refresh lgalloc stats.
pub(crate) const MZ_METRICS_LGALLOC_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_lgalloc_refresh_interval",
    Duration::from_secs(30),
    "How frequently to refresh lgalloc stats. A zero duration disables refreshing.",
    ParameterScope::Replica,
);

/// How frequently to refresh lgalloc stats.
pub(crate) const MZ_METRICS_RUSAGE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_rusage_refresh_interval",
    Duration::from_secs(30),
    "How frequently to refresh rusage stats. A zero duration disables refreshing.",
    ParameterScope::Replica,
);

/// How frequently to sample process resource usage.
///
/// This interval bounds how short a spike can be and still be observed, but only for the sources
/// with no kernel-side high-water mark. A kernel-maintained peak such as `cgroup memory.peak` is
/// exact no matter how rarely it is read. Deliberately separate from `memory_limiter_interval`,
/// which governs OOM-kill behavior and must not be retuned for introspection's sake.
pub(crate) const MZ_METRICS_USAGE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "mz_metrics_usage_refresh_interval",
    Duration::from_secs(5),
    "How frequently to sample process resource usage. A zero duration disables sampling.",
    ParameterScope::Replica,
);

/// Metric families removed from this process's exported metrics.
///
/// Comma-separated family names. A trailing `*` matches any family with that
/// prefix. See `export_filter` for the export-time semantics shared by all
/// `metrics_export_*` configs.
pub(crate) const METRICS_EXPORT_DISABLED_FAMILIES: Config<&str> = Config::new(
    "metrics_export_disabled_families",
    "",
    "Comma-separated metric family names (trailing `*` matches a prefix) removed from the \
     exported metrics.",
    ParameterScope::Environment,
);

/// Clusters whose per-cluster series are exported.
///
/// Comma-separated cluster IDs. When non-empty, exported series carrying an
/// `instance_id` or `cluster_id` label are kept only for the listed clusters.
/// Series without either label, or with an empty value, are unaffected.
pub(crate) const METRICS_EXPORT_CLUSTER_ALLOWLIST: Config<&str> = Config::new(
    "metrics_export_cluster_allowlist",
    "",
    "Comma-separated cluster IDs. When non-empty, series with an `instance_id` or `cluster_id` \
     label are exported only for these clusters.",
    ParameterScope::Environment,
);

/// Replicas whose per-replica series are exported.
///
/// Comma-separated replica IDs. When non-empty, exported series carrying a
/// `replica_id` label are kept only for the listed replicas.
pub(crate) const METRICS_EXPORT_REPLICA_ALLOWLIST: Config<&str> = Config::new(
    "metrics_export_replica_allowlist",
    "",
    "Comma-separated replica IDs. When non-empty, series with a `replica_id` label are \
     exported only for these replicas.",
    ParameterScope::Environment,
);

/// Upper bound on the samples exported for any one family.
///
/// Counted as the text encoder emits them, so a histogram series counts once
/// per bucket plus its `+Inf`, `_count` and `_sum` lines. A family exceeding
/// the bound is dropped whole rather than truncated, so a dashboard sees an
/// absent family instead of a partial one that looks complete. Zero disables
/// the bound.
pub(crate) const METRICS_EXPORT_MAX_SERIES_PER_FAMILY: Config<usize> = Config::new(
    "metrics_export_max_series_per_family",
    0,
    "Metric families with more exported samples than this are not exported. Zero disables the \
     bound.",
    ParameterScope::Environment,
);

/// Adds the full set of all storage `Config`s.
pub fn all_dyncfgs(configs: ConfigSet) -> ConfigSet {
    configs
        .add(&MZ_METRICS_LGALLOC_MAP_REFRESH_INTERVAL)
        .add(&MZ_METRICS_LGALLOC_REFRESH_INTERVAL)
        .add(&MZ_METRICS_RUSAGE_REFRESH_INTERVAL)
        .add(&MZ_METRICS_USAGE_REFRESH_INTERVAL)
        .add(&METRICS_EXPORT_DISABLED_FAMILIES)
        .add(&METRICS_EXPORT_CLUSTER_ALLOWLIST)
        .add(&METRICS_EXPORT_REPLICA_ALLOWLIST)
        .add(&METRICS_EXPORT_MAX_SERIES_PER_FAMILY)
}
