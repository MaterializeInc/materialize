// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for the persist committer.

use mz_ore::metric;
use mz_ore::metrics::{HistogramVec, IntGauge, MetricsRegistry};
use prometheus::IntCounterVec;

/// Metrics exposed by an in-envd `PersistCommitter` instance.
#[derive(Clone, Debug)]
pub struct CommitterMetrics {
    pub rpc_total: IntCounterVec,
    pub rpc_duration_seconds: HistogramVec,
    pub cache_hits_total: IntCounterVec,
    pub cache_misses_total: IntCounterVec,
    pub cached_shards: IntGauge,
    pub inflight_rpcs: IntGauge,
}

impl CommitterMetrics {
    pub fn register(registry: &MetricsRegistry) -> Self {
        Self {
            rpc_total: registry.register(metric!(
                name: "mz_persist_committer_rpc_total",
                help: "Count of committer RPCs by op and result.",
                var_labels: ["op", "result"],
            )),
            rpc_duration_seconds: registry.register(metric!(
                name: "mz_persist_committer_rpc_duration_seconds",
                help: "Latency of committer RPCs.",
                var_labels: ["op"],
                buckets: prometheus::exponential_buckets(0.0001, 2.0, 18).expect("valid buckets"),
            )),
            cache_hits_total: registry.register(metric!(
                name: "mz_persist_committer_cache_hits_total",
                help: "Cache hits by op.",
                var_labels: ["op"],
            )),
            cache_misses_total: registry.register(metric!(
                name: "mz_persist_committer_cache_misses_total",
                help: "Cache misses by op.",
                var_labels: ["op"],
            )),
            cached_shards: registry.register(metric!(
                name: "mz_persist_committer_cached_shards",
                help: "Number of shards held in the committer's in-memory cache.",
            )),
            inflight_rpcs: registry.register(metric!(
                name: "mz_persist_committer_inflight_rpcs",
                help: "In-flight committer RPCs.",
            )),
        }
    }

    /// Build a set of metrics backed by a fresh registry. For tests only.
    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self::register(&MetricsRegistry::new())
    }
}
