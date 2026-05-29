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
use mz_ore::metrics::{Histogram, IntGauge, MetricsRegistry};
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};

/// Metrics exposed by an in-envd `PersistCommitter` instance.
#[derive(Clone, Debug)]
pub struct CommitterMetrics {
    /// Total RPCs, labeled by op (head, scan, cas, truncate, list_keys) and
    /// result (ok, committed, mismatch, err_determinate, err_indeterminate).
    pub rpc_total: IntCounterVec,
    /// End-to-end RPC latency, including cache lookup and any wait inside
    /// the committer. Labeled by op.
    pub rpc_duration_seconds: HistogramVec,
    /// Latency of the underlying `Consensus` call only, i.e. CRDB roundtrip
    /// plus connection pool wait. Diff against `rpc_duration_seconds` to
    /// isolate committer-internal cost vs. backing-store cost. Labeled by op.
    pub backing_duration_seconds: HistogramVec,
    /// In-flight RPCs, summed across all ops. Useful as a coarse signal.
    pub inflight_rpcs: IntGauge,
    /// In-flight RPCs broken down by op. If one op pegs while others remain
    /// low, the backing store or committer handler for that op is the
    /// bottleneck.
    pub inflight_rpcs_by_op: IntGaugeVec,
    /// Cache hit counters, labeled by op.
    pub cache_hits_total: IntCounterVec,
    /// Cache miss counters, labeled by op.
    pub cache_misses_total: IntCounterVec,
    /// Current cardinality of the in-memory shard cache.
    pub cached_shards: IntGauge,
    /// Time from CaS mismatch to the background `head()` refresh task
    /// completing. If consistently slower than the client's retry interval,
    /// retries will see stale cache and may loop.
    pub cas_refresh_lag_seconds: Histogram,
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
                help: "End-to-end latency of committer RPCs.",
                var_labels: ["op"],
                buckets: prometheus::exponential_buckets(0.0001, 2.0, 18).expect("valid buckets"),
            )),
            backing_duration_seconds: registry.register(metric!(
                name: "mz_persist_committer_backing_duration_seconds",
                help: "Latency of the underlying Consensus call only.",
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
            inflight_rpcs_by_op: registry.register(metric!(
                name: "mz_persist_committer_inflight_rpcs_by_op",
                help: "In-flight committer RPCs by op.",
                var_labels: ["op"],
            )),
            cas_refresh_lag_seconds: registry.register(metric!(
                name: "mz_persist_committer_cas_refresh_lag_seconds",
                help: "Time from CaS mismatch to background head() refresh \
                       completion.",
                buckets: prometheus::exponential_buckets(0.0001, 2.0, 18).expect("valid buckets"),
            )),
        }
    }

    /// Build a set of metrics backed by a fresh registry. For tests only.
    #[cfg(test)]
    pub fn for_tests() -> Self {
        Self::register(&MetricsRegistry::new())
    }
}
