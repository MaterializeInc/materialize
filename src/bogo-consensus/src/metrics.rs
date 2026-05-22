// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Server-side metrics for the bogo-consensus gRPC service.

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{HistogramVec, IntCounterVec, IntGauge, IntGauge as UIntGauge};

/// Prometheus metrics registered on the server side.
///
/// Histograms are labeled by RPC name (`head`, `compare_and_set`, `scan`,
/// `truncate`, `list_keys`) and outcome (`ok`, `mismatch`, `err`). Gauges
/// expose the size of the in-memory state so a scraper can correlate request
/// latency with how large the store has grown during a run.
#[derive(Debug, Clone)]
pub struct BogoMetrics {
    pub rpc_started: IntCounterVec,
    pub rpc_completed: IntCounterVec,
    pub rpc_seconds: HistogramVec,
    pub rpc_bytes_in: IntCounterVec,
    pub rpc_bytes_out: IntCounterVec,
    pub shards_total: IntGauge,
    pub versions_total: UIntGauge,
}

impl BogoMetrics {
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            rpc_started: registry.register(metric!(
                name: "mz_bogo_consensus_rpc_started_total",
                help: "Count of RPCs received by the bogo-consensus server.",
                var_labels: ["rpc"],
            )),
            rpc_completed: registry.register(metric!(
                name: "mz_bogo_consensus_rpc_completed_total",
                help: "Count of RPCs that finished, labeled by outcome.",
                var_labels: ["rpc", "outcome"],
            )),
            rpc_seconds: registry.register(metric!(
                name: "mz_bogo_consensus_rpc_seconds",
                help: "Wall time spent serving each RPC.",
                var_labels: ["rpc"],
                buckets: histogram_seconds_buckets(0.000_050, 16.0),
            )),
            rpc_bytes_in: registry.register(metric!(
                name: "mz_bogo_consensus_rpc_bytes_in_total",
                help: "Total bytes of payload data accepted (CAS data field).",
                var_labels: ["rpc"],
            )),
            rpc_bytes_out: registry.register(metric!(
                name: "mz_bogo_consensus_rpc_bytes_out_total",
                help: "Total bytes of payload data returned (head/scan).",
                var_labels: ["rpc"],
            )),
            shards_total: registry.register(metric!(
                name: "mz_bogo_consensus_shards_total",
                help: "Number of distinct keys currently held in memory.",
            )),
            versions_total: registry.register(metric!(
                name: "mz_bogo_consensus_versions_total",
                help: "Total number of VersionedData entries currently held in memory.",
            )),
        }
    }
}
