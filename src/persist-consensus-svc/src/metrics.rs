// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics for the consensus service.

use mz_ore::metric;
use mz_ore::metrics::{IntCounter, IntGauge, MetricsRegistry};
use prometheus::Histogram;

/// Metrics for the consensus service actor and object store WAL.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConsensusMetrics {
    // --- Operation counters ---
    pub cas_committed: IntCounter,
    pub cas_rejected: IntCounter,
    pub head_ops: IntCounter,
    pub scan_ops: IntCounter,
    pub truncate_ops: IntCounter,
    pub list_keys_ops: IntCounter,

    // --- Object store write counters ---
    pub object_store_wal_writes: IntCounter,
    pub object_store_wal_write_bytes: IntCounter,
    pub object_store_snapshot_writes: IntCounter,
    pub object_store_snapshot_write_bytes: IntCounter,
    pub object_store_write_retries: IntCounter,
    pub object_store_write_retry_already_exists: IntCounter,

    // --- Object store latency ---
    pub object_store_wal_write_latency_seconds: Histogram,
    pub object_store_snapshot_write_latency_seconds: Histogram,

    // --- Flush metrics ---
    pub flush_count: IntCounter,
    pub flush_timer_triggered: IntCounter,
    pub flush_age_triggered: IntCounter,
    pub flush_explicit_triggered: IntCounter,
    pub flush_latency_seconds: Histogram,
    pub flush_ops_per_batch: Histogram,
    pub flush_shards_per_batch: Histogram,

    // --- In-memory state gauges ---
    pub active_shards: IntGauge,
    pub total_entries: IntGauge,
    pub approx_bytes: IntGauge,
}

impl ConsensusMetrics {
    /// Register all metrics into the given registry.
    pub fn register(registry: &MetricsRegistry) -> Self {
        ConsensusMetrics {
            cas_committed: registry.register(metric!(
                name: "mz_consensus_svc_cas_committed_total",
                help: "Total CAS operations committed.",
            )),
            cas_rejected: registry.register(metric!(
                name: "mz_consensus_svc_cas_rejected_total",
                help: "Total CAS operations rejected (expectation mismatch).",
            )),
            head_ops: registry.register(metric!(
                name: "mz_consensus_svc_head_ops_total",
                help: "Total head operations.",
            )),
            scan_ops: registry.register(metric!(
                name: "mz_consensus_svc_scan_ops_total",
                help: "Total scan operations.",
            )),
            truncate_ops: registry.register(metric!(
                name: "mz_consensus_svc_truncate_ops_total",
                help: "Total truncate operations.",
            )),
            list_keys_ops: registry.register(metric!(
                name: "mz_consensus_svc_list_keys_ops_total",
                help: "Total list_keys operations.",
            )),
            object_store_wal_writes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_wal_writes_total",
                help: "Total WAL batches written to object storage.",
            )),
            object_store_wal_write_bytes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_wal_write_bytes_total",
                help: "Total bytes written to object storage for WAL batches.",
            )),
            object_store_snapshot_writes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_writes_total",
                help: "Total snapshots written to object storage.",
            )),
            object_store_snapshot_write_bytes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_write_bytes_total",
                help: "Total bytes written to object storage for snapshots.",
            )),
            object_store_write_retries: registry.register(metric!(
                name: "mz_consensus_svc_object_store_write_retries_total",
                help: "Total object store write retries attempted.",
            )),
            object_store_write_retry_already_exists: registry.register(metric!(
                name: "mz_consensus_svc_object_store_write_retry_already_exists_total",
                help: "Object store write retries where object already existed (original write landed).",
            )),
            object_store_wal_write_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_object_store_wal_write_latency_seconds",
                help: "Histogram of object store WAL PUT latency in seconds.",
                buckets: vec![
                    0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064,
                    0.128, 0.256, 0.512, 1.0, 2.0, 5.0,
                ],
            )),
            object_store_snapshot_write_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_write_latency_seconds",
                help: "Histogram of object store snapshot PUT latency in seconds.",
                buckets: vec![
                    0.002, 0.004, 0.008, 0.016, 0.032, 0.064,
                    0.128, 0.256, 0.512, 1.0, 2.0, 5.0,
                ],
            )),
            flush_count: registry.register(metric!(
                name: "mz_consensus_svc_flush_count_total",
                help: "Total flush operations.",
            )),
            flush_timer_triggered: registry.register(metric!(
                name: "mz_consensus_svc_flush_timer_triggered_total",
                help: "Flushes triggered by the periodic flush interval timer.",
            )),
            flush_age_triggered: registry.register(metric!(
                name: "mz_consensus_svc_flush_age_triggered_total",
                help: "Flushes triggered by max pending age deadline.",
            )),
            flush_explicit_triggered: registry.register(metric!(
                name: "mz_consensus_svc_flush_explicit_triggered_total",
                help: "Flushes triggered by an explicit Flush command.",
            )),
            flush_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_flush_latency_seconds",
                help: "Histogram of flush latency in seconds.",
                buckets: vec![0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0],
            )),
            flush_ops_per_batch: registry.register(metric!(
                name: "mz_consensus_svc_flush_ops_per_batch",
                help: "Histogram of WAL ops per flush batch.",
                buckets: vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
            )),
            flush_shards_per_batch: registry.register(metric!(
                name: "mz_consensus_svc_flush_shards_per_batch",
                help: "Histogram of distinct shards per flush batch.",
                buckets: vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0],
            )),
            active_shards: registry.register(metric!(
                name: "mz_consensus_svc_active_shards",
                help: "Number of active shards tracked in memory.",
            )),
            total_entries: registry.register(metric!(
                name: "mz_consensus_svc_total_entries",
                help: "Total versioned entries across all shards.",
            )),
            approx_bytes: registry.register(metric!(
                name: "mz_consensus_svc_approx_bytes",
                help: "Approximate bytes of data held in memory.",
            )),
        }
    }
}
