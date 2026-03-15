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

/// Metrics for the acceptor (blind group commit).
#[derive(Debug, Clone)]
pub struct AcceptorMetrics {
    // --- Flush metrics ---
    pub flush_count: IntCounter,
    pub flush_timer_triggered: IntCounter,
    pub flush_timer_ticked: IntCounter,
    pub flush_explicit_triggered: IntCounter,
    pub flush_latency_seconds: Histogram,
    pub flush_proposals_per_batch: Histogram,

    // --- Per-proposal latency ---
    /// Time from when a proposal arrives at the acceptor to when the flush
    /// containing it begins. Measures queuing delay in the pending buffer.
    pub proposal_queue_seconds: Histogram,

    // --- Object store log write metrics ---
    pub object_store_log_writes: IntCounter,
    pub object_store_log_write_bytes: IntCounter,
    pub object_store_log_write_latency_seconds: Histogram,
    pub object_store_write_retries: IntCounter,
    pub object_store_write_retry_already_exists: IntCounter,
}

impl AcceptorMetrics {
    /// Register acceptor metrics into the given registry.
    pub fn register(registry: &MetricsRegistry) -> Self {
        AcceptorMetrics {
            flush_count: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_count_total",
                help: "Total flush operations.",
            )),
            flush_timer_triggered: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_timer_triggered_total",
                help: "Flushes triggered by the periodic flush interval timer.",
            )),
            flush_timer_ticked: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_timer_ticked_total",
                help: "Total timer ticks (including empty ones with no pending proposals).",
            )),
            flush_explicit_triggered: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_explicit_triggered_total",
                help: "Flushes triggered by an explicit Flush command.",
            )),
            flush_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_latency_seconds",
                help: "Histogram of flush latency in seconds.",
                buckets: vec![0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0],
            )),
            flush_proposals_per_batch: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_flush_proposals_per_batch",
                help: "Histogram of proposals per flush batch.",
                buckets: vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
            )),
            proposal_queue_seconds: registry.register(metric!(
                name: "mz_consensus_svc_acceptor_proposal_queue_seconds",
                help: "Time from proposal arrival to flush start (queuing delay).",
                buckets: vec![
                    0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.003, 0.004, 0.005,
                    0.006, 0.008, 0.01, 0.015, 0.02, 0.03, 0.05, 0.1, 0.25, 0.5, 1.0,
                ],
            )),
            object_store_log_writes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_log_writes_total",
                help: "Total log batches written to object storage.",
            )),
            object_store_log_write_bytes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_log_write_bytes_total",
                help: "Total bytes written to object storage for log batches.",
            )),
            object_store_log_write_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_object_store_log_write_latency_seconds",
                help: "Histogram of object store log PUT latency in seconds.",
                buckets: vec![
                    0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064,
                    0.128, 0.256, 0.512, 1.0, 2.0, 5.0,
                ],
            )),
            object_store_write_retries: registry.register(metric!(
                name: "mz_consensus_svc_object_store_write_retries_total",
                help: "Total object store write retries attempted.",
            )),
            object_store_write_retry_already_exists: registry.register(metric!(
                name: "mz_consensus_svc_object_store_write_retry_already_exists_total",
                help: "Object store write retries where object already existed (original write landed).",
            )),
        }
    }
}

/// Metrics for the learner (state machine / materializer).
#[derive(Debug, Clone)]
pub struct LearnerMetrics {
    // --- Operation counters ---
    pub cas_committed: IntCounter,
    pub cas_rejected: IntCounter,
    pub head_ops: IntCounter,
    pub scan_ops: IntCounter,
    pub truncate_ops: IntCounter,
    pub list_keys_ops: IntCounter,

    // --- Materialization ---
    pub batches_materialized: IntCounter,
    pub batch_materialize_latency_seconds: Histogram,

    // --- Snapshot metrics ---
    pub object_store_snapshot_writes: IntCounter,
    pub object_store_snapshot_write_bytes: IntCounter,
    pub object_store_snapshot_write_latency_seconds: Histogram,

    // --- Server-side operation latencies ---
    /// Time from command send to actor processing (mpsc channel + select delay).
    pub cmd_queue_seconds: Histogram,
    /// Total server-side head latency (including linearization wait).
    pub head_seconds: Histogram,
    /// Total server-side scan latency (including linearization wait).
    pub scan_seconds: Histogram,
    /// Time from AwaitCasResult command to response (includes materialization wait).
    pub cas_result_seconds: Histogram,
    /// Time from AwaitTruncateResult command to response (includes materialization wait).
    pub truncate_result_seconds: Histogram,

    // --- In-memory state gauges ---
    pub active_shards: IntGauge,
    pub total_entries: IntGauge,
    pub approx_bytes: IntGauge,
}

impl LearnerMetrics {
    /// Register learner metrics into the given registry.
    pub fn register(registry: &MetricsRegistry) -> Self {
        LearnerMetrics {
            cas_committed: registry.register(metric!(
                name: "mz_consensus_svc_learner_cas_committed_total",
                help: "Total CAS proposals committed during materialization.",
            )),
            cas_rejected: registry.register(metric!(
                name: "mz_consensus_svc_learner_cas_rejected_total",
                help: "Total CAS proposals rejected (expectation mismatch).",
            )),
            head_ops: registry.register(metric!(
                name: "mz_consensus_svc_learner_head_ops_total",
                help: "Total head read operations served.",
            )),
            scan_ops: registry.register(metric!(
                name: "mz_consensus_svc_learner_scan_ops_total",
                help: "Total scan read operations served.",
            )),
            truncate_ops: registry.register(metric!(
                name: "mz_consensus_svc_learner_truncate_ops_total",
                help: "Total truncate proposals evaluated.",
            )),
            list_keys_ops: registry.register(metric!(
                name: "mz_consensus_svc_learner_list_keys_ops_total",
                help: "Total list_keys operations served.",
            )),
            batches_materialized: registry.register(metric!(
                name: "mz_consensus_svc_learner_batches_materialized_total",
                help: "Total log batches materialized (CAS evaluated).",
            )),
            batch_materialize_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_batch_materialize_latency_seconds",
                help: "Histogram of per-batch materialization latency in seconds.",
                buckets: vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0,
                ],
            )),
            cmd_queue_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_cmd_queue_seconds",
                help: "Time from command send to learner actor processing.",
                buckets: vec![
                    0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01,
                    0.02, 0.05, 0.1, 0.25, 0.5, 1.0,
                ],
            )),
            head_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_head_seconds",
                help: "Server-side head latency including linearization wait.",
                buckets: vec![
                    0.0001, 0.0005, 0.001, 0.002, 0.005, 0.008, 0.01, 0.015,
                    0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0,
                ],
            )),
            scan_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_scan_seconds",
                help: "Server-side scan latency including linearization wait.",
                buckets: vec![
                    0.0001, 0.0005, 0.001, 0.002, 0.005, 0.008, 0.01, 0.015,
                    0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0,
                ],
            )),
            cas_result_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_cas_result_seconds",
                help: "Server-side CAS result latency including materialization wait.",
                buckets: vec![
                    0.0001, 0.0005, 0.001, 0.002, 0.005, 0.008, 0.01, 0.015,
                    0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0,
                ],
            )),
            truncate_result_seconds: registry.register(metric!(
                name: "mz_consensus_svc_learner_truncate_result_seconds",
                help: "Server-side truncate result latency including materialization wait.",
                buckets: vec![
                    0.0001, 0.0005, 0.001, 0.002, 0.005, 0.008, 0.01, 0.015,
                    0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0,
                ],
            )),
            object_store_snapshot_writes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_writes_total",
                help: "Total snapshots written to object storage.",
            )),
            object_store_snapshot_write_bytes: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_write_bytes_total",
                help: "Total bytes written to object storage for snapshots.",
            )),
            object_store_snapshot_write_latency_seconds: registry.register(metric!(
                name: "mz_consensus_svc_object_store_snapshot_write_latency_seconds",
                help: "Histogram of object store snapshot PUT latency in seconds.",
                buckets: vec![
                    0.002, 0.004, 0.008, 0.016, 0.032, 0.064,
                    0.128, 0.256, 0.512, 1.0, 2.0, 5.0,
                ],
            )),
            active_shards: registry.register(metric!(
                name: "mz_consensus_svc_learner_active_shards",
                help: "Number of active shards tracked in memory.",
            )),
            total_entries: registry.register(metric!(
                name: "mz_consensus_svc_learner_total_entries",
                help: "Total versioned entries across all shards.",
            )),
            approx_bytes: registry.register(metric!(
                name: "mz_consensus_svc_learner_approx_bytes",
                help: "Approximate bytes of data held in memory.",
            )),
        }
    }
}
