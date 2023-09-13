// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_client::metrics::{CommandMetrics, HistoryMetrics};
use mz_ore::metric;
use mz_ore::metrics::{raw, MetricsRegistry, UIntGauge};
use prometheus::core::{AtomicF64, GenericCounter};

/// Metrics exposed by compute replicas.
//
// Most of the metrics here use the `raw` implementations, rather than the `DeleteOnDrop` wrappers
// because their labels are fixed throughout the lifetime of the replica process. For example, any
// metric labeled only by `worker_id` can be `raw` since the number of workers cannot change.
//
// Metrics that are labelled by a dimension that can change throughout the lifetime of the process
// (such as `collection_id`) MUST NOT use the `raw` metric types and must use the `DeleteOnDrop`
// types instead, to avoid memory leaks.
#[derive(Clone, Debug)]
pub struct ComputeMetrics {
    // command history
    history_command_count: raw::UIntGaugeVec,
    history_dataflow_count: raw::UIntGaugeVec,

    // reconciliation
    reconciliation_reused_dataflows_count_total: raw::IntCounterVec,
    reconciliation_replaced_dataflows_count_total: raw::IntCounterVec,

    // arrangements
    arrangement_maintenance_seconds_total: raw::CounterVec,
    arrangement_maintenance_active_info: raw::UIntGaugeVec,
}

impl ComputeMetrics {
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            history_command_count: registry.register(metric!(
                name: "mz_compute_replica_history_command_count",
                help: "The number of commands in the replica's command history.",
                var_labels: ["worker_id", "command_type"],
            )),
            history_dataflow_count: registry.register(metric!(
                name: "mz_compute_replica_history_dataflow_count",
                help: "The number of dataflows in the replica's command history.",
                var_labels: ["worker_id"],
            )),
            reconciliation_reused_dataflows_count_total: registry.register(metric!(
                name: "mz_compute_reconciliation_reused_dataflows_count_total",
                help: "The total number of dataflows that were reused during compute reconciliation.",
                var_labels: ["worker_id"],
            )),
            reconciliation_replaced_dataflows_count_total: registry.register(metric!(
                name: "mz_compute_reconciliation_replaced_dataflows_count_total",
                help: "The total number of dataflows that were replaced during compute reconciliation.",
                var_labels: ["worker_id", "reason"],
            )),
            arrangement_maintenance_seconds_total: registry.register(metric!(
                name: "mz_arrangement_maintenance_seconds_total",
                help: "The total time spent maintaining arrangements.",
                var_labels: ["worker_id"],
            )),
            arrangement_maintenance_active_info: registry.register(metric!(
                name: "mz_arrangement_maintenance_active_info",
                help: "Whether maintenance is currently occuring.",
                var_labels: ["worker_id"],
            )),
        }
    }

    pub fn for_history(&self, worker_id: usize) -> HistoryMetrics<UIntGauge> {
        let worker = worker_id.to_string();
        let command_counts = CommandMetrics::build(|typ| {
            self.history_command_count
                .with_label_values(&[&worker, typ])
        });
        let dataflow_count = self.history_dataflow_count.with_label_values(&[&worker]);

        HistoryMetrics {
            command_counts,
            dataflow_count,
        }
    }

    pub fn for_traces(&self, worker_id: usize) -> TraceMetrics {
        let worker = worker_id.to_string();
        let maintenance_seconds_total = self
            .arrangement_maintenance_seconds_total
            .with_label_values(&[&worker]);
        let maintenance_active_info = self
            .arrangement_maintenance_active_info
            .with_label_values(&[&worker]);

        TraceMetrics {
            maintenance_seconds_total,
            maintenance_active_info,
        }
    }

    /// Record the reconciliation result for a single dataflow.
    ///
    /// Reconciliation is recorded as successful if the given properties all hold. Otherwise it is
    /// recorded as unsuccessful, with a reason based on the first property that does not hold.
    pub fn record_dataflow_reconciliation(
        &self,
        worker_id: usize,
        compatible: bool,
        uncompacted: bool,
        subscribe_free: bool,
    ) {
        let worker = worker_id.to_string();

        if !compatible {
            self.reconciliation_replaced_dataflows_count_total
                .with_label_values(&[&worker, "incompatible"])
                .inc();
        } else if !uncompacted {
            self.reconciliation_replaced_dataflows_count_total
                .with_label_values(&[&worker, "compacted"])
                .inc();
        } else if !subscribe_free {
            self.reconciliation_replaced_dataflows_count_total
                .with_label_values(&[&worker, "subscribe"])
                .inc();
        } else {
            self.reconciliation_reused_dataflows_count_total
                .with_label_values(&[&worker])
                .inc();
        }
    }
}

/// Metrics maintained by the trace manager.
pub struct TraceMetrics {
    pub maintenance_seconds_total: GenericCounter<AtomicF64>,
    /// 1 if this worker is currently doing maintenance.
    ///
    /// If maintenance turns out to take a very long time, this will allow us
    /// to gain a sense that Materialize is stuck on maintenance before the
    /// maintenance completes
    pub maintenance_active_info: UIntGauge,
}
