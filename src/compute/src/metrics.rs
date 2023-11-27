// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_client::metrics::{CommandMetrics, HistoryMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{raw, MetricsRegistry, UIntGauge};
use mz_repr::SharedRow;
use prometheus::core::{AtomicF64, GenericCounter};
use prometheus::Histogram;

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

    // timely step timings
    //
    // Note that this particular metric unfortunately takes some care to
    // interpret. It measures the duration of step_or_park calls, which
    // undesirably includes the parking. This is probably fine because we
    // regularly send progress information through persist sources, which likely
    // means the parking is capped at a second or two in practice. It also
    // doesn't do anything to let you pinpoint _which_ operator or worker isn't
    // yielding, but it should hopefully alert us when there is something to
    // look at.
    pub(crate) timely_step_duration_seconds: Histogram,

    pub(crate) delayed_time_seconds_total: raw::CounterVec,

    /// Heap capacity of the shared row
    pub(crate) shared_row_heap_capacity_bytes: raw::UIntGaugeVec,

    pub(crate) persist_peek_seconds: Histogram,
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
            timely_step_duration_seconds: registry.register(metric!(
                name: "mz_timely_step_duration_seconds",
                help: "The time spent in each compute step_or_park call",
                const_labels: {"cluster" => "compute"},
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 32.0),
            )),
            delayed_time_seconds_total: registry.register(metric!(
                name: "mz_dataflow_delayed_time_seconds_total",
                help: "The total time dataflow outputs were delayed relative to their inputs.",
                const_labels: {"cluster" => "compute"},
                var_labels: ["worker_id"],
            )),
            shared_row_heap_capacity_bytes: registry.register(metric!(
                name: "mz_dataflow_shared_row_heap_capacity_bytes",
                help: "The heap capacity of the shared row.",
                var_labels: ["worker_id"],
            )),

            persist_peek_seconds: registry.register(metric!(
                name: "mz_persist_peek_seconds",
                help: "Time spent in (experimental) Persist fast-path peeks.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
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

    pub fn for_logging(&self, worker_id: usize) -> LoggingMetrics {
        let worker = worker_id.to_string();
        let delayed_time_seconds_total = self
            .delayed_time_seconds_total
            .with_label_values(&[&worker]);

        LoggingMetrics {
            delayed_time_seconds_total,
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

    /// Record the heap capacity of the shared row.
    pub fn record_shared_row_metrics(&self, worker_id: usize) {
        let worker = worker_id.to_string();

        let binding = SharedRow::get();
        self.shared_row_heap_capacity_bytes
            .with_label_values(&[&worker])
            .set(u64::cast_from(binding.borrow().heap_capacity()));
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

/// Metrics maintained by the logging dataflows.
#[derive(Clone)]
pub struct LoggingMetrics {
    pub delayed_time_seconds_total: GenericCounter<AtomicF64>,
}
