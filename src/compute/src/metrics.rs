// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use mz_compute_client::metrics::{CommandMetrics, HistoryMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{
    MakeCollectorOpts, MetricTag, MetricVisibility, MetricsRegistry, UIntGauge, raw,
};
use mz_repr::{GlobalId, SharedRow};
use prometheus::core::{AtomicF64, GenericCounter};
use prometheus::proto::LabelPair;
use prometheus::{Histogram, HistogramVec, IntCounter};

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
    // Optional workload class label to apply to all metrics in registry.
    workload_class: Arc<Mutex<Option<String>>>,

    // command history
    history_command_count: raw::UIntGaugeVec,
    history_dataflow_count: raw::UIntGaugeVec,

    // reconciliation
    reconciliation_reused_dataflows_count_total: raw::IntCounterVec,
    reconciliation_replaced_dataflows_count_total: raw::IntCounterVec,

    // arrangements
    arrangement_maintenance_seconds_total: raw::CounterVec,
    arrangement_maintenance_active_info: raw::UIntGaugeVec,

    // timings
    //
    // Note that this particular metric unfortunately takes some care to
    // interpret. It measures the duration of step_or_park calls, which
    // undesirably includes the parking. This is probably fine because we
    // regularly send progress information through persist sources, which likely
    // means the parking is capped at a second or two in practice. It also
    // doesn't do anything to let you pinpoint _which_ operator or worker isn't
    // yielding, but it should hopefully alert us when there is something to
    // look at.
    timely_step_duration_seconds: HistogramVec,
    persist_peek_seconds: HistogramVec,
    stashed_peek_seconds: HistogramVec,
    handle_command_duration_seconds: HistogramVec,

    // Index peek timing phases (per-cluster, no worker label)
    index_peek_total_seconds: Histogram,
    index_peek_seek_fulfillment_seconds: Histogram,
    index_peek_error_scan_seconds: Histogram,
    index_peek_cursor_setup_seconds: Histogram,
    index_peek_row_iteration_seconds: Histogram,
    index_peek_result_sort_seconds: Histogram,
    index_peek_frontier_check_seconds: Histogram,
    index_peek_row_collection_seconds: Histogram,

    // memory usage
    shared_row_heap_capacity_bytes: raw::UIntGaugeVec,

    // replica expiration
    replica_expiration_timestamp_seconds: raw::UIntGaugeVec,
    replica_expiration_remaining_seconds: raw::GaugeVec,

    // collections
    collection_count: raw::UIntGaugeVec,

    // subscribes
    subscribe_snapshots_skipped_total: IntCounter,
}

/// Applies the per-role const label to `opts`, unless `role` is `Solo`.
///
/// The two named roles (maintenance, interactive) each get a distinct `role` label so a second
/// compute runtime in the same process registers a distinct series rather than colliding with the
/// first. `Solo` omits the label so a single-runtime deployment registers exactly as it did before
/// a second runtime existed.
fn with_role(
    mut opts: MakeCollectorOpts,
    role: crate::server::ComputeRuntimeRole,
) -> MakeCollectorOpts {
    if let Some(label) = role.label() {
        opts.opts = opts.opts.const_label("role", label);
    }
    opts
}

impl ComputeMetrics {
    /// Registers the compute metrics for `role` into `registry`.
    ///
    /// The two named roles carry a `role` const label so that a second compute runtime in the same
    /// process registers a distinct series rather than colliding with the first. `Solo` carries no
    /// such label.
    pub fn register_with(
        registry: &MetricsRegistry,
        role: crate::server::ComputeRuntimeRole,
    ) -> Self {
        let workload_class = Arc::new(Mutex::new(None));

        // Apply a `workload_class` label to all metrics in the registry when we
        // have a known workload class.
        //
        // The postprocessor rewrites every metric in the whole registry, so only the maintenance
        // runtime registers it. A second registration from the interactive runtime would push the
        // label twice onto each metric and produce a duplicate-label scrape error.
        if role.owns_process_globals() {
            registry.register_postprocessor({
                let workload_class = Arc::clone(&workload_class);
                move |metrics| {
                    let workload_class: Option<String> =
                        workload_class.lock().expect("lock poisoned").clone();
                    let Some(workload_class) = workload_class else {
                        return;
                    };
                    for metric in metrics {
                        for metric in metric.mut_metric() {
                            let mut label = LabelPair::default();
                            label.set_name("workload_class".into());
                            label.set_value(workload_class.clone());

                            let mut labels = metric.take_label();
                            labels.push(label);
                            metric.set_label(labels);
                        }
                    }
                }
            });
        }

        Self {
            workload_class,
            history_command_count: registry.register(with_role(metric!(
                name: "mz_compute_replica_history_command_count",
                help: "The number of commands in the replica's command history.",
                var_labels: ["worker_id", "command_type"],
            ), role)),
            history_dataflow_count: registry.register(with_role(metric!(
                name: "mz_compute_replica_history_dataflow_count",
                help: "The number of dataflows in the replica's command history.",
                var_labels: ["worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Compute],
            ), role)),
            reconciliation_reused_dataflows_count_total: registry.register(with_role(metric!(
                name: "mz_compute_reconciliation_reused_dataflows_count_total",
                help: "The total number of dataflows that were reused during compute reconciliation.",
                var_labels: ["worker_id"],
            ), role)),
            reconciliation_replaced_dataflows_count_total: registry.register(with_role(metric!(
                name: "mz_compute_reconciliation_replaced_dataflows_count_total",
                help: "The total number of dataflows that were replaced during compute reconciliation.",
                var_labels: ["worker_id", "reason"],
            ), role)),
            arrangement_maintenance_seconds_total: registry.register(with_role(metric!(
                name: "mz_arrangement_maintenance_seconds_total",
                help: "The total time spent maintaining arrangements.",
                var_labels: ["worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Compute],
            ), role)),
            arrangement_maintenance_active_info: registry.register(with_role(metric!(
                name: "mz_arrangement_maintenance_active_info",
                help: "Whether maintenance is currently occuring.",
                var_labels: ["worker_id"],
            ), role)),
            timely_step_duration_seconds: registry.register(with_role(metric!(
                name: "mz_timely_step_duration_seconds",
                help: "The time spent in each compute step_or_park call",
                const_labels: {"cluster" => "compute"},
                var_labels: ["worker_id"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 32.0),
            ), role)),
            shared_row_heap_capacity_bytes: registry.register(with_role(metric!(
                name: "mz_dataflow_shared_row_heap_capacity_bytes",
                help: "The heap capacity of the shared row.",
                var_labels: ["worker_id"],
            ), role)),
            persist_peek_seconds: registry.register(with_role(metric!(
                name: "mz_persist_peek_seconds",
                help: "Time spent in (experimental) Persist fast-path peeks.",
                var_labels: ["worker_id"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            stashed_peek_seconds: registry.register(with_role(metric!(
                name: "mz_stashed_peek_seconds",
                help: "Time spent reading a peek result and stashing it in the peek result stash (aka. persist blob).",
                var_labels: ["worker_id"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            handle_command_duration_seconds: registry.register(with_role(metric!(
                name: "mz_cluster_handle_command_duration_seconds",
                help: "Time spent in handling commands.",
                const_labels: {"cluster" => "compute"},
                var_labels: ["worker_id", "command_type"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_total_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_total_seconds",
                help: "Total time processing index peeks, from process_peek entry to response. Excluding peeks that use the peek response stash.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_seek_fulfillment_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_seek_fulfillment_seconds",
                help: "Time in seek_fulfillment method including frontier checks and data collection.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_error_scan_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_error_scan_seconds",
                help: "Time scanning the error trace for errors.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_cursor_setup_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_cursor_setup_seconds",
                help: "Time setting up cursor and literal constraints.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_row_iteration_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_row_iteration_seconds",
                help: "Time iterating rows and evaluating MFP.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_result_sort_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_result_sort_seconds",
                help: "Time sorting intermediate results during peek collection.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_frontier_check_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_frontier_check_seconds",
                help: "Time checking trace frontiers.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            index_peek_row_collection_seconds: registry.register(with_role(metric!(
                name: "mz_index_peek_row_collection_seconds",
                help: "Time constructing RowCollection from peek results.",
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 8.0),
            ), role)),
            replica_expiration_timestamp_seconds: registry.register(with_role(metric!(
                name: "mz_dataflow_replica_expiration_timestamp_seconds",
                help: "The replica expiration timestamp in seconds since epoch.",
                var_labels: ["worker_id"],
            ), role)),
            replica_expiration_remaining_seconds: registry.register(with_role(metric!(
                name: "mz_dataflow_replica_expiration_remaining_seconds",
                help: "The remaining seconds until replica expiration. Can go negative, can lag behind.",
                var_labels: ["worker_id"],
            ), role)),
            collection_count: registry.register(with_role(metric!(
                name: "mz_compute_collection_count",
                help: "The number and hydration status of maintained compute collections.",
                var_labels: ["worker_id", "type", "hydrated"],
            ), role)),
            subscribe_snapshots_skipped_total: registry.register(with_role(metric!(
                name: "mz_subscribe_snapshots_skipped_total",
                help: "The number of collection snapshots that were skipped by the subscribe snapshot optimization.",
            ), role)),
        }
    }

    /// Sets the workload class for the compute metrics.
    pub fn set_workload_class(&self, workload_class: Option<String>) {
        let mut guard = self.workload_class.lock().expect("lock poisoned");
        *guard = workload_class
    }

    pub fn for_worker(&self, worker_id: usize) -> WorkerMetrics {
        let worker = worker_id.to_string();
        let arrangement_maintenance_seconds_total = self
            .arrangement_maintenance_seconds_total
            .with_label_values(&[&worker]);
        let arrangement_maintenance_active_info = self
            .arrangement_maintenance_active_info
            .with_label_values(&[&worker]);
        let timely_step_duration_seconds = self
            .timely_step_duration_seconds
            .with_label_values(&[&worker]);
        let persist_peek_seconds = self.persist_peek_seconds.with_label_values(&[&worker]);
        let stashed_peek_seconds = self.stashed_peek_seconds.with_label_values(&[&worker]);
        let handle_command_duration_seconds = CommandMetrics::build(|typ| {
            self.handle_command_duration_seconds
                .with_label_values(&[worker.as_ref(), typ])
        });
        let index_peek_total_seconds = self.index_peek_total_seconds.clone();
        let index_peek_seek_fulfillment_seconds = self.index_peek_seek_fulfillment_seconds.clone();
        let index_peek_error_scan_seconds = self.index_peek_error_scan_seconds.clone();
        let index_peek_cursor_setup_seconds = self.index_peek_cursor_setup_seconds.clone();
        let index_peek_row_iteration_seconds = self.index_peek_row_iteration_seconds.clone();
        let index_peek_result_sort_seconds = self.index_peek_result_sort_seconds.clone();
        let index_peek_frontier_check_seconds = self.index_peek_frontier_check_seconds.clone();
        let index_peek_row_collection_seconds = self.index_peek_row_collection_seconds.clone();
        let replica_expiration_timestamp_seconds = self
            .replica_expiration_timestamp_seconds
            .with_label_values(&[&worker]);
        let replica_expiration_remaining_seconds = self
            .replica_expiration_remaining_seconds
            .with_label_values(&[&worker]);
        let shared_row_heap_capacity_bytes = self
            .shared_row_heap_capacity_bytes
            .with_label_values(&[&worker]);

        WorkerMetrics {
            worker_label: worker,
            metrics: self.clone(),
            arrangement_maintenance_seconds_total,
            arrangement_maintenance_active_info,
            timely_step_duration_seconds,
            persist_peek_seconds,
            stashed_peek_seconds,
            handle_command_duration_seconds,
            index_peek_total_seconds,
            index_peek_seek_fulfillment_seconds,
            index_peek_error_scan_seconds,
            index_peek_cursor_setup_seconds,
            index_peek_row_iteration_seconds,
            index_peek_result_sort_seconds,
            index_peek_frontier_check_seconds,
            index_peek_row_collection_seconds,
            replica_expiration_timestamp_seconds,
            replica_expiration_remaining_seconds,
            shared_row_heap_capacity_bytes,
        }
    }
}

/// Per-worker metrics.
#[derive(Clone, Debug)]
pub struct WorkerMetrics {
    worker_label: String,
    metrics: ComputeMetrics,

    /// The amount of time spent in arrangement maintenance.
    pub(crate) arrangement_maintenance_seconds_total: GenericCounter<AtomicF64>,
    /// 1 if this worker is currently doing maintenance.
    ///
    /// If maintenance turns out to take a very long time, this will allow us
    /// to gain a sense that Materialize is stuck on maintenance before the
    /// maintenance completes
    pub(crate) arrangement_maintenance_active_info: UIntGauge,
    /// Histogram of Timely step timings.
    pub(crate) timely_step_duration_seconds: Histogram,
    /// Histogram of persist peek durations.
    pub(crate) persist_peek_seconds: Histogram,
    /// Histogram of stashed peek durations.
    pub(crate) stashed_peek_seconds: Histogram,
    /// Histogram of command handling durations.
    pub(crate) handle_command_duration_seconds: CommandMetrics<Histogram>,
    /// Histogram of total index peek durations.
    pub(crate) index_peek_total_seconds: Histogram,
    /// Histogram of index peek seek_fulfillment durations.
    pub(crate) index_peek_seek_fulfillment_seconds: Histogram,
    /// Histogram of index peek error scan durations.
    pub(crate) index_peek_error_scan_seconds: Histogram,
    /// Histogram of index peek cursor setup durations.
    pub(crate) index_peek_cursor_setup_seconds: Histogram,
    /// Histogram of index peek row iteration durations.
    pub(crate) index_peek_row_iteration_seconds: Histogram,
    /// Histogram of index peek result sort durations.
    pub(crate) index_peek_result_sort_seconds: Histogram,
    /// Histogram of index peek frontier check durations.
    pub(crate) index_peek_frontier_check_seconds: Histogram,
    /// Histogram of index peek row collection construction durations.
    pub(crate) index_peek_row_collection_seconds: Histogram,
    /// The timestamp of replica expiration.
    pub(crate) replica_expiration_timestamp_seconds: UIntGauge,
    /// Remaining seconds until replica expiration.
    pub(crate) replica_expiration_remaining_seconds: raw::Gauge,
    /// Heap capacity of the shared row.
    shared_row_heap_capacity_bytes: UIntGauge,
}

impl WorkerMetrics {
    pub fn for_history(&self) -> HistoryMetrics<UIntGauge> {
        let command_counts = CommandMetrics::build(|typ| {
            self.metrics
                .history_command_count
                .with_label_values(&[self.worker_label.as_ref(), typ])
        });
        let dataflow_count = self
            .metrics
            .history_dataflow_count
            .with_label_values(&[&self.worker_label]);

        HistoryMetrics {
            command_counts,
            dataflow_count,
        }
    }

    /// Record the reconciliation result for a single dataflow.
    ///
    /// Reconciliation is recorded as successful if the given properties all hold. Otherwise it is
    /// recorded as unsuccessful, with a reason based on the first property that does not hold.
    ///
    /// The properties are:
    ///  * compatible: The old and new dataflow descriptions are compatible.
    ///  * uncompacted: Collections currently installed for the dataflow exports have not been
    ///                 allowed to compact beyond that new dataflow as-of.
    ///  * subscribe_free: The dataflow does not export a subscribe sink.
    ///  * copy_to_free: The dataflow does not export a copy-to sink.
    ///  * dependencies_retained: All local inputs to the dataflow were retained by compute
    ///                           reconciliation.
    pub fn record_dataflow_reconciliation(
        &self,
        compatible: bool,
        uncompacted: bool,
        subscribe_free: bool,
        copy_to_free: bool,
        dependencies_retained: bool,
    ) {
        if !compatible {
            self.metrics
                .reconciliation_replaced_dataflows_count_total
                .with_label_values(&[self.worker_label.as_ref(), "incompatible"])
                .inc();
        } else if !uncompacted {
            self.metrics
                .reconciliation_replaced_dataflows_count_total
                .with_label_values(&[self.worker_label.as_ref(), "compacted"])
                .inc();
        } else if !subscribe_free {
            self.metrics
                .reconciliation_replaced_dataflows_count_total
                .with_label_values(&[self.worker_label.as_ref(), "subscribe"])
                .inc();
        } else if !copy_to_free {
            self.metrics
                .reconciliation_replaced_dataflows_count_total
                .with_label_values(&[self.worker_label.as_ref(), "copy-to"])
                .inc();
        } else if !dependencies_retained {
            self.metrics
                .reconciliation_replaced_dataflows_count_total
                .with_label_values(&[self.worker_label.as_ref(), "dependency"])
                .inc();
        } else {
            self.metrics
                .reconciliation_reused_dataflows_count_total
                .with_label_values(&[&self.worker_label])
                .inc();
        }
    }

    /// Record the heap capacity of the shared row.
    pub fn record_shared_row_metrics(&self) {
        let binding = SharedRow::get();
        self.shared_row_heap_capacity_bytes
            .set(u64::cast_from(binding.byte_capacity()));
    }

    /// Increase the count of maintained collections.
    fn inc_collection_count(&self, collection_type: &str, hydrated: bool) {
        let hydrated = if hydrated { "1" } else { "0" };
        self.metrics
            .collection_count
            .with_label_values(&[self.worker_label.as_ref(), collection_type, hydrated])
            .inc();
    }

    /// Decrease the count of maintained collections.
    fn dec_collection_count(&self, collection_type: &str, hydrated: bool) {
        let hydrated = if hydrated { "1" } else { "0" };
        self.metrics
            .collection_count
            .with_label_values(&[self.worker_label.as_ref(), collection_type, hydrated])
            .dec();
    }

    pub fn inc_subscribe_snapshot_optimization(&self) {
        self.metrics.subscribe_snapshots_skipped_total.inc()
    }

    /// Sets the workload class for the compute metrics.
    pub fn set_workload_class(&self, workload_class: Option<String>) {
        self.metrics.set_workload_class(workload_class);
    }

    pub fn for_collection(&self, id: GlobalId) -> CollectionMetrics {
        CollectionMetrics::new(id, self.clone())
    }
}

/// Collection metrics.
///
/// Note that these metrics do _not_ have a `collection_id` label. We avoid introducing
/// per-collection, per-worker metrics because the number of resulting time series would
/// potentially be huge. Instead we count classes of collections, such as hydrated collections.
#[derive(Clone, Debug)]
pub struct CollectionMetrics {
    metrics: WorkerMetrics,
    collection_type: &'static str,
    collection_hydrated: bool,
}

impl CollectionMetrics {
    pub fn new(collection_id: GlobalId, metrics: WorkerMetrics) -> Self {
        let collection_type = match collection_id {
            GlobalId::System(_) => "system",
            GlobalId::IntrospectionSourceIndex(_) => "log",
            GlobalId::User(_) => "user",
            GlobalId::Transient(_) => "transient",
            GlobalId::Explain => "explain",
        };
        let collection_hydrated = false;

        metrics.inc_collection_count(collection_type, collection_hydrated);

        Self {
            metrics,
            collection_type,
            collection_hydrated,
        }
    }

    /// Record this collection as hydration.
    pub fn record_collection_hydrated(&mut self) {
        if self.collection_hydrated {
            return;
        }

        self.metrics
            .dec_collection_count(self.collection_type, false);
        self.metrics
            .inc_collection_count(self.collection_type, true);
        self.collection_hydrated = true;
    }
}

impl Drop for CollectionMetrics {
    fn drop(&mut self) {
        self.metrics
            .dec_collection_count(self.collection_type, self.collection_hydrated);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_ore::metrics::MetricsRegistry;

    use super::ComputeMetrics;
    use crate::server::ComputeRuntimeRole;

    /// The `Solo` (single-runtime) role registers exactly as compute did before a second runtime
    /// existed: no metric carries a `role` label, so single-runtime dashboards and exact-match
    /// alerts are byte-unchanged.
    #[mz_ore::test]
    fn solo_runtime_omits_role_label() {
        let registry = MetricsRegistry::new();
        let metrics = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Solo);
        // Instantiate the per-worker children so the `*Vec` families emit rows to inspect.
        let _worker = metrics.for_worker(0);

        for family in registry.gather() {
            for metric in family.get_metric() {
                for label in metric.get_label() {
                    assert_ne!(
                        label.name(),
                        "role",
                        "solo metric {} unexpectedly carries a role label",
                        family.name(),
                    );
                }
            }
        }
    }

    /// The two named roles each carry their own `role` label, so two runtimes in one process
    /// register distinct series rather than colliding. Registering both on one registry also
    /// exercises the non-collision that lets them coexist.
    #[mz_ore::test]
    fn named_roles_carry_distinct_role_label() {
        let registry = MetricsRegistry::new();
        let maintenance = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Maintenance);
        let interactive = ComputeMetrics::register_with(&registry, ComputeRuntimeRole::Interactive);
        let _maintenance_worker = maintenance.for_worker(0);
        let _interactive_worker = interactive.for_worker(0);

        let mut roles = BTreeSet::new();
        for family in registry.gather() {
            for metric in family.get_metric() {
                let role = metric
                    .get_label()
                    .iter()
                    .find(|label| label.name() == "role")
                    .unwrap_or_else(|| panic!("metric {} missing a role label", family.name()));
                roles.insert(role.value().to_string());
            }
        }

        assert!(roles.contains("maintenance"), "roles seen: {roles:?}");
        assert!(roles.contains("interactive"), "roles seen: {roles:?}");
    }
}
