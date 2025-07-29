// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for upsert sources.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, DeleteOnDropHistogram, GaugeVec, HistogramVec,
    IntCounterVec, MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_repr::GlobalId;
use mz_rocksdb::RocksDBInstanceMetrics;
use mz_storage_operators::metrics::BackpressureMetrics;
use prometheus::core::{AtomicF64, AtomicU64};

/// Metric definitions for the `upsert` operator.
#[derive(Clone, Debug)]
pub(crate) struct UpsertMetricDefs {
    pub(crate) rehydration_latency: GaugeVec,
    pub(crate) rehydration_total: UIntGaugeVec,
    pub(crate) rehydration_updates: UIntGaugeVec,

    // These are used by `shared`.
    pub(crate) merge_snapshot_latency: HistogramVec,
    pub(crate) merge_snapshot_updates: IntCounterVec,
    pub(crate) merge_snapshot_inserts: IntCounterVec,
    pub(crate) merge_snapshot_deletes: IntCounterVec,
    pub(crate) upsert_inserts: IntCounterVec,
    pub(crate) upsert_updates: IntCounterVec,
    pub(crate) upsert_deletes: IntCounterVec,
    pub(crate) multi_get_latency: HistogramVec,
    pub(crate) multi_get_size: IntCounterVec,
    pub(crate) multi_get_result_count: IntCounterVec,
    pub(crate) multi_get_result_bytes: IntCounterVec,
    pub(crate) multi_put_latency: HistogramVec,
    pub(crate) multi_put_size: IntCounterVec,

    // These are used by `rocksdb`.
    pub(crate) rocksdb_multi_get_latency: HistogramVec,
    pub(crate) rocksdb_multi_get_size: IntCounterVec,
    pub(crate) rocksdb_multi_get_result_count: IntCounterVec,
    pub(crate) rocksdb_multi_get_result_bytes: IntCounterVec,
    pub(crate) rocksdb_multi_get_count: IntCounterVec,
    pub(crate) rocksdb_multi_put_count: IntCounterVec,
    pub(crate) rocksdb_multi_put_latency: HistogramVec,
    pub(crate) rocksdb_multi_put_size: IntCounterVec,
    // These are maps so that multiple timely workers can interact with the same
    // `DeleteOnDropHistogram`, which is only dropped once ALL workers drop it.
    // The map may contain arbitrary, old `Weak`s for deleted sources, which are
    // only cleaned if those sources are recreated.
    //
    // We don't parameterize these by `worker_id` like the `rehydration_*` ones
    // to save on time-series cardinality.
    pub(crate) shared: Arc<Mutex<BTreeMap<GlobalId, Weak<UpsertSharedMetrics>>>>,
    pub(crate) rocksdb_shared:
        Arc<Mutex<BTreeMap<GlobalId, Weak<mz_rocksdb::RocksDBSharedMetrics>>>>,
}

impl UpsertMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        let shared = Arc::new(Mutex::new(BTreeMap::new()));
        let rocksdb_shared = Arc::new(Mutex::new(BTreeMap::new()));
        Self {
            rehydration_latency: registry.register(metric!(
                name: "mz_storage_upsert_state_rehydration_latency",
                help: "The latency, per-worker, in fractional seconds, \
                    of rehydrating the upsert state for this source",
                var_labels: ["source_id", "worker_id"],
            )),
            rehydration_total: registry.register(metric!(
                name: "mz_storage_upsert_state_rehydration_total",
                help: "The number of values \
                    per-worker, rehydrated into the upsert state for \
                    this source",
                var_labels: ["source_id", "worker_id"],
            )),
            rehydration_updates: registry.register(metric!(
                name: "mz_storage_upsert_state_rehydration_updates",
                help: "The number of updates (both negative and positive), \
                    per-worker, rehydrated into the upsert state for \
                    this source",
                var_labels: ["source_id", "worker_id"],
            )),
            // Choose a relatively low number of representative buckets.
            merge_snapshot_latency: registry.register(metric!(
                name: "mz_storage_upsert_merge_snapshot_latency",
                help: "The latencies, in fractional seconds, \
                    of merging snapshot updates into upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            merge_snapshot_updates: registry.register(metric!(
                name: "mz_storage_upsert_merge_snapshot_updates_total",
                help: "The batch size, \
                    of merging snapshot updates into upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id", "worker_id"],
            )),
            merge_snapshot_inserts: registry.register(metric!(
                name: "mz_storage_upsert_merge_snapshot_inserts_total",
                help: "The number of inserts in a batch for merging snapshot updates \
                    for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            merge_snapshot_deletes: registry.register(metric!(
                name: "mz_storage_upsert_merge_snapshot_deletes_total",
                help: "The number of deletes in a batch for merging snapshot updates \
                    for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            upsert_inserts: registry.register(metric!(
                name: "mz_storage_upsert_inserts_total",
                help: "The number of inserts done by the upsert operator",
                var_labels: ["source_id", "worker_id"],
            )),
            upsert_updates: registry.register(metric!(
                name: "mz_storage_upsert_updates_total",
                help: "The number of updates done by the upsert operator",
                var_labels: ["source_id", "worker_id"],
            )),
            upsert_deletes: registry.register(metric!(
                name: "mz_storage_upsert_deletes_total",
                help: "The number of deletes done by the upsert operator.",
                var_labels: ["source_id", "worker_id"],
            )),
            multi_get_latency: registry.register(metric!(
                name: "mz_storage_upsert_multi_get_latency",
                help: "The latencies, in fractional seconds, \
                    of getting values from the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            multi_get_size: registry.register(metric!(
                name: "mz_storage_upsert_multi_get_size_total",
                help: "The batch size, \
                    of getting values from the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id", "worker_id"],
            )),
            multi_get_result_count: registry.register(metric!(
                name: "mz_storage_upsert_multi_get_result_count_total",
                help: "The number of non-empty records returned in a multi_get batch. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id", "worker_id"],
            )),
            multi_get_result_bytes: registry.register(metric!(
                name: "mz_storage_upsert_multi_get_result_bytes_total",
                help: "The total size of records returned in a multi_get batch. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id", "worker_id"],
            )),
            multi_put_latency: registry.register(metric!(
                name: "mz_storage_upsert_multi_put_latency",
                help: "The latencies, in fractional seconds, \
                    of getting values into the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            multi_put_size: registry.register(metric!(
                name: "mz_storage_upsert_multi_put_size_total",
                help: "The batch size, \
                    of getting values into the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id", "worker_id"],
            )),
            shared,
            rocksdb_multi_get_latency: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_latency",
                help: "The latencies, in fractional seconds, \
                    of getting batches of values from RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            rocksdb_multi_get_size: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_size_total",
                help: "The batch size, \
                    of getting batches of values from RocksDB for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_multi_get_result_count: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_result_count_total",
                help: "The number of non-empty records returned, \
                    when getting batches of values from RocksDB for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_multi_get_result_bytes: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_result_bytes_total",
                help: "The total size of records returned, \
                    when getting batches of values from RocksDB for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_multi_get_count: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_count_total",
                help: "The number of calls to rocksdb multi_get.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_multi_put_count: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_put_count_total",
                help: "The number of calls to rocksdb multi_put.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_multi_put_latency: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_put_latency",
                help: "The latencies, in fractional seconds, \
                    of putting batches of values into RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            rocksdb_multi_put_size: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_put_size_total",
                help: "The batch size, \
                    of putting batches of values into RocksDB for this source.",
                var_labels: ["source_id", "worker_id"],
            )),
            rocksdb_shared,
        }
    }

    /// Get a shared-across-workers instance of an `UpsertSharedMetrics`.
    pub(crate) fn shared(&self, source_id: &GlobalId) -> Arc<UpsertSharedMetrics> {
        let mut shared = self.shared.lock().expect("mutex poisoned");
        if let Some(shared_metrics) = shared.get(source_id) {
            if let Some(shared_metrics) = shared_metrics.upgrade() {
                return Arc::clone(&shared_metrics);
            } else {
                assert!(shared.remove(source_id).is_some());
            }
        }
        let shared_metrics = Arc::new(UpsertSharedMetrics::new(self, *source_id));
        assert!(
            shared
                .insert(source_id.clone(), Arc::downgrade(&shared_metrics))
                .is_none()
        );
        shared_metrics
    }

    /// Get a shared-across-workers instance of an `RocksDBSharedMetrics`
    pub(crate) fn rocksdb_shared(
        &self,
        source_id: &GlobalId,
    ) -> Arc<mz_rocksdb::RocksDBSharedMetrics> {
        let mut rocksdb = self.rocksdb_shared.lock().expect("mutex poisoned");
        if let Some(shared_metrics) = rocksdb.get(source_id) {
            if let Some(shared_metrics) = shared_metrics.upgrade() {
                return Arc::clone(&shared_metrics);
            } else {
                assert!(rocksdb.remove(source_id).is_some());
            }
        }

        let rocksdb_metrics = {
            let source_id = source_id.to_string();
            mz_rocksdb::RocksDBSharedMetrics {
                multi_get_latency: self
                    .rocksdb_multi_get_latency
                    .get_delete_on_drop_metric(vec![source_id.clone()]),
                multi_put_latency: self
                    .rocksdb_multi_put_latency
                    .get_delete_on_drop_metric(vec![source_id.clone()]),
            }
        };

        let rocksdb_metrics = Arc::new(rocksdb_metrics);
        assert!(
            rocksdb
                .insert(source_id.clone(), Arc::downgrade(&rocksdb_metrics))
                .is_none()
        );
        rocksdb_metrics
    }
}

/// Metrics for upsert source shared across workers.
#[derive(Debug)]
pub(crate) struct UpsertSharedMetrics {
    pub(crate) merge_snapshot_latency: DeleteOnDropHistogram<Vec<String>>,
    pub(crate) multi_get_latency: DeleteOnDropHistogram<Vec<String>>,
    pub(crate) multi_put_latency: DeleteOnDropHistogram<Vec<String>>,
}

impl UpsertSharedMetrics {
    /// Create an `UpsertSharedMetrics` from the `UpsertMetricDefs`
    fn new(metrics: &UpsertMetricDefs, source_id: GlobalId) -> Self {
        let source_id = source_id.to_string();
        UpsertSharedMetrics {
            merge_snapshot_latency: metrics
                .merge_snapshot_latency
                .get_delete_on_drop_metric(vec![source_id.clone()]),
            multi_get_latency: metrics
                .multi_get_latency
                .get_delete_on_drop_metric(vec![source_id.clone()]),
            multi_put_latency: metrics
                .multi_put_latency
                .get_delete_on_drop_metric(vec![source_id.clone()]),
        }
    }
}

/// Metrics related to backpressure in `UPSERT` dataflows.
#[derive(Clone, Debug)]
pub(crate) struct UpsertBackpressureMetricDefs {
    pub(crate) emitted_bytes: IntCounterVec,
    pub(crate) last_backpressured_bytes: UIntGaugeVec,
    pub(crate) retired_bytes: IntCounterVec,
}

impl UpsertBackpressureMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        // We add a `worker_id` label here, even though only 1 worker is ever
        // active, as this is the simplest way to avoid the non-active
        // workers from un-registering metrics. This is consistent with how
        // `persist_sink` metrics work.
        Self {
            emitted_bytes: registry.register(metric!(
                name: "mz_storage_upsert_backpressure_emitted_bytes",
                help: "A counter with the number of emitted bytes.",
                var_labels: ["source_id", "worker_id"],
            )),
            last_backpressured_bytes: registry.register(metric!(
                name: "mz_storage_upsert_backpressure_last_backpressured_bytes",
                help: "The last count of bytes we are waiting to be retired in \
                    the operator. This cannot be directly compared to \
                    `retired_bytes`, but CAN indicate that backpressure is happening.",
                var_labels: ["source_id", "worker_id"],
            )),
            retired_bytes: registry.register(metric!(
                name: "mz_storage_upsert_backpressure_retired_bytes",
                help:"A counter with the number of bytes retired by downstream processing.",
                var_labels: ["source_id", "worker_id"],
            )),
        }
    }
}

/// Metrics for the `upsert` operator.
pub struct UpsertMetrics {
    pub(crate) rehydration_latency: DeleteOnDropGauge<AtomicF64, Vec<String>>,
    pub(crate) rehydration_total: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) rehydration_updates: DeleteOnDropGauge<AtomicU64, Vec<String>>,

    pub(crate) merge_snapshot_updates: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) merge_snapshot_inserts: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) merge_snapshot_deletes: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) upsert_inserts: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) upsert_updates: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) upsert_deletes: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) multi_get_size: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) multi_get_result_bytes: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) multi_get_result_count: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) multi_put_size: DeleteOnDropCounter<AtomicU64, Vec<String>>,

    pub(crate) shared: Arc<UpsertSharedMetrics>,
    pub(crate) rocksdb_shared: Arc<mz_rocksdb::RocksDBSharedMetrics>,
    pub(crate) rocksdb_instance_metrics: Arc<mz_rocksdb::RocksDBInstanceMetrics>,
    // `UpsertMetrics` keeps a reference (through `Arc`'s) to backpressure metrics, so that
    // they are not dropped when the `persist_source` operator is dropped.
    _backpressure_metrics: Option<BackpressureMetrics>,
}

impl UpsertMetrics {
    /// Create an `UpsertMetrics` from the `UpsertMetricDefs`.
    pub(crate) fn new(
        defs: &UpsertMetricDefs,
        source_id: GlobalId,
        worker_id: usize,
        backpressure_metrics: Option<BackpressureMetrics>,
    ) -> Self {
        let source_id_s = source_id.to_string();
        let worker_id = worker_id.to_string();
        Self {
            rehydration_latency: defs
                .rehydration_latency
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            rehydration_total: defs
                .rehydration_total
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            rehydration_updates: defs
                .rehydration_updates
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            merge_snapshot_updates: defs
                .merge_snapshot_updates
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            merge_snapshot_inserts: defs
                .merge_snapshot_inserts
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            merge_snapshot_deletes: defs
                .merge_snapshot_deletes
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            upsert_inserts: defs
                .upsert_inserts
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            upsert_updates: defs
                .upsert_updates
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            upsert_deletes: defs
                .upsert_deletes
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            multi_get_size: defs
                .multi_get_size
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            multi_get_result_count: defs
                .multi_get_result_count
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            multi_get_result_bytes: defs
                .multi_get_result_bytes
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
            multi_put_size: defs
                .multi_put_size
                .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),

            shared: defs.shared(&source_id),
            rocksdb_shared: defs.rocksdb_shared(&source_id),
            rocksdb_instance_metrics: Arc::new(RocksDBInstanceMetrics {
                multi_get_size: defs
                    .rocksdb_multi_get_size
                    .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
                multi_get_result_count: defs
                    .rocksdb_multi_get_result_count
                    .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
                multi_get_result_bytes: defs
                    .rocksdb_multi_get_result_bytes
                    .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
                multi_get_count: defs
                    .rocksdb_multi_get_count
                    .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
                multi_put_count: defs
                    .rocksdb_multi_put_count
                    .get_delete_on_drop_metric(vec![source_id_s.clone(), worker_id.clone()]),
                multi_put_size: defs
                    .rocksdb_multi_put_size
                    .get_delete_on_drop_metric(vec![source_id_s, worker_id]),
            }),
            _backpressure_metrics: backpressure_metrics,
        }
    }
}
