// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! "Base" metrics used by all dataflow sources.
//!
//! We label metrics by the concrete source that they get emitted from (which makes these metrics
//! in-eligible for ingestion by third parties), so that means we have to register the metric
//! vectors to the registry once, and then generate concrete instantiations of them for the
//! appropriate source.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropHistogram, GaugeVec, HistogramVec, HistogramVecExt, IntCounter, IntCounterVec,
    IntGaugeVec, MetricsRegistry, UIntGaugeVec,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_repr::GlobalId;
use prometheus::core::{AtomicI64, GenericCounterVec};

#[derive(Clone, Debug)]
pub(super) struct SourceSpecificMetrics {
    pub(super) capability: UIntGaugeVec,
    pub(super) resume_upper: IntGaugeVec,
    /// A timestamp gauge representing forward progress
    /// in the data shard.
    pub(super) progress: IntGaugeVec,
    pub(super) row_inserts: IntCounterVec,
    pub(super) row_retractions: IntCounterVec,
    pub(super) error_inserts: IntCounterVec,
    pub(super) error_retractions: IntCounterVec,
    pub(super) persist_sink_processed_batches: IntCounterVec,
    pub(super) offset_commit_failures: IntCounterVec,
}

impl SourceSpecificMetrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            // TODO(guswynn): some of these metrics are not clear when subsources are involved, and
            // should be fixed
            capability: registry.register(metric!(
                name: "mz_capability",
                help: "The current capability for this dataflow. This corresponds to min(mz_partition_closed_ts)",
                var_labels: ["topic", "source_id", "worker_id"],
            )),
            resume_upper: registry.register(metric!(
                name: "mz_resume_upper",
                // TODO(guswynn): should this also track the resumption frontier operator?
                help: "The timestamp-domain resumption frontier chosen for a source's ingestion",
                var_labels: ["source_id"],
            )),
            progress: registry.register(metric!(
                name: "mz_source_progress",
                help: "A timestamp gauge representing forward progess in the data shard",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            row_inserts: registry.register(metric!(
                name: "mz_source_row_inserts",
                help: "A counter representing the actual number of rows being inserted to the data shard",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            row_retractions: registry.register(metric!(
                name: "mz_source_row_retractions",
                help: "A counter representing the actual number of rows being retracted from the data shard",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            error_inserts: registry.register(metric!(
                name: "mz_source_error_inserts",
                help: "A counter representing the actual number of errors being inserted to the data shard",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            error_retractions: registry.register(metric!(
                name: "mz_source_error_retractions",
                help: "A counter representing the actual number of errors being retracted from the data shard",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            persist_sink_processed_batches: registry.register(metric!(
                name: "mz_source_processed_batches",
                help: "A counter representing the number of persist sink batches with actual data \
                we have successfully processed.",
                var_labels: ["source_id", "output", "shard", "worker_id"],
            )),
            offset_commit_failures: registry.register(metric!(
                name: "mz_source_offset_commit_failures",
                help: "A counter representing how many times we have failed to commit offsets for a source",
                var_labels: ["source_id"],
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PartitionSpecificMetrics {
    pub(super) offset_ingested: UIntGaugeVec,
    pub(super) offset_received: UIntGaugeVec,
    pub(super) closed_ts: UIntGaugeVec,
    pub(super) messages_ingested: GenericCounterVec<AtomicI64>,
    pub(super) partition_offset_max: IntGaugeVec,
}

impl PartitionSpecificMetrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            offset_ingested: registry.register(metric!(
                name: "mz_partition_offset_ingested",
                help: "The most recent offset that we have ingested into a dataflow. This correspond to \
                 data that we have 1)ingested 2) assigned a timestamp",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
            offset_received: registry.register(metric!(
                name: "mz_partition_offset_received",
                help: "The most recent offset that we have been received by this source.",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
            closed_ts: registry.register(metric!(
                name: "mz_partition_closed_ts",
                help: "The highest closed timestamp for each partition in this dataflow",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
            messages_ingested: registry.register(metric!(
                name: "mz_messages_ingested",
                help: "The number of messages ingested per partition.",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
            partition_offset_max: registry.register(metric!(
                name: "mz_kafka_partition_offset_max",
                help: "High watermark offset on broker for partition",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PostgresSourceSpecificMetrics {
    pub(super) total_messages: IntCounterVec,
    pub(super) transactions: IntCounterVec,
    pub(super) ignored_messages: IntCounterVec,
    pub(super) insert_messages: IntCounterVec,
    pub(super) update_messages: IntCounterVec,
    pub(super) delete_messages: IntCounterVec,
    pub(super) tables_in_publication: UIntGaugeVec,
    pub(super) wal_lsn: UIntGaugeVec,
}

impl PostgresSourceSpecificMetrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            total_messages: registry.register(metric!(
                name: "mz_postgres_per_source_messages_total",
                help: "The total number of replication messages for this source, not expected to be the sum of the other values.",
                var_labels: ["source_id"],
            )),
            transactions: registry.register(metric!(
                name: "mz_postgres_per_source_transactions_total",
                help: "The number of committed transactions for all tables in this source",
                var_labels: ["source_id"],
            )),
            ignored_messages: registry.register(metric!(
                name: "mz_postgres_per_source_ignored_messages",
                help: "The number of messages ignored because of an irrelevant type or relation_id",
                var_labels: ["source_id"],
            )),
            insert_messages: registry.register(metric!(
                name: "mz_postgres_per_source_inserts",
                help: "The number of inserts for all tables in this source",
                var_labels: ["source_id"],
            )),
            update_messages: registry.register(metric!(
                name: "mz_postgres_per_source_updates",
                help: "The number of updates for all tables in this source",
                var_labels: ["source_id"],
            )),
            delete_messages: registry.register(metric!(
                name: "mz_postgres_per_source_deletes",
                help: "The number of deletes for all tables in this source",
                var_labels: ["source_id"],
            )),
            tables_in_publication: registry.register(metric!(
                name: "mz_postgres_per_source_tables_count",
                help: "The number of upstream tables for this source",
                var_labels: ["source_id"],
            )),
            wal_lsn: registry.register(metric!(
                name: "mz_postgres_per_source_wal_lsn",
                help: "LSN of the latest transaction committed for this source, see Postgres Replication docs for more details on LSN",
                var_labels: ["source_id"],
            ))
        }
    }
}

/// Metrics for the `upsert` operator.
#[derive(Clone, Debug)]
pub(super) struct UpsertMetrics {
    pub(super) rehydration_latency: GaugeVec,
    pub(super) rehydration_total: UIntGaugeVec,
    pub(super) rehydration_updates: UIntGaugeVec,

    // These are used by `shared`.
    pub(super) merge_snapshot_latency: HistogramVec,
    pub(super) merge_snapshot_updates: HistogramVec,
    pub(super) multi_get_latency: HistogramVec,
    pub(super) multi_get_size: HistogramVec,
    pub(super) multi_put_latency: HistogramVec,
    pub(super) multi_put_size: HistogramVec,

    // These are used by `rocksdb`.
    pub(super) rocksdb_multi_get_latency: HistogramVec,
    pub(super) rocksdb_multi_get_size: HistogramVec,
    pub(super) rocksdb_multi_put_latency: HistogramVec,
    pub(super) rocksdb_multi_put_size: HistogramVec,
    // These are maps so that multiple timely workers can interact with the same
    // `DeleteOnDropHistogram`, which is only dropped once ALL workers drop it.
    // The map may contain arbitrary, old `Weak`s for deleted sources, which are
    // only cleaned if those sources are recreated.
    //
    // We don't parameterize these by `worker_id` like the `rehydration_*` ones
    // to save on time-series cardinality.
    pub(super) shared: Arc<Mutex<BTreeMap<GlobalId, Weak<UpsertSharedMetrics>>>>,
    pub(super) rocksdb: Arc<Mutex<BTreeMap<GlobalId, Weak<mz_rocksdb::RocksDBMetrics>>>>,
}

impl UpsertMetrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        let shared = Arc::new(Mutex::new(BTreeMap::new()));
        let rocksdb = Arc::new(Mutex::new(BTreeMap::new()));
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
                name: "mz_storage_upsert_merge_snapshot_updates",
                help: "The batch size, \
                    of merging snapshot updates into upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: vec![10.0, 100.0, 1000.0, 10000.0, 100000.0],
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
            // Choose a relatively low number of representative buckets.
            multi_get_size: registry.register(metric!(
                name: "mz_storage_upsert_multi_get_size",
                help: "The batch size, \
                    of getting values from the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: vec![10.0, 100.0, 1000.0, 10000.0, 100000.0],
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
            // Choose a relatively low number of representative buckets.
            multi_put_size: registry.register(metric!(
                name: "mz_storage_upsert_multi_put_size",
                help: "The batch size, \
                    of getting values into the upsert state for this source. \
                    Specific implementations of upsert state may have more detailed \
                    metrics about sub-batches.",
                var_labels: ["source_id"],
                buckets: vec![10.0, 100.0, 1000.0, 10000.0, 100000.0],
            )),
            shared,
            rocksdb_multi_get_latency: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_latency",
                help: "The latencies, in fractional seconds, \
                    of getting batches of values from RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            // Choose a relatively low number of representative buckets.
            rocksdb_multi_get_size: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_get_size",
                help: "The batch size, \
                    of getting batches of values from RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: vec![10.0, 100.0, 1000.0, 10000.0, 100000.0],
            )),
            rocksdb_multi_put_latency: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_put_latency",
                help: "The latencies, in fractional seconds, \
                    of putting batches of values into RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
            )),
            // Choose a relatively low number of representative buckets.
            rocksdb_multi_put_size: registry.register(metric!(
                name: "mz_storage_rocksdb_multi_put_size",
                help: "The batch size, \
                    of putting batches of values into RocksDB for this source.",
                var_labels: ["source_id"],
                buckets: vec![10.0, 100.0, 1000.0, 10000.0, 100000.0],
            )),
            rocksdb,
        }
    }

    pub(super) fn shared(&self, source_id: &GlobalId) -> Arc<UpsertSharedMetrics> {
        let mut shared = self.shared.lock().expect("mutex poisoned");
        if let Some(shared_metrics) = shared.get(source_id) {
            if let Some(shared_metrics) = shared_metrics.upgrade() {
                return Arc::clone(&shared_metrics);
            } else {
                assert!(shared.remove(source_id).is_some());
            }
        }
        let shared_metrics = Arc::new(UpsertSharedMetrics::new(source_id, self));
        assert!(shared
            .insert(source_id.clone(), Arc::downgrade(&shared_metrics))
            .is_none());
        shared_metrics
    }

    pub(super) fn rocksdb(&self, source_id: &GlobalId) -> Arc<mz_rocksdb::RocksDBMetrics> {
        let mut rocksdb = self.rocksdb.lock().expect("mutex poisoned");
        if let Some(shared_metrics) = rocksdb.get(source_id) {
            if let Some(shared_metrics) = shared_metrics.upgrade() {
                return Arc::clone(&shared_metrics);
            } else {
                assert!(rocksdb.remove(source_id).is_some());
            }
        }

        let rocksdb_metrics = {
            let source_id = source_id.to_string();
            mz_rocksdb::RocksDBMetrics {
                multi_get_latency: self
                    .rocksdb_multi_get_latency
                    .get_delete_on_drop_histogram(vec![source_id.clone()]),
                multi_get_size: self
                    .rocksdb_multi_get_size
                    .get_delete_on_drop_histogram(vec![source_id.clone()]),
                multi_put_latency: self
                    .rocksdb_multi_put_latency
                    .get_delete_on_drop_histogram(vec![source_id.clone()]),
                multi_put_size: self
                    .rocksdb_multi_put_size
                    .get_delete_on_drop_histogram(vec![source_id]),
            }
        };

        let rocksdb_metrics = Arc::new(rocksdb_metrics);
        assert!(rocksdb
            .insert(source_id.clone(), Arc::downgrade(&rocksdb_metrics))
            .is_none());
        rocksdb_metrics
    }
}

#[derive(Debug)]
pub(crate) struct UpsertSharedMetrics {
    pub(crate) merge_snapshot_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    pub(crate) merge_snapshot_updates: DeleteOnDropHistogram<'static, Vec<String>>,
    pub(crate) multi_get_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    pub(crate) multi_get_size: DeleteOnDropHistogram<'static, Vec<String>>,
    pub(crate) multi_put_latency: DeleteOnDropHistogram<'static, Vec<String>>,
    pub(crate) multi_put_size: DeleteOnDropHistogram<'static, Vec<String>>,
}

impl UpsertSharedMetrics {
    fn new(source_id: &GlobalId, metrics: &UpsertMetrics) -> Self {
        let source_id = source_id.to_string();
        UpsertSharedMetrics {
            merge_snapshot_latency: metrics
                .merge_snapshot_latency
                .get_delete_on_drop_histogram(vec![source_id.clone()]),
            merge_snapshot_updates: metrics
                .merge_snapshot_updates
                .get_delete_on_drop_histogram(vec![source_id.clone()]),
            multi_get_latency: metrics
                .multi_get_latency
                .get_delete_on_drop_histogram(vec![source_id.clone()]),
            multi_get_size: metrics
                .multi_get_size
                .get_delete_on_drop_histogram(vec![source_id.clone()]),
            multi_put_latency: metrics
                .multi_put_latency
                .get_delete_on_drop_histogram(vec![source_id.clone()]),
            multi_put_size: metrics
                .multi_put_size
                .get_delete_on_drop_histogram(vec![source_id]),
        }
    }
}

/// A set of base metrics that hang off a central metrics registry, labeled by the source they
/// belong to.
#[derive(Debug, Clone)]
pub struct SourceBaseMetrics {
    pub(super) source_specific: SourceSpecificMetrics,
    pub(super) partition_specific: PartitionSpecificMetrics,
    pub(super) postgres_source_specific: PostgresSourceSpecificMetrics,

    pub(super) upsert_specific: UpsertMetrics,

    pub(crate) bytes_read: IntCounter,

    /// Metrics that are also exposed to users.
    pub(crate) source_statistics: crate::statistics::SourceStatisticsMetricsDefinitions,
}

impl SourceBaseMetrics {
    /// TODO(undocumented)
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            source_specific: SourceSpecificMetrics::register_with(registry),
            partition_specific: PartitionSpecificMetrics::register_with(registry),
            postgres_source_specific: PostgresSourceSpecificMetrics::register_with(registry),

            upsert_specific: UpsertMetrics::register_with(registry),

            bytes_read: registry.register(metric!(
                name: "mz_bytes_read_total",
                help: "Count of bytes read from sources",
            )),
            source_statistics: crate::statistics::SourceStatisticsMetricsDefinitions::register_with(
                registry,
            ),
        }
    }
}
