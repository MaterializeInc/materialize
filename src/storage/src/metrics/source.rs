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

use mz_expr::PartitionId;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, IntCounter, IntCounterVec,
    IntGaugeVec, MetricsRegistry, UIntGaugeVec,
};
use mz_repr::GlobalId;
use mz_storage_types::sources::MzOffset;
use prometheus::core::{AtomicI64, AtomicU64, GenericCounterVec};

use super::kafka::KafkaPartitionMetricDefs;
use super::postgres::PgSourceMetricDefs;
use super::upsert::{UpsertBackpressureMetricDefs, UpsertMetricDefs};
use crate::statistics::SourceStatisticsMetricDefs;

/// Definitions for per-partition metrics.
#[derive(Clone, Debug)]
pub(crate) struct PartitionMetricDefs {
    pub(crate) offset_ingested: UIntGaugeVec,
    pub(crate) offset_received: UIntGaugeVec,
    pub(crate) closed_ts: UIntGaugeVec,
    pub(crate) messages_ingested: GenericCounterVec<AtomicI64>,
}

impl PartitionMetricDefs {
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
        }
    }
}

/// Partition-specific metrics.
pub(crate) struct PartitionMetrics {
    /// Highest offset that has been received by the source and timestamped
    pub(crate) offset_ingested: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Highest offset that has been received by the source
    pub(crate) offset_received: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Value of the highest timestamp that is closed (for which all messages have been ingested)
    pub(crate) closed_ts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// Total number of messages that have been received by the source and timestamped
    pub(crate) messages_ingested: DeleteOnDropCounter<'static, AtomicI64, Vec<String>>,
    pub(crate) last_offset: u64,
    pub(crate) last_timestamp: i64,
}

impl PartitionMetrics {
    /// Record the latest offset ingested high-water mark
    fn record_offset(
        &mut self,
        _source_name: &str,
        _source_id: GlobalId,
        _partition_id: &PartitionId,
        offset: u64,
        timestamp: i64,
    ) {
        self.offset_received.set(offset);
        self.offset_ingested.set(offset);
        self.last_offset = offset;
        self.last_timestamp = timestamp;
    }

    /// Initializes partition metrics for a given (source_id, partition_id)
    pub(crate) fn new(
        defs: &PartitionMetricDefs,
        source_name: &str,
        source_id: GlobalId,
        partition_id: &PartitionId,
    ) -> PartitionMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.to_string(),
            partition_id.to_string(),
        ];
        PartitionMetrics {
            offset_ingested: defs
                .offset_ingested
                .get_delete_on_drop_gauge(labels.to_vec()),
            offset_received: defs
                .offset_received
                .get_delete_on_drop_gauge(labels.to_vec()),
            closed_ts: defs.closed_ts.get_delete_on_drop_gauge(labels.to_vec()),
            messages_ingested: defs
                .messages_ingested
                .get_delete_on_drop_counter(labels.to_vec()),
            last_offset: 0,
            last_timestamp: 0,
        }
    }
}

/// Definitions for general metrics about sources that are not specific to the source type.
///
/// Registers metrics for
/// `SourceMetrics`, `PartitionMetrics`, `OffsetCommitMetrics`, and `SourcePersistSinkMetrics`.
#[derive(Clone, Debug)]
pub(crate) struct GeneralSourceMetricDefs {
    // Source metrics
    pub(crate) capability: UIntGaugeVec,
    pub(crate) resume_upper: IntGaugeVec,
    pub(crate) inmemory_remap_bindings: UIntGaugeVec,

    // Per-partition metrics
    pub(crate) partition_defs: PartitionMetricDefs,

    // OffsetCommitMetrics
    pub(crate) offset_commit_failures: IntCounterVec,

    // `persist_sink` metrics
    /// A timestamp gauge representing forward progress
    /// in the data shard.
    pub(crate) progress: IntGaugeVec,
    pub(crate) row_inserts: IntCounterVec,
    pub(crate) row_retractions: IntCounterVec,
    pub(crate) error_inserts: IntCounterVec,
    pub(crate) error_retractions: IntCounterVec,
    pub(crate) persist_sink_processed_batches: IntCounterVec,
}

impl GeneralSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
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
            inmemory_remap_bindings: registry.register(metric!(
                name: "mz_source_inmemory_remap_bindings",
                help: "The number of in-memory remap bindings that reclocking a time needs to iterate over.",
                var_labels: ["source_id", "worker_id"],
            )),
            offset_commit_failures: registry.register(metric!(
                name: "mz_source_offset_commit_failures",
                help: "A counter representing how many times we have failed to commit offsets for a source",
                var_labels: ["source_id"],
            )),
            partition_defs: PartitionMetricDefs::register_with(registry),
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
        }
    }
}

/// General metrics about sources that are not specific to the source type, including partitions
/// metrics.
pub(crate) struct SourceMetrics {
    /// Value of the capability associated with this source
    pub(crate) capability: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The resume_upper for a source.
    pub(crate) resume_upper: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Per-partition Prometheus metrics.
    pub(crate) partition_metrics: BTreeMap<PartitionId, PartitionMetrics>,
    /// The number of in-memory remap bindings that reclocking a time needs to iterate over.
    pub(crate) inmemory_remap_bindings: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,

    // Used for creation `partition_metrics` on-the-fly.
    partition_defs: PartitionMetricDefs,
    source_id: GlobalId,
    source_name: String,
}

impl SourceMetrics {
    /// Initializes source metrics for a given (source_id, worker_id)
    pub(crate) fn new(
        defs: &GeneralSourceMetricDefs,
        source_name: &str,
        source_id: GlobalId,
        worker_id: &str,
    ) -> SourceMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.to_string(),
            worker_id.to_string(),
        ];
        SourceMetrics {
            capability: defs.capability.get_delete_on_drop_gauge(labels.to_vec()),
            resume_upper: defs
                .resume_upper
                .get_delete_on_drop_gauge(vec![source_id.to_string()]),
            inmemory_remap_bindings: defs
                .inmemory_remap_bindings
                .get_delete_on_drop_gauge(vec![source_id.to_string(), worker_id.to_string()]),
            partition_metrics: Default::default(),
            partition_defs: defs.partition_defs.clone(),
            source_id,
            source_name: source_name.to_string(),
        }
    }

    /// Log updates to which offsets / timestamps read up to.
    pub(crate) fn record_partition_offsets(
        &mut self,
        offsets: BTreeMap<PartitionId, (MzOffset, mz_repr::Timestamp, i64)>,
    ) {
        for (partition, (offset, timestamp, count)) in offsets {
            let metric = self
                .partition_metrics
                .entry(partition.clone())
                .or_insert_with(|| {
                    PartitionMetrics::new(
                        &self.partition_defs,
                        &self.source_name,
                        self.source_id,
                        &partition,
                    )
                });

            metric.messages_ingested.inc_by(count);

            metric.record_offset(
                &self.source_name,
                self.source_id,
                &partition,
                offset.offset,
                i64::try_from(timestamp).expect("materialize exists after 250M AD"),
            );
        }
    }
}

/// Source-specific metrics used by the `persist_sink`
pub(crate) struct SourcePersistSinkMetrics {
    pub(crate) progress: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    pub(crate) row_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) row_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) processed_batches: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl SourcePersistSinkMetrics {
    /// Initializes source metrics used in the `persist_sink`.
    pub(crate) fn new(
        defs: &GeneralSourceMetricDefs,
        _source_id: GlobalId,
        parent_source_id: GlobalId,
        worker_id: usize,
        shard_id: &mz_persist_client::ShardId,
        output_index: usize,
    ) -> SourcePersistSinkMetrics {
        let shard = shard_id.to_string();
        SourcePersistSinkMetrics {
            progress: defs.progress.get_delete_on_drop_gauge(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            row_inserts: defs.row_inserts.get_delete_on_drop_counter(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            row_retractions: defs.row_retractions.get_delete_on_drop_counter(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            error_inserts: defs.error_inserts.get_delete_on_drop_counter(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            error_retractions: defs.error_retractions.get_delete_on_drop_counter(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            processed_batches: defs
                .persist_sink_processed_batches
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard,
                    worker_id.to_string(),
                ]),
        }
    }
}

/// Metrics about committing offsets.
pub(crate) struct OffsetCommitMetrics {
    /// The offset-domain resume_upper for a source.
    pub(crate) offset_commit_failures: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl OffsetCommitMetrics {
    /// Initialises partition metrics for a given (source_id, partition_id)
    pub(crate) fn new(defs: &GeneralSourceMetricDefs, source_id: GlobalId) -> OffsetCommitMetrics {
        OffsetCommitMetrics {
            offset_commit_failures: defs
                .offset_commit_failures
                .get_delete_on_drop_counter(vec![source_id.to_string()]),
        }
    }
}

/// A set of base metrics that hang off a central metrics registry, labeled by the source they
/// belong to.
#[derive(Debug, Clone)]
pub struct SourceBaseMetrics {
    pub(crate) source_defs: GeneralSourceMetricDefs,
    pub(crate) postgres_defs: PgSourceMetricDefs,
    pub(crate) kafka_partition_defs: KafkaPartitionMetricDefs,
    pub(crate) upsert_defs: UpsertMetricDefs,
    pub(crate) upsert_backpressure_defs: UpsertBackpressureMetricDefs,
    /// A cluster-wide counter shared across all sources.
    pub(crate) bytes_read: IntCounter,
    /// Metrics that are also exposed to users. Defined in the `statistics` module because
    /// they have mappings for table insertion.
    pub(crate) source_statistics: SourceStatisticsMetricDefs,
}

impl SourceBaseMetrics {
    /// Register the `SourceBaseMetrics` with the registry.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            source_defs: GeneralSourceMetricDefs::register_with(registry),
            postgres_defs: PgSourceMetricDefs::register_with(registry),
            kafka_partition_defs: KafkaPartitionMetricDefs::register_with(registry),
            upsert_defs: UpsertMetricDefs::register_with(registry),
            upsert_backpressure_defs: UpsertBackpressureMetricDefs::register_with(registry),
            bytes_read: registry.register(metric!(
                name: "mz_bytes_read_total",
                help: "Count of bytes read from sources",
            )),
            source_statistics: SourceStatisticsMetricDefs::register_with(registry),
        }
    }
}
