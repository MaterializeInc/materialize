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

use super::upsert::{UpsertBackpressureMetricDefs, UpsertMetricDefs};

/// Source-specific metrics in the persist sink
pub(crate) struct SourcePersistSinkMetrics {
    pub(crate) progress: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    pub(crate) row_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) row_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) error_retractions: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) processed_batches: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl SourcePersistSinkMetrics {
    /// Initialises source metrics used in the `persist_sink`.
    pub(crate) fn new(
        base: &SourceBaseMetrics,
        _source_id: GlobalId,
        parent_source_id: GlobalId,
        worker_id: usize,
        shard_id: &mz_persist_client::ShardId,
        output_index: usize,
    ) -> SourcePersistSinkMetrics {
        let shard = shard_id.to_string();
        SourcePersistSinkMetrics {
            progress: base.source_specific.progress.get_delete_on_drop_gauge(vec![
                parent_source_id.to_string(),
                output_index.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            row_inserts: base
                .source_specific
                .row_inserts
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                    worker_id.to_string(),
                ]),
            row_retractions: base
                .source_specific
                .row_retractions
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                    worker_id.to_string(),
                ]),
            error_inserts: base
                .source_specific
                .error_inserts
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                    worker_id.to_string(),
                ]),
            error_retractions: base
                .source_specific
                .error_retractions
                .get_delete_on_drop_counter(vec![
                    parent_source_id.to_string(),
                    output_index.to_string(),
                    shard.clone(),
                    worker_id.to_string(),
                ]),
            processed_batches: base
                .source_specific
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

/// Source-specific Prometheus metrics
pub(crate) struct SourceMetrics {
    /// Value of the capability associated with this source
    pub(crate) capability: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The resume_upper for a source.
    pub(crate) resume_upper: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// Per-partition Prometheus metrics.
    pub(crate) partition_metrics: BTreeMap<PartitionId, PartitionMetrics>,
    /// The number of in-memory remap bindings that reclocking a time needs to iterate over.
    pub(crate) inmemory_remap_bindings: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    source_name: String,
    source_id: GlobalId,
    base_metrics: SourceBaseMetrics,
}

impl SourceMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub(crate) fn new(
        base: &SourceBaseMetrics,
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
            capability: base
                .source_specific
                .capability
                .get_delete_on_drop_gauge(labels.to_vec()),
            resume_upper: base
                .source_specific
                .resume_upper
                .get_delete_on_drop_gauge(vec![source_id.to_string()]),
            inmemory_remap_bindings: base
                .source_specific
                .inmemory_remap_bindings
                .get_delete_on_drop_gauge(vec![source_id.to_string(), worker_id.to_string()]),
            partition_metrics: Default::default(),
            source_name: source_name.to_string(),
            source_id,
            base_metrics: base.clone(),
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
                        &self.base_metrics,
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

/// Partition-specific metrics, recorded to both Prometheus and a system table
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

    /// Initialises partition metrics for a given (source_id, partition_id)
    pub(crate) fn new(
        base_metrics: &SourceBaseMetrics,
        source_name: &str,
        source_id: GlobalId,
        partition_id: &PartitionId,
    ) -> PartitionMetrics {
        let labels = &[
            source_name.to_string(),
            source_id.to_string(),
            partition_id.to_string(),
        ];
        let base = &base_metrics.partition_specific;
        PartitionMetrics {
            offset_ingested: base
                .offset_ingested
                .get_delete_on_drop_gauge(labels.to_vec()),
            offset_received: base
                .offset_received
                .get_delete_on_drop_gauge(labels.to_vec()),
            closed_ts: base.closed_ts.get_delete_on_drop_gauge(labels.to_vec()),
            messages_ingested: base
                .messages_ingested
                .get_delete_on_drop_counter(labels.to_vec()),
            last_offset: 0,
            last_timestamp: 0,
        }
    }
}

/// Source reader operator specific Prometheus metrics
pub(crate) struct SourceReaderMetrics {
    source_id: GlobalId,
    base_metrics: SourceBaseMetrics,
}

impl SourceReaderMetrics {
    /// Initialises source metrics for a given (source_id, worker_id)
    pub(crate) fn new(base: &SourceBaseMetrics, source_id: GlobalId) -> SourceReaderMetrics {
        SourceReaderMetrics {
            source_id,
            base_metrics: base.clone(),
        }
    }

    /// Get metrics struct for offset committing.
    pub(crate) fn offset_commit_metrics(&self) -> OffsetCommitMetrics {
        OffsetCommitMetrics::new(&self.base_metrics, self.source_id)
    }
}

/// Metrics about committing offsets
pub(crate) struct OffsetCommitMetrics {
    /// The offset-domain resume_upper for a source.
    pub(crate) offset_commit_failures: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
}

impl OffsetCommitMetrics {
    /// Initialises partition metrics for a given (source_id, partition_id)
    pub(crate) fn new(
        base_metrics: &SourceBaseMetrics,
        source_id: GlobalId,
    ) -> OffsetCommitMetrics {
        let base = &base_metrics.source_specific;
        OffsetCommitMetrics {
            offset_commit_failures: base
                .offset_commit_failures
                .get_delete_on_drop_counter(vec![source_id.to_string()]),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SourceSpecificMetrics {
    pub(crate) capability: UIntGaugeVec,
    pub(crate) resume_upper: IntGaugeVec,
    /// A timestamp gauge representing forward progress
    /// in the data shard.
    pub(crate) progress: IntGaugeVec,
    pub(crate) row_inserts: IntCounterVec,
    pub(crate) row_retractions: IntCounterVec,
    pub(crate) error_inserts: IntCounterVec,
    pub(crate) error_retractions: IntCounterVec,
    pub(crate) persist_sink_processed_batches: IntCounterVec,
    pub(crate) offset_commit_failures: IntCounterVec,
    pub(crate) inmemory_remap_bindings: UIntGaugeVec,
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
            inmemory_remap_bindings: registry.register(metric!(
                name: "mz_source_inmemory_remap_bindings",
                help: "The number of in-memory remap bindings that reclocking a time needs to iterate over.",
                var_labels: ["source_id", "worker_id"],
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionSpecificMetrics {
    pub(crate) offset_ingested: UIntGaugeVec,
    pub(crate) offset_received: UIntGaugeVec,
    pub(crate) closed_ts: UIntGaugeVec,
    pub(crate) messages_ingested: GenericCounterVec<AtomicI64>,
    pub(crate) partition_offset_max: IntGaugeVec,
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
pub(crate) struct PostgresSourceSpecificMetrics {
    pub(crate) total_messages: IntCounterVec,
    pub(crate) transactions: IntCounterVec,
    pub(crate) ignored_messages: IntCounterVec,
    pub(crate) insert_messages: IntCounterVec,
    pub(crate) update_messages: IntCounterVec,
    pub(crate) delete_messages: IntCounterVec,
    pub(crate) tables_in_publication: UIntGaugeVec,
    pub(crate) wal_lsn: UIntGaugeVec,
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

/// A set of base metrics that hang off a central metrics registry, labeled by the source they
/// belong to.
#[derive(Debug, Clone)]
pub struct SourceBaseMetrics {
    pub(crate) source_specific: SourceSpecificMetrics,
    pub(crate) partition_specific: PartitionSpecificMetrics,
    pub(crate) postgres_source_specific: PostgresSourceSpecificMetrics,

    pub(crate) upsert_specific: UpsertMetricDefs,
    pub(crate) upsert_backpressure_specific: UpsertBackpressureMetricDefs,

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

            upsert_specific: UpsertMetricDefs::register_with(registry),
            upsert_backpressure_specific: UpsertBackpressureMetricDefs::register_with(registry),

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
