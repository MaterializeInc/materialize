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

use mz_ore::metric;
use mz_ore::metrics::{IntCounter, IntCounterVec, IntGaugeVec, MetricsRegistry, UIntGaugeVec};
use prometheus::core::{AtomicI64, GenericCounterVec};

/// The base metrics set for the s3 module.
#[derive(Clone, Debug)]
pub(crate) struct S3Metrics {
    pub(crate) objects_downloaded: IntCounterVec,
    pub(crate) objects_duplicate: IntCounterVec,
    pub(crate) bytes_downloaded: IntCounterVec,
    pub(crate) messages_ingested: IntCounterVec,

    pub(crate) objects_discovered: IntCounterVec,
}

impl S3Metrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            objects_downloaded: registry.register(metric!(
                name: "mz_s3_objects_downloaded",
                help: "The number of s3 objects that we have downloaded.",
                var_labels: ["bucket_id", "source_id"],
            )),
            objects_duplicate: registry.register(metric!(
                name: "mz_s3_objects_duplicate_detected",
                help: "The number of s3 objects that are duplicates, and therefore not downloaded.",
                var_labels: ["bucket_id", "source_id"],
            )),
            bytes_downloaded: registry.register(metric!(
                name: "mz_s3_bytes_downloaded",
                help: "The total count of bytes downloaded for this source.",
                var_labels: ["bucket_id", "source_id"],
            )),
            messages_ingested: registry.register(metric!(
                name: "mz_s3_messages_ingested",
                help: "The number of messages ingested for this bucket.",
                var_labels: ["bucket_id", "source_id"],
            )),

            objects_discovered: registry.register(metric!(
                name: "mz_s3_objects_discovered",
                help: "The number of s3 objects that we have discovered via SCAN or SQS.",
                var_labels: ["bucket_id", "source_id"],
            )),
        }
    }
}

/// The base metrics set for the kinesis module.
#[derive(Clone, Debug)]
pub(crate) struct KinesisMetrics {
    pub(crate) millis_behind_latest: IntGaugeVec,
}

impl KinesisMetrics {
    fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            millis_behind_latest: registry.register(metric!(
                name: "mz_kinesis_shard_millis_behind_latest",
                help: "How far the shard is behind the tip of the stream",
                var_labels: ["stream_name", "shard_id"],
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct SourceSpecificMetrics {
    pub(super) enable_multi_worker_storage_persist_sink: IntGaugeVec,
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
            enable_multi_worker_storage_persist_sink: registry.register(metric!(
                name: "mz_source_enable_multi_worker_storage_persist_sink",
                help: "Whether or not the new multi-worker persist_sink is actively being used",
                var_labels: ["source_id", "worker_id", "parent_source_id", "output"],
            )),
            // TODO(guswynn): some of these metrics are broken when there are more than 1
            // worker-id, and need to be fixed
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
                var_labels: ["source_id", "output", "shard"],
            )),
            row_inserts: registry.register(metric!(
                name: "mz_source_row_inserts",
                help: "A counter representing the actual number of rows being inserted to the data shard",
                var_labels: ["source_id", "output", "shard"],
            )),
            row_retractions: registry.register(metric!(
                name: "mz_source_row_retractions",
                help: "A counter representing the actual number of rows being retracted from the data shard",
                var_labels: ["source_id", "output", "shard"],
            )),
            error_inserts: registry.register(metric!(
                name: "mz_source_error_inserts",
                help: "A counter representing the actual number of errors being inserted to the data shard",
                var_labels: ["source_id", "output", "shard"],
            )),
            error_retractions: registry.register(metric!(
                name: "mz_source_error_retractions",
                help: "A counter representing the actual number of errors being retracted from the data shard",
                var_labels: ["source_id", "output", "shard"],
            )),
            persist_sink_processed_batches: registry.register(metric!(
                name: "mz_source_processed_batches",
                help: "A counter representing the number of persist sink batches with actual data \
                we have successfully processed.",
                var_labels: ["source_id", "output", "shard"],
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
    pub(super) source_resume_upper: UIntGaugeVec,
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
            source_resume_upper: registry.register(metric!(
                name: "mz_source_resume_upper",
                help: "The offset-domain upper that is used for initializing this partition",
                var_labels: ["source_id", "partition_id"],
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

/// A set of base metrics that hang off a central metrics registry, labeled by the source they
/// belong to.
#[derive(Debug, Clone)]
pub struct SourceBaseMetrics {
    pub(super) source_specific: SourceSpecificMetrics,
    pub(super) partition_specific: PartitionSpecificMetrics,
    pub(super) postgres_source_specific: PostgresSourceSpecificMetrics,

    pub(crate) s3: S3Metrics,
    pub(crate) kinesis: KinesisMetrics,

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

            s3: S3Metrics::register_with(registry),
            kinesis: KinesisMetrics::register_with(registry),

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
