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
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, IntCounter, IntCounterVec, IntGaugeVec,
    MetricsRegistry, UIntGaugeVec,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicI64, AtomicU64};

pub mod kafka;
pub mod mysql;
pub mod postgres;

/// Definitions for general metrics about sources that are not specific to the source type.
///
/// Registers metrics for
/// `SourceMetrics`, `OffsetCommitMetrics`, and `SourcePersistSinkMetrics`.
#[derive(Clone, Debug)]
pub(crate) struct GeneralSourceMetricDefs {
    // Source metrics
    pub(crate) resume_upper: IntGaugeVec,
    pub(crate) commit_upper_ready_times: UIntGaugeVec,
    pub(crate) commit_upper_accepted_times: UIntGaugeVec,

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
            resume_upper: registry.register(metric!(
                name: "mz_resume_upper",
                // TODO(guswynn): should this also track the resumption frontier operator?
                help: "The timestamp-domain resumption frontier chosen for a source's ingestion",
                var_labels: ["source_id"],
            )),
            commit_upper_ready_times: registry.register(metric!(
                name: "mz_source_commit_upper_ready_times",
                help: "The number of ready remap bindings that are held in the reclock commit upper operator.",
                var_labels: ["source_id", "worker_id"],
            )),
            commit_upper_accepted_times: registry.register(metric!(
                name: "mz_source_commit_upper_accepted_times",
                help: "The number of accepted remap bindings that are held in the reclock commit upper operator.",
                var_labels: ["source_id", "worker_id"],
            )),
            offset_commit_failures: registry.register(metric!(
                name: "mz_source_offset_commit_failures",
                help: "A counter representing how many times we have failed to commit offsets for a source",
                var_labels: ["source_id"],
            )),
            progress: registry.register(metric!(
                name: "mz_source_progress",
                help: "A timestamp gauge representing forward progess in the data shard",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
            row_inserts: registry.register(metric!(
                name: "mz_source_row_inserts",
                help: "A counter representing the actual number of rows being inserted to the data shard",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
            row_retractions: registry.register(metric!(
                name: "mz_source_row_retractions",
                help: "A counter representing the actual number of rows being retracted from the data shard",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
            error_inserts: registry.register(metric!(
                name: "mz_source_error_inserts",
                help: "A counter representing the actual number of errors being inserted to the data shard",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
            error_retractions: registry.register(metric!(
                name: "mz_source_error_retractions",
                help: "A counter representing the actual number of errors being retracted from the data shard",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
            persist_sink_processed_batches: registry.register(metric!(
                name: "mz_source_processed_batches",
                help: "A counter representing the number of persist sink batches with actual data \
                we have successfully processed.",
                var_labels: ["source_id", "shard", "worker_id"],
            )),
        }
    }
}

/// General metrics about sources that are not specific to the source type
pub(crate) struct SourceMetrics {
    /// The resume_upper for a source.
    pub(crate) resume_upper: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of ready remap bindings that are held in the reclock commit upper operator.
    pub(crate) commit_upper_ready_times: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The number of accepted remap bindings that are held in the reclock commit upper operator.
    pub(crate) commit_upper_accepted_times: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SourceMetrics {
    /// Initializes source metrics for a given (source_id, worker_id)
    pub(crate) fn new(
        defs: &GeneralSourceMetricDefs,
        source_id: GlobalId,
        worker_id: usize,
    ) -> SourceMetrics {
        SourceMetrics {
            resume_upper: defs
                .resume_upper
                .get_delete_on_drop_metric(vec![source_id.to_string()]),
            commit_upper_ready_times: defs
                .commit_upper_ready_times
                .get_delete_on_drop_metric(vec![source_id.to_string(), worker_id.to_string()]),
            commit_upper_accepted_times: defs
                .commit_upper_accepted_times
                .get_delete_on_drop_metric(vec![source_id.to_string(), worker_id.to_string()]),
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
    ) -> SourcePersistSinkMetrics {
        let shard = shard_id.to_string();
        SourcePersistSinkMetrics {
            progress: defs.progress.get_delete_on_drop_metric(vec![
                parent_source_id.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            row_inserts: defs.row_inserts.get_delete_on_drop_metric(vec![
                parent_source_id.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            row_retractions: defs.row_retractions.get_delete_on_drop_metric(vec![
                parent_source_id.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            error_inserts: defs.error_inserts.get_delete_on_drop_metric(vec![
                parent_source_id.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            error_retractions: defs.error_retractions.get_delete_on_drop_metric(vec![
                parent_source_id.to_string(),
                shard.clone(),
                worker_id.to_string(),
            ]),
            processed_batches: defs
                .persist_sink_processed_batches
                .get_delete_on_drop_metric(vec![
                    parent_source_id.to_string(),
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
                .get_delete_on_drop_metric(vec![source_id.to_string()]),
        }
    }
}

/// A set of base metrics that hang off a central metrics registry, labeled by the source they
/// belong to.
#[derive(Debug, Clone)]
pub(crate) struct SourceMetricDefs {
    pub(crate) source_defs: GeneralSourceMetricDefs,
    pub(crate) postgres_defs: postgres::PgSourceMetricDefs,
    pub(crate) mysql_defs: mysql::MySqlSourceMetricDefs,
    pub(crate) kafka_source_defs: kafka::KafkaSourceMetricDefs,
    /// A cluster-wide counter shared across all sources.
    pub(crate) bytes_read: IntCounter,
}

impl SourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            source_defs: GeneralSourceMetricDefs::register_with(registry),
            postgres_defs: postgres::PgSourceMetricDefs::register_with(registry),
            mysql_defs: mysql::MySqlSourceMetricDefs::register_with(registry),
            kafka_source_defs: kafka::KafkaSourceMetricDefs::register_with(registry),
            bytes_read: registry.register(metric!(
                name: "mz_bytes_read_total",
                help: "Count of bytes read from sources",
            )),
        }
    }
}
