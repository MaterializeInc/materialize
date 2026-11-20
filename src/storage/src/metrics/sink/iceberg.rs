// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for iceberg sinks.

use mz_ore::{
    metric,
    metrics::{
        DeregisterOnDropCounter, DeregisterOnDropGauge, DeregisterOnDropHistogram,
        DeregisterOnDropVec, MetricTag, MetricVisibility, raw,
    },
    stats::histogram_seconds_buckets,
};
use mz_repr::GlobalId;
use prometheus::core::AtomicU64;

#[derive(Debug, Clone)]
pub(crate) struct IcebergSinkMetricDefs {
    /// Number of data files written by the iceberg sink.
    pub data_files_written: DeregisterOnDropVec<raw::IntCounterVec>,
    /// Number of delete files written by the iceberg sink.
    pub delete_files_written: DeregisterOnDropVec<raw::IntCounterVec>,
    /// Number of stashed rows in the iceberg sink.
    pub stashed_rows: DeregisterOnDropVec<raw::UIntGaugeVec>,
    /// Number of snapshots committed by the iceberg sink.
    pub snapshots_committed: DeregisterOnDropVec<raw::IntCounterVec>,
    /// Commit failures in the iceberg sink.
    pub commit_failures: DeregisterOnDropVec<raw::IntCounterVec>,
    /// Commit conflicts in the iceberg sink.
    pub commit_conflicts: DeregisterOnDropVec<raw::IntCounterVec>,
    /// Time spent committing batches to Iceberg.
    pub commit_duration_seconds: DeregisterOnDropVec<raw::HistogramVec>,
    /// Time spent closing Iceberg DeltaWriters.
    pub writer_close_duration_seconds: DeregisterOnDropVec<raw::HistogramVec>,
}

impl IcebergSinkMetricDefs {
    pub(crate) fn register_with(registry: &mz_ore::metrics::MetricsRegistry) -> Self {
        Self {
            data_files_written: registry.register(metric!(
                name: "mz_sink_iceberg_data_files_written",
                help: "Number of data files written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            delete_files_written: registry.register(metric!(
                name: "mz_sink_iceberg_delete_files_written",
                help: "Number of delete files written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            stashed_rows: registry.register(metric!(
                name: "mz_sink_iceberg_stashed_rows",
                help: "Number of stashed rows in the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            snapshots_committed: registry.register(metric!(
                name: "mz_sink_iceberg_snapshots_committed",
                help: "Number of snapshots committed by the iceberg sink",
                var_labels: ["sink_id", "worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            commit_failures: registry.register(metric!(
                name: "mz_sink_iceberg_commit_failures",
                help: "Number of commit failures in the iceberg sink",
                var_labels: ["sink_id", "worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            commit_conflicts: registry.register(metric!(
                name: "mz_sink_iceberg_commit_conflicts",
                help: "Number of commit conflicts in the iceberg sink",
                var_labels: ["sink_id", "worker_id"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            commit_duration_seconds: registry.register(metric!(
                name: "mz_sink_iceberg_commit_duration_seconds",
                help: "Time spent committing batches to Iceberg in seconds",
                var_labels: ["sink_id", "worker_id"],
                buckets: histogram_seconds_buckets(0.001, 32.0),
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Sink],
            )),
            writer_close_duration_seconds: registry.register(metric!(
                name: "mz_sink_iceberg_writer_close_duration_seconds",
                help: "Time spent closing Iceberg DeltaWriters in seconds",
                var_labels: ["sink_id", "worker_id"],
                buckets: histogram_seconds_buckets(0.001, 32.0),
            )),
        }
    }
}

pub(crate) struct IcebergSinkMetrics {
    /// Number of data files written by the iceberg sink.
    pub data_files_written: DeregisterOnDropCounter<AtomicU64>,
    /// Number of delete files written by the iceberg sink.
    pub delete_files_written: DeregisterOnDropCounter<AtomicU64>,
    /// Number of stashed rows in the iceberg sink.
    pub stashed_rows: DeregisterOnDropGauge<AtomicU64>,
    /// Number of snapshots committed by the iceberg sink.
    pub snapshots_committed: DeregisterOnDropCounter<AtomicU64>,
    /// Number of commit failures in the iceberg sink.
    pub commit_failures: DeregisterOnDropCounter<AtomicU64>,
    /// Number of commit conflicts in the iceberg sink.
    pub commit_conflicts: DeregisterOnDropCounter<AtomicU64>,
    /// Time spent committing batches to Iceberg.
    pub commit_duration_seconds: DeregisterOnDropHistogram,
    /// Time spent closing Iceberg DeltaWriters.
    pub writer_close_duration_seconds: DeregisterOnDropHistogram,
}

impl IcebergSinkMetrics {
    /// Create a `IcebergSinkMetrics` from the `IcebergSinkMetricDefs`.
    pub(crate) fn new(defs: &IcebergSinkMetricDefs, sink_id: GlobalId, worker_id: usize) -> Self {
        let labels = vec![sink_id.to_string(), worker_id.to_string()];
        Self {
            data_files_written: defs
                .data_files_written
                .get_deregister_on_drop_metric(labels.clone()),
            delete_files_written: defs
                .delete_files_written
                .get_deregister_on_drop_metric(labels.clone()),
            stashed_rows: defs
                .stashed_rows
                .get_deregister_on_drop_metric(labels.clone()),
            snapshots_committed: defs
                .snapshots_committed
                .get_deregister_on_drop_metric(labels.clone()),
            commit_failures: defs
                .commit_failures
                .get_deregister_on_drop_metric(labels.clone()),
            commit_conflicts: defs
                .commit_conflicts
                .get_deregister_on_drop_metric(labels.clone()),
            commit_duration_seconds: defs
                .commit_duration_seconds
                .get_deregister_on_drop_metric(labels.clone()),
            writer_close_duration_seconds: defs
                .writer_close_duration_seconds
                .get_deregister_on_drop_metric(labels),
        }
    }
}
