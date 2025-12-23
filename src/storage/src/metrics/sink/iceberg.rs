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
    metrics::{DeleteOnDropCounter, DeleteOnDropGauge, IntCounterVec, UIntGaugeVec},
};
use mz_repr::GlobalId;
use prometheus::core::AtomicU64;

#[derive(Debug, Clone)]
pub(crate) struct IcebergSinkMetricDefs {
    /// Number of rows written by the iceberg sink.
    pub rows_written: IntCounterVec,
    /// Number of rows deleted by the iceberg sink.
    pub rows_deleted: IntCounterVec,
    /// Number of data files written by the iceberg sink.
    pub data_files_written: IntCounterVec,
    /// Number of delete files written by the iceberg sink.
    pub delete_files_written: IntCounterVec,
    /// Number of stashed rows in the iceberg sink.
    pub stashed_rows: UIntGaugeVec,
    /// Total number of bytes written in data and delete files to object storage.
    pub bytes_written: IntCounterVec,
    /// Number of snapshots committed by the iceberg sink.
    pub snapshots_committed: IntCounterVec,
    /// Commit failures in the iceberg sink.
    pub commit_failures: IntCounterVec,
    /// Commit conflicts in the iceberg sink.
    pub commit_conflicts: IntCounterVec,
}

impl IcebergSinkMetricDefs {
    // Every metric must have a worker specific id associated with it. These are later wrapped
    // in a DeleteOnDrop helper. If the label was just `source_id` and one worker completed, it
    // would call the DeleteOnDrop code that deregestiers the metric for `source_id`. Other
    // workers may still be running, but the metrics registry will no longer record or report
    // metrics for that `source_id`.
    pub(crate) fn register_with(registry: &mz_ore::metrics::MetricsRegistry) -> Self {
        Self {
            rows_written: registry.register(metric!(
                name: "sink_iceberg_rows_written",
                help: "Number of rows written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            rows_deleted: registry.register(metric!(
                name: "sink_iceberg_rows_deleted",
                help: "Number of rows deleted by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            data_files_written: registry.register(metric!(
                name: "sink_iceberg_data_files_written",
                help: "Number of data files written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            delete_files_written: registry.register(metric!(
                name: "sink_iceberg_delete_files_written",
                help: "Number of delete files written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            stashed_rows: registry.register(metric!(
                name: "sink_iceberg_stashed_rows",
                help: "Number of stashed rows in the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            bytes_written: registry.register(metric!(
                name: "sink_iceberg_bytes_written",
                help: "Number of bytes written by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            snapshots_committed: registry.register(metric!(
                name: "sink_iceberg_snapshots_committed",
                help: "Number of snapshots committed by the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            commit_failures: registry.register(metric!(
                name: "sink_iceberg_commit_failures",
                help: "Number of commit failures in the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
            commit_conflicts: registry.register(metric!(
                name: "sink_iceberg_commit_conflicts",
                help: "Number of commit conflicts in the iceberg sink",
                var_labels: ["sink_id", "worker_id"]
            )),
        }
    }
}

#[derive(Clone)]
pub(crate) struct IcebergSinkMetrics {
    /// Number of rows written by the iceberg sink.
    pub rows_written: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of rows deleted by the iceberg sink.
    pub rows_deleted: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of data files written by the iceberg sink.
    pub data_files_written: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of delete files written by the iceberg sink.
    pub delete_files_written: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of stashed rows in the iceberg sink.
    pub stashed_rows: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    /// Number of bytes written by the iceberg sink.
    pub bytes_written: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of snapshots committed by the iceberg sink.
    pub snapshots_committed: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of commit failures in the iceberg sink.
    pub commit_failures: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    /// Number of commit conflicts in the iceberg sink.
    pub commit_conflicts: DeleteOnDropCounter<AtomicU64, Vec<String>>,
}

impl IcebergSinkMetrics {
    /// Create a `IcebergSinkMetrics` from the `IcebergSinkMetricDefs`.
    pub(crate) fn new(defs: &IcebergSinkMetricDefs, sink_id: GlobalId, worker_id: usize) -> Self {
        let labels = vec![sink_id.to_string(), worker_id.to_string()];
        Self {
            rows_written: defs.rows_written.get_delete_on_drop_metric(labels.clone()),
            rows_deleted: defs.rows_deleted.get_delete_on_drop_metric(labels.clone()),
            data_files_written: defs
                .data_files_written
                .get_delete_on_drop_metric(labels.clone()),
            delete_files_written: defs
                .delete_files_written
                .get_delete_on_drop_metric(labels.clone()),
            stashed_rows: defs.stashed_rows.get_delete_on_drop_metric(labels.clone()),
            bytes_written: defs.bytes_written.get_delete_on_drop_metric(labels.clone()),
            snapshots_committed: defs
                .snapshots_committed
                .get_delete_on_drop_metric(labels.clone()),
            commit_failures: defs
                .commit_failures
                .get_delete_on_drop_metric(labels.clone()),
            commit_conflicts: defs.commit_conflicts.get_delete_on_drop_metric(labels),
        }
    }
}
