// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for SQL Server.

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, GaugeVec, IntCounterVec, MetricsRegistry, UIntGaugeVec,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex;

/// Definitions for SQL Server source metrics.
#[derive(Clone, Debug)]
pub(crate) struct SqlServerSourceMetricDefs {
    pub(crate) total_messages: IntCounterVec,
    pub(crate) insert_messages: IntCounterVec,
    pub(crate) update_messages: IntCounterVec,
    pub(crate) delete_messages: IntCounterVec,
    pub(crate) tables_in_source: UIntGaugeVec,
    pub(crate) lsn: UIntGaugeVec,
    pub(crate) table_count_latency: GaugeVec,
    pub(crate) table_lock_latency: GaugeVec,
}

impl SqlServerSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            total_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_messages_total",
                help: "The total number of replication messages for this source, not expected to be the sum of the other values.",
                var_labels: ["source_id"],
            )),
            insert_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_inserts",
                help: "The number of inserts for all tables in this source",
                var_labels: ["source_id"],
            )),
            update_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_updates",
                help: "The number of updates for all tables in this source",
                var_labels: ["source_id"],
            )),
            delete_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_deletes",
                help: "The number of deletes for all tables in this source",
                var_labels: ["source_id"],
            )),
            tables_in_source: registry.register(metric!(
                name: "mz_sql_server_per_source_tables_count",
                help: "The number of upstream tables for this source",
                var_labels: ["source_id"],
            )),
            lsn: registry.register(metric!(
                name: "mz_sql_server_per_source_lsn",
                help: "LSN of the latest transaction committed for this source, see SQL Server CDC docs for more details on LSN",
                var_labels: ["source_id"],
            )),
            table_count_latency: registry.register(metric!(
                name: "mz_sql_server_snapshot_count_latency",
                help: "The wall time used to obtain snapshot sizes.",
                var_labels: ["source_id", "table_name"],
            )),
            table_lock_latency: registry.register(metric!(
                name: "mz_sql_server_snapshot_lock_latency",
                help: "The wall time used to obtain snapshot sizes.",
                var_labels: ["source_id", "table_name"],
            )),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SqlServerSnapshotMetrics {
    source_id: GlobalId,
    // This has to be shared between tokio tasks and the replication operator, as the collection
    // of these metrics happens once in those tasks, which do not live long enough to keep them
    // alive.
    gauges: Arc<Mutex<Vec<DeleteOnDropGauge<AtomicF64, Vec<String>>>>>,
    defs: SqlServerSourceMetricDefs,
}

impl SqlServerSnapshotMetrics {
    pub(crate) fn record_table_count_latency(&self, table_name: Arc<str>, latency: f64) {
        let latency_gauge = self
            .defs
            .table_count_latency
            .get_delete_on_drop_metric(vec![self.source_id.to_string(), table_name.to_string()]);
        latency_gauge.set(latency);
        self.gauges.lock().expect("poisoned").push(latency_gauge)
    }
    pub(crate) fn record_table_lock_latency(&self, table_name: Arc<str>, latency: f64) {
        let latency_gauge = self
            .defs
            .table_lock_latency
            .get_delete_on_drop_metric(vec![self.source_id.to_string(), table_name.to_string()]);
        latency_gauge.set(latency);
        self.gauges.lock().expect("poisoned").push(latency_gauge)
    }
}

/// Metrics for sql_server sources.
pub(crate) struct SqlServerSourceMetrics {
    pub(crate) inserts: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) updates: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) deletes: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) total: DeleteOnDropCounter<AtomicU64, Vec<String>>,
    pub(crate) tables: DeleteOnDropGauge<AtomicU64, Vec<String>>,
    pub(crate) lsn: DeleteOnDropGauge<AtomicU64, Vec<String>>,

    pub(crate) snapshot_metrics: SqlServerSnapshotMetrics,
}

impl SqlServerSourceMetrics {
    /// Create a `SqlServerSourceMetrics` from the `SqlServerSourceMetricDefs`.
    pub(crate) fn new(defs: &SqlServerSourceMetricDefs, source_id: GlobalId) -> Self {
        let labels = &[source_id.to_string()];
        Self {
            inserts: defs
                .insert_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            updates: defs
                .update_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            deletes: defs
                .delete_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            total: defs
                .total_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            tables: defs
                .tables_in_source
                .get_delete_on_drop_metric(labels.to_vec()),
            lsn: defs.lsn.get_delete_on_drop_metric(labels.to_vec()),
            snapshot_metrics: SqlServerSnapshotMetrics {
                source_id,
                gauges: Default::default(),
                defs: defs.clone(),
            },
        }
    }
}
