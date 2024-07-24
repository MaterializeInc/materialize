// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for MySQL.

use std::sync::Arc;
use std::sync::Mutex;

use mz_ore::metric;
use mz_ore::metrics::{
    DeleteOnDropCounter, DeleteOnDropGauge, GaugeVec, IntCounterVec, MetricsRegistry, UIntGaugeVec,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicU64};

#[derive(Clone, Debug)]
pub(crate) struct MySqlSourceMetricDefs {
    pub(crate) total_messages: IntCounterVec,
    pub(crate) ignored_messages: IntCounterVec,
    pub(crate) insert_rows: IntCounterVec,
    pub(crate) update_rows: IntCounterVec,
    pub(crate) delete_rows: IntCounterVec,
    pub(crate) tables: UIntGaugeVec,
    pub(crate) gtid_txids: UIntGaugeVec,

    pub(crate) snapshot_defs: MySqlSnapshotMetricDefs,
}

impl MySqlSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            total_messages: registry.register(metric!(
                name: "mz_mysql_per_source_messages_total",
                help: "The total number of replication messages for this source, not expected to be the sum of the other values.",
                var_labels: ["source_id"],
            )),
            ignored_messages: registry.register(metric!(
                name: "mz_mysql_per_source_ignored_messages",
                help: "The number of messages ignored because of an irrelevant type or relation_id",
                var_labels: ["source_id"],
            )),
            insert_rows: registry.register(metric!(
                name: "mz_mysql_per_source_inserts",
                help: "The number of inserts for all tables in this source",
                var_labels: ["source_id"],
            )),
            update_rows: registry.register(metric!(
                name: "mz_mysql_per_source_updates",
                help: "The number of updates for all tables in this source",
                var_labels: ["source_id"],
            )),
            delete_rows: registry.register(metric!(
                name: "mz_mysql_per_source_deletes",
                help: "The number of deletes for all tables in this source",
                var_labels: ["source_id"],
            )),
            tables: registry.register(metric!(
                name: "mz_mysql_per_source_tables_count",
                help: "The number of upstream tables for this source",
                var_labels: ["source_id"],
            )),
            gtid_txids: registry.register(metric!(
                name: "mz_mysql_sum_gtid_txns",
                help: "The sum of all transaction-ids committed for each GTID Source-ID UUID seen for this source",
                var_labels: ["source_id"],
            )),
            snapshot_defs: MySqlSnapshotMetricDefs::register_with(registry),
        }
    }
}

/// Metrics for MySql sources.
pub(crate) struct MySqlSourceMetrics {
    pub(crate) inserts: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) updates: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) deletes: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) ignored: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) total: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) tables: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) gtid_txids: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub(crate) snapshot_metrics: MySqlSnapshotMetrics,
}

impl MySqlSourceMetrics {
    /// Create a `MySqlSourceMetrics` from the `MySqlSourceMetricDefs`.
    pub(crate) fn new(defs: &MySqlSourceMetricDefs, source_id: GlobalId) -> Self {
        let labels = &[source_id.to_string()];
        Self {
            inserts: defs.insert_rows.get_delete_on_drop_metric(labels.to_vec()),
            updates: defs.update_rows.get_delete_on_drop_metric(labels.to_vec()),
            deletes: defs.delete_rows.get_delete_on_drop_metric(labels.to_vec()),
            ignored: defs
                .ignored_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            total: defs
                .total_messages
                .get_delete_on_drop_metric(labels.to_vec()),
            tables: defs.tables.get_delete_on_drop_metric(labels.to_vec()),
            gtid_txids: defs.gtid_txids.get_delete_on_drop_metric(labels.to_vec()),
            snapshot_metrics: MySqlSnapshotMetrics {
                source_id,
                gauges: Default::default(),
                defs: defs.snapshot_defs.clone(),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MySqlSnapshotMetricDefs {
    pub(crate) table_count_latency: GaugeVec,
}

impl MySqlSnapshotMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            table_count_latency: registry.register(metric!(
                name: "mz_mysql_snapshot_count_latency",
                help: "The wall time used to obtain snapshot sizes.",
                var_labels: ["source_id", "table_name", "schema"],
            )),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MySqlSnapshotMetrics {
    source_id: GlobalId,
    // This has to be shared between tokio tasks and the replication operator, as the collection
    // of these metrics happens once in those tasks, which do not live long enough to keep them
    // alive.
    gauges: Arc<Mutex<Vec<DeleteOnDropGauge<'static, AtomicF64, Vec<String>>>>>,
    defs: MySqlSnapshotMetricDefs,
}

impl MySqlSnapshotMetrics {
    pub(crate) fn record_table_count_latency(
        &self,
        table_name: String,
        schema: String,
        latency: f64,
    ) {
        let latency_gauge = self
            .defs
            .table_count_latency
            .get_delete_on_drop_metric(vec![self.source_id.to_string(), table_name, schema]);
        latency_gauge.set(latency);
        self.gauges.lock().expect("poisoned").push(latency_gauge)
    }
}
