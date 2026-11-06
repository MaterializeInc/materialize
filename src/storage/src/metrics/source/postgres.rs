// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for Postgres.

use mz_ore::metric;
use mz_ore::metrics::{
    DeregisterOnDropCounter, DeregisterOnDropGauge, DeregisterOnDropVec, MetricsRegistry, raw,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex;

/// Definitions for Postgres source metrics.
#[derive(Clone, Debug)]
pub(crate) struct PgSourceMetricDefs {
    pub(crate) total_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) transactions: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) ignored_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) insert_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) update_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) delete_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) tables_in_publication: DeregisterOnDropVec<raw::UIntGaugeVec>,
    pub(crate) wal_lsn: DeregisterOnDropVec<raw::UIntGaugeVec>,
    pub(crate) table_count_latency: DeregisterOnDropVec<raw::GaugeVec>,
}

impl PgSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
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
            )),
            table_count_latency: registry.register(metric!(
                name: "mz_postgres_snapshot_count_latency",
                help: "The wall time used to obtain snapshot sizes.",
                var_labels: ["source_id", "table_name"],
            )),
        }
    }
}

#[derive(Clone)]
pub(crate) struct PgSnapshotMetrics {
    source_id: GlobalId,
    // This has to be shared between tokio tasks and the replication operator, as the collection
    // of these metrics happens once in those tasks, which do not live long enough to keep them
    // alive.
    gauges: Arc<Mutex<Vec<DeregisterOnDropGauge<AtomicF64>>>>,
    defs: PgSourceMetricDefs,
}

impl PgSnapshotMetrics {
    pub(crate) fn record_table_count_latency(&self, table_name: String, latency: f64) {
        let latency_gauge = self
            .defs
            .table_count_latency
            .get_deregister_on_drop_metric(vec![self.source_id.to_string(), table_name]);
        latency_gauge.set(latency);
        self.gauges.lock().expect("poisoned").push(latency_gauge)
    }
}

/// Metrics for Postgres sources.
pub(crate) struct PgSourceMetrics {
    pub(crate) inserts: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) updates: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) deletes: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) ignored: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) total: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) transactions: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) tables: DeregisterOnDropGauge<AtomicU64>,
    pub(crate) lsn: DeregisterOnDropGauge<AtomicU64>,

    pub(crate) snapshot_metrics: PgSnapshotMetrics,
}

impl PgSourceMetrics {
    /// Create a `PgSourceMetrics` from the `PgSourceMetricDefs`.
    pub(crate) fn new(defs: &PgSourceMetricDefs, source_id: GlobalId) -> Self {
        let labels = &[source_id.to_string()];
        Self {
            inserts: defs
                .insert_messages
                .get_deregister_on_drop_metric(labels.to_vec()),
            updates: defs
                .update_messages
                .get_deregister_on_drop_metric(labels.to_vec()),
            deletes: defs
                .delete_messages
                .get_deregister_on_drop_metric(labels.to_vec()),
            ignored: defs
                .ignored_messages
                .get_deregister_on_drop_metric(labels.to_vec()),
            total: defs
                .total_messages
                .get_deregister_on_drop_metric(labels.to_vec()),
            transactions: defs
                .transactions
                .get_deregister_on_drop_metric(labels.to_vec()),
            tables: defs
                .tables_in_publication
                .get_deregister_on_drop_metric(labels.to_vec()),
            lsn: defs.wal_lsn.get_deregister_on_drop_metric(labels.to_vec()),
            snapshot_metrics: PgSnapshotMetrics {
                source_id,
                gauges: Default::default(),
                defs: defs.clone(),
            },
        }
    }
}
