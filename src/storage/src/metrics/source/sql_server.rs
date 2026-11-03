// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for Postgres.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Mutex;

use mz_ore::metric;
use mz_ore::metrics::{
    DeregisterOnDropCounter, DeregisterOnDropGauge, DeregisterOnDropVec, MetricsRegistry, raw,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicI64, AtomicU64};

/// Definitions for Postgres source metrics.
#[derive(Clone, Debug)]
pub(crate) struct SqlServerSourceMetricDefs {
    pub(crate) ignored_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) insert_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) update_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) delete_messages: DeregisterOnDropVec<raw::IntCounterVec>,
    pub(crate) snapshot_table_count: DeregisterOnDropVec<raw::UIntGaugeVec>,
    pub(crate) snapshot_table_size_latency: DeregisterOnDropVec<raw::GaugeVec>,
    pub(crate) snapshot_table_lock: DeregisterOnDropVec<raw::IntGaugeVec>,
}

impl SqlServerSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            ignored_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_ignored_messages",
                help: "The number of messages ignored because of an irrelevant type or relation_id",
                var_labels: ["source_id", "worker_id"],
            )),
            insert_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_inserts",
                help: "The number of inserts for all tables in this source",
                var_labels: ["source_id", "worker_id"],
            )),
            update_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_updates",
                help: "The number of updates for all tables in this source",
                var_labels: ["source_id", "worker_id"],
            )),
            delete_messages: registry.register(metric!(
                name: "mz_sql_server_per_source_deletes",
                help: "The number of deletes for all tables in this source",
                var_labels: ["source_id", "worker_id"],
            )),
            snapshot_table_count: registry.register(metric!(
                name: "mz_sql_server_snapshot_table_count",
                help: "The number of tables that SQL Server still needs to snapshot",
                var_labels: ["source_id", "worker_id"],
            )),
            snapshot_table_size_latency: registry.register(metric!(
                name: "mz_sql_server_snapshot_count_latency",
                help: "The wall time used to obtain snapshot sizes.",
                var_labels: ["source_id", "worker_id", "table_name"],
            )),
            snapshot_table_lock: registry.register(metric!(
                name: "mz_sql_server_snapshot_table_lock",
                help: "The upstream tables locked for snapshot.",
                var_labels: ["source_id", "worker_id", "table_name"],
            )),
        }
    }
}
#[derive(Clone)]
/// Metrics for Postgres sources.
pub(crate) struct SqlServerSourceMetrics {
    // stored as String to avoid having to convert them repeatedly.
    source_id: String,
    worker_id: String,
    defs: SqlServerSourceMetricDefs,
    // Currently, this structure is not accessed across threads.
    snapshot_table_size_latency: Rc<Mutex<BTreeMap<String, DeregisterOnDropGauge<AtomicF64>>>>,
    snapshot_table_lock_count: Rc<Mutex<BTreeMap<String, DeregisterOnDropGauge<AtomicI64>>>>,

    pub(crate) inserts: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) updates: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) deletes: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) ignored: DeregisterOnDropCounter<AtomicU64>,
    pub(crate) snapshot_table_count: DeregisterOnDropGauge<AtomicU64>,
}

impl SqlServerSourceMetrics {
    /// Create a `SqlServerSourceMetrics` from the `SqlServerSourceMetricDefs`.
    pub(crate) fn new(
        defs: &SqlServerSourceMetricDefs,
        source_id: GlobalId,
        worker_id: usize,
    ) -> Self {
        let source_id_labels = &[source_id.to_string(), worker_id.to_string()];
        Self {
            source_id: source_id.to_string(),
            worker_id: worker_id.to_string(),
            defs: defs.clone(),
            inserts: defs
                .insert_messages
                .get_deregister_on_drop_metric(source_id_labels.to_vec()),
            updates: defs
                .update_messages
                .get_deregister_on_drop_metric(source_id_labels.to_vec()),
            deletes: defs
                .delete_messages
                .get_deregister_on_drop_metric(source_id_labels.to_vec()),
            ignored: defs
                .ignored_messages
                .get_deregister_on_drop_metric(source_id_labels.to_vec()),
            snapshot_table_count: defs
                .snapshot_table_count
                .get_deregister_on_drop_metric(source_id_labels.to_vec()),
            snapshot_table_size_latency: Default::default(),
            snapshot_table_lock_count: Default::default(),
        }
    }

    pub fn set_snapshot_table_size_latency(&self, table_name: &str, latency: f64) {
        let mut snapshot_table_size_latency =
            self.snapshot_table_size_latency.lock().expect("poisoned");
        match snapshot_table_size_latency.entry(table_name.to_string()) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let labels = vec![
                    self.source_id.clone(),
                    self.worker_id.clone(),
                    table_name.to_string(),
                ];
                let metric = self
                    .defs
                    .snapshot_table_size_latency
                    .get_deregister_on_drop_metric(labels);
                vacant_entry.insert(metric).set(latency);
            }
            std::collections::btree_map::Entry::Occupied(occupied_entry) => {
                occupied_entry.get().set(latency)
            }
        }
    }

    pub fn update_snapshot_table_lock_count(&self, table_name: &str, delta: i64) {
        let mut snapshot_table_lock_count =
            self.snapshot_table_lock_count.lock().expect("poisoned");
        match snapshot_table_lock_count.entry(table_name.to_string()) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let labels = vec![
                    self.source_id.clone(),
                    self.worker_id.clone(),
                    table_name.to_string(),
                ];
                let metric = self
                    .defs
                    .snapshot_table_lock
                    .get_deregister_on_drop_metric(labels);
                vacant_entry.insert(metric).add(delta);
            }
            std::collections::btree_map::Entry::Occupied(occupied_entry) => {
                occupied_entry.get().add(delta);
            }
        }
    }
}
