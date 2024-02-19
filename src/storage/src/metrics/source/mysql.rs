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
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVec, GaugeVecExt, MetricsRegistry, UIntGaugeVec};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicU64};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct MySqlSourceMetricDefs {
    pub(crate) table_count: UIntGaugeVec,
    pub(crate) table_count_latency: GaugeVec,
    pub(crate) table_estimate: UIntGaugeVec,
    pub(crate) table_estimate_latency: GaugeVec,
}

impl MySqlSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            table_count: registry.register(metric!(
                name: "mz_mysql_snapshot_count",
                help: "The count(*) of tables in the sources snapshot.",
                var_labels: ["source_id", "table_name", "schema"],
            )),
            table_count_latency: registry.register(metric!(
                name: "mz_mysql_snapshot_count_latency",
                help: "The wall time used to obtain `mz_mysql_snapshot_count`.",
                var_labels: ["source_id", "table_name", "schema"],
            )),
            table_estimate: registry.register(metric!(
                name: "mz_mysql_snapshot_estimate",
                help: "An estimate of the size of tables in the sources snapshot.",
                var_labels: ["source_id", "table_name", "schema"],
            )),
            table_estimate_latency: registry.register(metric!(
                name: "mz_mysql_snapshot_estimate_latency",
                help: "The wall time used to obtain `mz_mysql_snapshot_estimate`.",
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
    gauges: Arc<
        Mutex<
            Vec<(
                DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
                DeleteOnDropGauge<'static, AtomicF64, Vec<String>>,
            )>,
        >,
    >,
    defs: MySqlSourceMetricDefs,
}

impl MySqlSnapshotMetrics {
    pub(crate) fn record_table_count(
        &self,
        table_name: String,
        schema: String,
        count: u64,
        latency: f64,
    ) {
        let gauge = self.defs.table_count.get_delete_on_drop_gauge(vec![
            self.source_id.to_string(),
            table_name.clone(),
            schema.clone(),
        ]);
        let latency_gauge = self.defs.table_count_latency.get_delete_on_drop_gauge(vec![
            self.source_id.to_string(),
            table_name,
            schema,
        ]);
        gauge.set(count);
        latency_gauge.set(latency);
        self.gauges
            .lock()
            .expect("poisoned")
            .push((gauge, latency_gauge))
    }
    pub(crate) fn record_table_estimate(
        &self,
        table_name: String,
        schema: String,
        estimate: u64,
        latency: f64,
    ) {
        let gauge = self.defs.table_estimate.get_delete_on_drop_gauge(vec![
            self.source_id.to_string(),
            table_name.clone(),
            schema.clone(),
        ]);
        let latency_gauge = self
            .defs
            .table_estimate_latency
            .get_delete_on_drop_gauge(vec![self.source_id.to_string(), table_name.clone(), schema]);
        gauge.set(estimate);
        latency_gauge.set(latency);
        self.gauges
            .lock()
            .expect("poisoned")
            .push((gauge, latency_gauge))
    }
}

/// Metrics for MySql sources.
pub(crate) struct MySqlSourceMetrics {
    pub(crate) snapshot_metrics: MySqlSnapshotMetrics,
}

impl MySqlSourceMetrics {
    /// Create a `MySqlSourceMetrics` from the `MySqlSourceMetricDefs`.
    pub(crate) fn new(defs: &MySqlSourceMetricDefs, source_id: GlobalId) -> Self {
        Self {
            snapshot_metrics: MySqlSnapshotMetrics {
                source_id,
                gauges: Default::default(),
                defs: defs.clone(),
            },
        }
    }
}
