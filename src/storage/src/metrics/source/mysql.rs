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
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVec, GaugeVecExt, MetricsRegistry};
use mz_repr::GlobalId;
use prometheus::core::AtomicF64;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct MySqlSourceMetricDefs {
    pub(crate) table_count_latency: GaugeVec,
}

impl MySqlSourceMetricDefs {
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
    defs: MySqlSourceMetricDefs,
}

impl MySqlSnapshotMetrics {
    pub(crate) fn record_table_count_latency(
        &self,
        table_name: String,
        schema: String,
        latency: f64,
    ) {
        let latency_gauge = self.defs.table_count_latency.get_delete_on_drop_gauge(vec![
            self.source_id.to_string(),
            table_name,
            schema,
        ]);
        latency_gauge.set(latency);
        self.gauges.lock().expect("poisoned").push(latency_gauge)
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
