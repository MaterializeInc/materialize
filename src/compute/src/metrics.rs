// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_client::metrics::{CommandMetrics, HistoryMetrics};
use mz_ore::metric;
use mz_ore::metrics::raw::{IntCounterVec, UIntGaugeVec};
use mz_ore::metrics::{IntCounter, MetricsRegistry, UIntGauge};

#[derive(Clone, Debug)]
pub struct ComputeMetrics {
    pub history_command_count: UIntGaugeVec,
    pub history_dataflow_count: UIntGauge,
    pub reconciliation_reused_dataflows: IntCounter,
    pub reconciliation_replaced_dataflows: IntCounterVec,
}

impl ComputeMetrics {
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            history_command_count: registry.register(metric!(
                name: "mz_compute_replica_history_command_count",
                help: "The number of commands in the replica's command history.",
                var_labels: ["command_type"],
            )),
            history_dataflow_count: registry.register(metric!(
                name: "mz_compute_replica_history_dataflow_count",
                help: "The number of dataflows in the replica's command history.",
            )),
            reconciliation_reused_dataflows: registry.register(metric!(
                name: "mz_compute_reconciliation_reused_dataflows",
                help: "The number of dataflows that were reused during compute reconciliation.",
            )),
            reconciliation_replaced_dataflows: registry.register(metric!(
                name: "mz_compute_reconciliation_replaced_dataflows",
                help: "The number of dataflows that were replaced during compute reconciliation.",
                var_labels: ["reason"],
            )),
        }
    }

    pub fn for_history(&self) -> HistoryMetrics<UIntGauge> {
        let command_counts = CommandMetrics::build(|typ| {
            self.history_command_count
                .get_metric_with_label_values(&[typ])
                .unwrap()
        });
        let dataflow_count = self.history_dataflow_count.clone();

        HistoryMetrics {
            command_counts,
            dataflow_count,
        }
    }

    /// Record the reconciliation result for a single dataflow.
    ///
    /// Reconciliation is recorded as successful if the given properties all hold. Otherwise it is
    /// recorded as unsuccessful, with a reason based on the first property that does not hold.
    pub fn record_dataflow_reconciliation(
        &self,
        compatible: bool,
        uncompacted: bool,
        subscribe_free: bool,
    ) {
        if !compatible {
            self.reconciliation_replaced_dataflows
                .with_label_values(&["incompatible"])
                .inc();
        } else if !uncompacted {
            self.reconciliation_replaced_dataflows
                .with_label_values(&["compacted"])
                .inc();
        } else if !subscribe_free {
            self.reconciliation_replaced_dataflows
                .with_label_values(&["subscribe"])
                .inc();
        } else {
            self.reconciliation_reused_dataflows.inc();
        }
    }
}
