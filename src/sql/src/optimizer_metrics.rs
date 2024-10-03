// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics collected by the optimizer.

use std::time::Duration;

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{HistogramVec, IntCounterVec};

/// Optimizer metrics.
#[derive(Debug, Clone)]
pub struct OptimizerMetrics {
    e2e_optimization_time_seconds: HistogramVec,
    outer_join_lowering_cases: IntCounterVec,
}

impl OptimizerMetrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            e2e_optimization_time_seconds: registry.register(metric!(
                 name: "mz_optimizer_e2e_optimization_time_seconds",
                 help: "A histogram of end-to-end optimization times since restart.",
                 var_labels: ["object_type"],
                 buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
            outer_join_lowering_cases: registry.register(metric!(
                name: "outer_join_lowering_cases",
                help: "How many times the different outer join lowering cases happened.",
                var_labels: ["case"],
            )),
        }
    }

    pub fn observe_e2e_optimization_time(&self, object_type: &str, duration: Duration) {
        self.e2e_optimization_time_seconds
            .with_label_values(&[object_type])
            .observe(duration.as_secs_f64());
        if duration > Duration::from_millis(500) {
            tracing::warn!(
                object_type = object_type,
                duration = format!("{}ms", duration.as_millis()),
                "optimizer took more than 500ms"
            );
        }
    }

    pub fn inc_outer_join_lowering(&self, case: &str) {
        self.outer_join_lowering_cases
            .with_label_values(&[case])
            .inc()
    }
}
