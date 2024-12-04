// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics shared by both compute and storage.

use std::time::Duration;

use mz_ore::metric;
use mz_ore::metrics::{
    CounterVec, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVec, IntCounterVec, MetricsRegistry,
};
use mz_ore::stats::SlidingMinMax;
use prometheus::core::{AtomicF64, AtomicU64};

/// Controller metrics.
#[derive(Debug, Clone)]
pub struct ControllerMetrics {
    dataflow_wallclock_lag_seconds: GaugeVec,
    dataflow_wallclock_lag_seconds_sum: CounterVec,
    dataflow_wallclock_lag_seconds_count: IntCounterVec,
}

impl ControllerMetrics {
    /// Create a metrics instance registered into the given registry.
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        Self {
            // The next three metrics immitate a summary metric type. The `prometheus` crate lacks
            // support for summaries, so we roll our own. Note that we also only expose the 0- and
            // the 1-quantile, i.e., minimum and maximum lag values.
            dataflow_wallclock_lag_seconds: metrics_registry.register(metric!(
                name: "mz_dataflow_wallclock_lag_seconds",
                help: "A summary of the second-by-second lag of the dataflow frontier relative \
                       to wallclock time, aggregated over the last minute.",
                var_labels: ["instance_id", "replica_id", "collection_id", "quantile"],
            )),
            dataflow_wallclock_lag_seconds_sum: metrics_registry.register(metric!(
                name: "mz_dataflow_wallclock_lag_seconds_sum",
                help: "The total sum of dataflow wallclock lag measurements.",
                var_labels: ["instance_id", "replica_id", "collection_id"],
            )),
            dataflow_wallclock_lag_seconds_count: metrics_registry.register(metric!(
                name: "mz_dataflow_wallclock_lag_seconds_count",
                help: "The total count of dataflow wallclock lag measurements.",
                var_labels: ["instance_id", "replica_id", "collection_id"],
            )),
        }
    }

    /// Return an object that tracks wallclock lag metrics for the given collection on the given
    /// cluster and replica.
    pub fn wallclock_lag_metrics(
        &self,
        collection_id: String,
        instance_id: Option<String>,
        replica_id: Option<String>,
    ) -> WallclockLagMetrics {
        let labels = vec![
            instance_id.unwrap_or_default(),
            replica_id.unwrap_or_default(),
            collection_id,
        ];

        let labels_with_quantile = |quantile: &str| {
            labels
                .iter()
                .cloned()
                .chain([quantile.to_string()])
                .collect()
        };

        let wallclock_lag_seconds_min = self
            .dataflow_wallclock_lag_seconds
            .get_delete_on_drop_metric(labels_with_quantile("0"));
        let wallclock_lag_seconds_max = self
            .dataflow_wallclock_lag_seconds
            .get_delete_on_drop_metric(labels_with_quantile("1"));
        let wallclock_lag_seconds_sum = self
            .dataflow_wallclock_lag_seconds_sum
            .get_delete_on_drop_metric(labels.clone());
        let wallclock_lag_seconds_count = self
            .dataflow_wallclock_lag_seconds_count
            .get_delete_on_drop_metric(labels);
        let wallclock_lag_minmax = SlidingMinMax::new(60);

        WallclockLagMetrics {
            wallclock_lag_seconds_min,
            wallclock_lag_seconds_max,
            wallclock_lag_seconds_sum,
            wallclock_lag_seconds_count,
            wallclock_lag_minmax,
        }
    }
}

/// Metrics tracking frontier wallclock lag for a collection.
#[derive(Debug)]
pub struct WallclockLagMetrics {
    /// Gauge tracking minimum dataflow wallclock lag.
    wallclock_lag_seconds_min: DeleteOnDropGauge<'static, AtomicF64, Vec<String>>,
    /// Gauge tracking maximum dataflow wallclock lag.
    wallclock_lag_seconds_max: DeleteOnDropGauge<'static, AtomicF64, Vec<String>>,
    /// Counter tracking the total sum of dataflow wallclock lag.
    wallclock_lag_seconds_sum: DeleteOnDropCounter<'static, AtomicF64, Vec<String>>,
    /// Counter tracking the total count of dataflow wallclock lag measurements.
    wallclock_lag_seconds_count: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,

    /// State maintaining minimum and maximum wallclock lag.
    wallclock_lag_minmax: SlidingMinMax<f32>,
}

impl WallclockLagMetrics {
    /// Observe a new wallclock lag measurement.
    pub fn observe(&mut self, lag: Duration) {
        let lag_secs = lag.as_secs_f32();

        self.wallclock_lag_minmax.add_sample(lag_secs);

        let (&min, &max) = self
            .wallclock_lag_minmax
            .get()
            .expect("just added a sample");

        self.wallclock_lag_seconds_min.set(min.into());
        self.wallclock_lag_seconds_max.set(max.into());
        self.wallclock_lag_seconds_sum.inc_by(lag_secs.into());
        self.wallclock_lag_seconds_count.inc();
    }
}
