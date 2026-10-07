// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics shared by both compute and storage.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mz_ore::cast::CastLossy;
use mz_ore::metric;
use mz_ore::metrics::{
    CounterVec, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVec, IntCounterVec, MetricTag,
    MetricVisibility, MetricsRegistry,
};
use mz_ore::stats::SlidingMinMax;
use prometheus::core::{AtomicF64, AtomicU64};
use prometheus::proto::LabelPair;

/// Controller metrics.
#[derive(Debug, Clone)]
pub struct ControllerMetrics {
    workload_classes: Arc<Mutex<BTreeMap<String, String>>>,
    dataflow_wallclock_lag_seconds: GaugeVec,
    dataflow_wallclock_lag_seconds_sum: CounterVec,
    dataflow_wallclock_lag_seconds_count: IntCounterVec,
}

impl ControllerMetrics {
    /// Create a metrics instance registered into the given registry.
    pub fn new(metrics_registry: &MetricsRegistry) -> Self {
        let workload_classes = Arc::new(Mutex::new(BTreeMap::<String, String>::new()));

        // Apply a `workload_class` label to all metrics in the registry that
        // have an `instance_id` label for an instance whose workload class is
        // known.
        metrics_registry.register_postprocessor({
            let workload_classes = Arc::clone(&workload_classes);
            move |metrics| {
                let workload_classes = workload_classes.lock().expect("lock poisoned").clone();
                for metric in metrics {
                    'metric: for metric in metric.mut_metric() {
                        for label in metric.get_label() {
                            if label.name() == "instance_id" {
                                if let Some(workload_class) =
                                    workload_classes.get(label.value()).cloned()
                                {
                                    let mut label = LabelPair::default();
                                    label.set_name("workload_class".into());
                                    label.set_value(workload_class);

                                    let mut labels = metric.take_label();
                                    labels.push(label);
                                    metric.set_label(labels);
                                }
                                continue 'metric;
                            }
                        }
                    }
                }
            }
        });

        Self {
            workload_classes,
            // The next three metrics immitate a summary metric type. The `prometheus` crate lacks
            // support for summaries, so we roll our own. Note that we also only expose the 0- and
            // the 1-quantile, i.e., minimum and maximum lag values.
            dataflow_wallclock_lag_seconds: metrics_registry.register(metric!(
                name: "mz_dataflow_wallclock_lag_seconds",
                help: "A summary of the second-by-second lag of the dataflow frontier relative \
                       to wallclock time, aggregated over the last minute.",
                var_labels: ["instance_id", "replica_id", "collection_id", "quantile"],
                visibility: MetricVisibility::Public,
                tags: [MetricTag::Compute, MetricTag::Source, MetricTag::Sink],
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

    /// Set the current committed workload class used to enrich cluster metrics.
    /// `None` removes enrichment, both for RESET and cluster deletion.
    pub fn set_workload_class(&self, cluster_id: String, workload_class: Option<String>) {
        let mut classes = self.workload_classes.lock().expect("lock poisoned");
        if let Some(workload_class) = workload_class {
            classes.insert(cluster_id, workload_class);
        } else {
            classes.remove(&cluster_id);
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
    wallclock_lag_seconds_min: DeleteOnDropGauge<AtomicF64, Vec<String>>,
    /// Gauge tracking maximum dataflow wallclock lag.
    wallclock_lag_seconds_max: DeleteOnDropGauge<AtomicF64, Vec<String>>,
    /// Counter tracking the total sum of dataflow wallclock lag.
    wallclock_lag_seconds_sum: DeleteOnDropCounter<AtomicF64, Vec<String>>,
    /// Counter tracking the total count of dataflow wallclock lag measurements.
    wallclock_lag_seconds_count: DeleteOnDropCounter<AtomicU64, Vec<String>>,

    /// State maintaining minimum and maximum wallclock lag.
    wallclock_lag_minmax: SlidingMinMax<f32>,
}

impl WallclockLagMetrics {
    /// Observe a new wallclock lag measurement.
    pub fn observe(&mut self, lag_secs: u64) {
        let lag_secs = f32::cast_lossy(lag_secs);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn workload_labels_follow_cluster_metadata_without_recreating_metrics() {
        let registry = MetricsRegistry::new();
        let metrics = ControllerMetrics::new(&registry);
        let mut lag =
            metrics.wallclock_lag_metrics("u1".into(), Some("u1".into()), Some("u1".into()));
        let mut other =
            metrics.wallclock_lag_metrics("u2".into(), Some("u2".into()), Some("u2".into()));
        lag.observe(1);
        other.observe(1);

        for class in [None, Some("production"), Some("staging"), None] {
            metrics.set_workload_class("u1".into(), class.map(str::to_owned));
            let families = registry.gather();
            let counts = families
                .iter()
                .find(|family| family.name() == "mz_dataflow_wallclock_lag_seconds_count")
                .expect("retained collection metrics");
            assert_eq!(counts.get_metric().len(), 2);
            for metric in counts.get_metric() {
                let labels = metric.get_label();
                let cluster = labels
                    .iter()
                    .find(|label| label.name() == "instance_id")
                    .unwrap()
                    .value();
                let classes: Vec<_> = labels
                    .iter()
                    .filter(|label| label.name() == "workload_class")
                    .map(|label| label.value())
                    .collect();
                let expected = if cluster == "u1" { class } else { None };
                assert_eq!(classes, expected.into_iter().collect::<Vec<_>>());
                assert_eq!(metric.get_counter().value(), 1.0);
            }
        }
    }
}
