// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tools for interacting with Prometheus metrics

use std::collections::BTreeMap;
use std::fmt;
use std::time::Instant;

use sysinfo::{ProcessorExt, SystemExt};

use ore::cast::CastFrom;
use ore::cgroup::{self, MemoryLimit};
use ore::metric;
use ore::metrics::ThirdPartyMetric;
use ore::metrics::{ComputedGauge, MetricsRegistry, UIntGauge, UIntGaugeVec};
use ore::option::OptionExt;

use crate::BUILD_INFO;

/// Global metrics for the materialized server.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// The number of workers active in the system.
    pub worker_count: UIntGauge,
    /// The number of seconds that the system has been running.
    pub uptime: ComputedGauge,
    /// The amount of time we spend gathering metrics in prometheus endpoints.
    pub request_metrics_gather: UIntGauge,
    /// The amount of time we spend encoding metrics in prometheus endpoints.
    pub request_metrics_encode: UIntGauge,
    /// The amount of time we spend gathering third-party metrics in prometheus
    /// endpoints.
    pub third_party_request_metrics_gather: UIntGauge,
    /// The amount of time we spend encoding third-party metrics in prometheus
    /// endpoints.
    pub third_party_request_metrics_encode: UIntGauge,
}

impl Metrics {
    pub fn register_with(
        registry: &mut MetricsRegistry,
        workers: usize,
        start_instant: Instant,
    ) -> Self {
        let worker_count: UIntGauge = registry.register(metric!(
            name: "mz_server_metadata_timely_worker_threads",
            help: "number of timely worker threads",
            const_labels: {"count" => workers},
        ));
        worker_count.set(u64::cast_from(workers));

        let uptime = {
            let mut system = sysinfo::System::new();
            system.refresh_system();

            let memory_limit = cgroup::detect_memory_limit().unwrap_or_else(|| MemoryLimit {
                max: None,
                swap_max: None,
            });

            registry.register_computed_gauge(
                metric!(
                    name: "mz_server_metadata_seconds",
                    help: "server metadata, value is uptime",
                    const_labels: {
                        "build_time" => BUILD_INFO.time,
                        "version" => BUILD_INFO.version,
                        "build_sha" => BUILD_INFO.sha,
                        "os" => &os_info::get().to_string(),
                        "ncpus_logical" => &num_cpus::get().to_string(),
                        "ncpus_physical" => &num_cpus::get_physical().to_string(),
                        "cpu0" => &{
                            match &system.processors().get(0) {
                                None => "<unknown>".to_string(),
                                Some(cpu0) => format!("{} {}MHz", cpu0.brand(), cpu0.frequency()),
                            }
                        },
                        "memory_total" => &system.total_memory().to_string(),
                        "memory_limit" => memory_limit.max.map(|l| l / 1024).display_or("none"),
                        "swap_limit" => memory_limit.swap_max.map(|l| l / 1024).display_or("none")
                    },
                ),
                move || start_instant.elapsed().as_secs_f64(),
            )
        };

        let request_metrics: ThirdPartyMetric<UIntGaugeVec> = registry.register_third_party_visible(metric!(
            name: "mz_server_scrape_metrics_times",
            help: "how long it took to gather metrics, used for very low frequency high accuracy measures",
            var_labels: ["action"],
        ));

        Self {
            worker_count,
            uptime,
            request_metrics_gather: request_metrics
                .third_party_metric_with_label_values(&["gather"]),
            request_metrics_encode: request_metrics
                .third_party_metric_with_label_values(&["encode"]),
            third_party_request_metrics_encode: request_metrics
                .third_party_metric_with_label_values(&["gather_third_party"]),
            third_party_request_metrics_gather: request_metrics
                .third_party_metric_with_label_values(&["encode_third_party"]),
        }
    }
}

#[derive(Debug)]
pub enum PromMetric<'a> {
    Counter {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Gauge {
        name: &'a str,
        value: f64,
        labels: BTreeMap<&'a str, &'a str>,
    },
    Histogram {
        name: &'a str,
        count: u64,
        labels: BTreeMap<&'a str, &'a str>,
    },
}

impl fmt::Display for PromMetric<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn fmt(
            f: &mut fmt::Formatter,
            name: &str,
            value: impl fmt::Display,
            labels: &BTreeMap<&str, &str>,
        ) -> fmt::Result {
            write!(f, "{} ", name)?;
            for (n, v) in labels.iter() {
                write!(f, "{}={} ", n, v)?;
            }
            writeln!(f, "{}", value)
        }
        match self {
            PromMetric::Counter {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Gauge {
                name,
                value,
                labels,
            } => fmt(f, name, value, labels),
            PromMetric::Histogram {
                name,
                count,
                labels,
            } => fmt(f, name, count, labels),
        }
    }
}

impl<'p> PromMetric<'p> {
    pub fn from_metric_family<'a>(
        m: &'a prometheus::proto::MetricFamily,
    ) -> Result<Vec<PromMetric<'a>>, ()> {
        use prometheus::proto::MetricType;
        fn l2m(metric: &prometheus::proto::Metric) -> BTreeMap<&str, &str> {
            metric
                .get_label()
                .iter()
                .map(|lp| (lp.get_name(), lp.get_value()))
                .collect()
        }
        m.get_metric()
            .iter()
            .map(|metric| {
                Ok(match m.get_field_type() {
                    MetricType::COUNTER => PromMetric::Counter {
                        name: m.get_name(),
                        value: metric.get_counter().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::GAUGE => PromMetric::Gauge {
                        name: m.get_name(),
                        value: metric.get_gauge().get_value(),
                        labels: l2m(metric),
                    },
                    MetricType::HISTOGRAM => PromMetric::Histogram {
                        name: m.get_name(),
                        count: metric.get_histogram().get_sample_count(),
                        labels: l2m(metric),
                    },
                    _ => return Err(()),
                })
            })
            .collect()
    }
}
